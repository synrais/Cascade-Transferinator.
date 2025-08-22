#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cascade Transferinator — Aqueduct Edition
Reliable, folder-by-folder (cascaded alphabetical) file transfers with durable writes and optional resume.
"""

from __future__ import annotations
import os, sys, time, sqlite3, signal, threading, hashlib  # hashlib used for resume key
from pathlib import Path
from dataclasses import dataclass
from queue import Queue, Empty
from typing import Optional, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

APP_NAME = "Cascade Transferinator"
__version__ = "x_X"

# Drag & drop (falls back if package not installed)
try:
    from tkinterdnd2 import DND_FILES, TkinterDnD  # pip install tkinterdnd2
    DND_AVAILABLE = True
except Exception:
    DND_AVAILABLE = False

CHUNK_DEFAULT = 8 * 1024 * 1024
MAX_RETRIES = 5

# ---------- Hidden/system helpers ----------
def _win_is_hidden_or_system(path: Path) -> bool:
    try:
        import ctypes
        FILE_ATTRIBUTE_HIDDEN = 0x2
        FILE_ATTRIBUTE_SYSTEM = 0x4
        attrs = ctypes.windll.kernel32.GetFileAttributesW(str(path))
        return attrs != -1 and bool(attrs & (FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM))
    except Exception:
        return False

def _posix_is_hidden(path: Path) -> bool:
    return path.name.startswith(".")

def is_hidden_or_system(path: Path) -> bool:
    return _win_is_hidden_or_system(path) if sys.platform == "win32" else _posix_is_hidden(path)

def is_effectively_hidden(path: Path, src_root: Path) -> bool:
    cur = path
    while True:
        if is_hidden_or_system(cur):
            return True
        if cur == src_root:
            break
        cur = cur.parent
    return False

# ---------- Dir-first iterator ----------
def iter_files_dirfirst(src_root: Path):
    """Pre-order, directories-first, case-insensitive; single scandir per directory."""
    def walk(dirpath: Path):
        try:
            entries = list(os.scandir(dirpath))
        except FileNotFoundError:
            return
        visible = []
        for e in entries:
            try:
                is_dir = e.is_dir(follow_symlinks=False)
                is_file = e.is_file(follow_symlinks=False)
            except PermissionError:
                continue
            p = Path(e.path)
            if not is_effectively_hidden(p, src_root):
                visible.append((e, e.name.lower(), is_dir, is_file))
        visible.sort(key=lambda t: (not t[2], t[1]))  # dirs first, alpha
        for e, _, is_dir, _ in visible:
            if is_dir:
                yield from walk(Path(e.path))
        for e, _, _, is_file in visible:
            if is_file:
                yield Path(e.path)
    yield from walk(src_root)

# ---------- Durability ----------
def fsync_path(p: Path):
    try:
        fd = os.open(p, os.O_RDONLY)
        try: os.fsync(fd)
        finally: os.close(fd)
    except Exception:
        pass

# ---------- Resume DB paths ----------
def _resume_base_dir() -> Path:
    if sys.platform == "win32":
        base = os.getenv("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
        return Path(base) / "CascadeTransferinator" / "manifests"
    elif sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "CascadeTransferinator" / "manifests"
    else:
        return Path.home() / ".cache" / "cascade-transferinator" / "manifests"

def resume_db_path_for_pair(src: Path, dst: Path) -> Path:
    key = (str(src.resolve()) + "::" + str(dst.resolve())).encode("utf-8")
    h = hashlib.sha1(key).hexdigest()
    return _resume_base_dir() / f"{h}.db"

# ---------- Engine ----------
@dataclass
class EngineOptions:
    chunk: int = CHUNK_DEFAULT
    verify: bool = False       # fast verify-by-size
    overwrite: bool = False
    use_resume: bool = False
    db_path: Optional[Path] = None

class TransferEngine:
    def __init__(self, src: Path, dst: Path, opts: EngineOptions, progress_q: Queue):
        self.src = src.resolve()
        self.dst = dst.resolve()
        self.opts = opts
        self.q = progress_q
        self.stop_flag = False
        self.conn: Optional[sqlite3.Connection] = None

        # runtime counters for speed reporting
        self.start_t = time.monotonic()
        self.recent_start = self.start_t
        self.total_bytes_copied = 0
        self.recent_bytes = 0

    def stop(self): self.stop_flag = True
    def _post(self, kind: str, **payload): self.q.put((kind, payload))

    def _post_speed(self):
        elapsed = max(1e-6, time.monotonic() - self.start_t)
        recent_elapsed = max(1e-6, time.monotonic() - self.recent_start)
        self._post(
            "speed",
            avg=(self.total_bytes_copied / (1024 * 1024)) / elapsed,
            recent=(self.recent_bytes / (1024 * 1024)) / recent_elapsed,
        )
        self.recent_bytes = 0
        self.recent_start = time.monotonic()

    def _init_db(self):
        if not self.opts.use_resume:
            self.conn = None; return
        dbp = self.opts.db_path or resume_db_path_for_pair(self.src, self.dst)
        dbp.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(dbp))
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=FULL;")
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS files(
            relpath TEXT PRIMARY KEY,
            size INTEGER,
            mtime_ns INTEGER,
            status TEXT,
            attempts INTEGER DEFAULT 0,
            seq INTEGER
        );""")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_status_seq ON files(status, seq);")
        self.conn.commit()
        self._post("status", msg=f"Using resume file: {dbp}")

    def _build_manifest(self) -> int:
        rows = []; seq = 0
        for p in iter_files_dirfirst(self.src):
            rel = str(p.relative_to(self.src)); st = p.stat(); seq += 1
            rows.append((rel, st.st_size, st.st_mtime_ns, seq))
        if self.conn:
            with self.conn:
                for rel, size, mtime, seq in rows:
                    self.conn.execute(
                        """INSERT INTO files(relpath, size, mtime_ns, status, seq)
                           VALUES(?,?,?,?,?)
                           ON CONFLICT(relpath) DO UPDATE SET
                               size=excluded.size,
                               mtime_ns=excluded.mtime_ns,
                               seq=excluded.seq""",
                        (rel, size, mtime, "pending", seq),
                    )
        return len(rows)

    def _ensure_parent(self, p: Path): p.parent.mkdir(parents=True, exist_ok=True)

    def _copy_direct(self, src: Path, dest: Path, expect_size: int) -> Tuple[bool, str]:
        self._ensure_parent(dest)

        if dest.exists() and dest.is_file() and self.opts.overwrite:
            try: dest.unlink()
            except Exception: return False, "error"

        if dest.exists() and dest.is_file() and not self.opts.overwrite:
            try:
                if dest.stat().st_size == expect_size:
                    fsync_path(dest.parent)
                    return True, "kept"
            except Exception:
                pass
            try: dest.unlink()
            except Exception: return False, "error"

        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        try:
            flags |= os.O_BINARY  # Windows
        except Exception:
            pass

        fd = -1
        try:
            fd = os.open(dest, flags, 0o644)
            with src.open("rb", buffering=0) as rf:
                while True:
                    b = rf.read(self.opts.chunk)
                    if not b: break
                    os.write(fd, b)

                    # progress events
                    self._post("bytes", bytes=len(b), rel=str(dest.relative_to(self.dst)))
                    self.total_bytes_copied += len(b)
                    self.recent_bytes += len(b)
                    if time.monotonic() - self.recent_start >= 1.0:
                        self._post_speed()

            os.fsync(fd)
            if not self.opts.verify:
                try:
                    if os.fstat(fd).st_size != expect_size:
                        os.close(fd); fd = -1
                        try: dest.unlink()
                        except Exception: pass
                        return False, "error"
                except Exception:
                    pass
            fsync_path(dest.parent)
            # final speed flush for this file if any recent bytes remain
            if self.recent_bytes > 0:
                self._post_speed()
            return True, "copied"

        except Exception:
            try:
                if fd != -1:
                    try: os.close(fd)
                    except Exception: pass
                if dest.exists(): dest.unlink()
            except Exception: pass
            return False, "error"
        finally:
            if fd != -1:
                try: os.close(fd)
                except Exception: pass

    def run(self):
        if not self.src.exists() or not self.src.is_dir():
            self._post("error", msg=f"Source dir does not exist: {self.src}")
            return
        self.dst.mkdir(parents=True, exist_ok=True)

        self._init_db()
        total_files = self._build_manifest()
        self._post("summary", total_files=total_files)

        # If no DB, build simple pending list
        if not self.conn:
            pending = [(str(p.relative_to(self.src)), p.stat().st_size) for p in iter_files_dirfirst(self.src)]

        while not self.stop_flag:
            if self.conn:
                row = self.conn.execute(
                    """SELECT relpath, size FROM files
                       WHERE status IN ('pending','error')
                       ORDER BY seq ASC, relpath COLLATE NOCASE ASC
                       LIMIT 1"""
                ).fetchone()
                if not row: break
                rel, fsize = row
            else:
                if not pending: break
                rel, fsize = pending.pop(0)

            # announce current file/folder
            top = rel.split(os.sep, 1)[0] if rel else ""
            self._post("panel", rel=rel, top=top)

            src = self.src / rel
            dst = self.dst / rel
            if not (src.exists() and src.is_file()):
                if self.conn:
                    with self.conn:
                        self.conn.execute("UPDATE files SET status='error' WHERE relpath=?", (rel,))
                self._post("tick"); continue

            ok, _ = self._copy_direct(src, dst, expect_size=fsize)
            if self.conn:
                with self.conn:
                    self.conn.execute(
                        "UPDATE files SET status=? WHERE relpath=?",
                        ("done" if ok else "error", rel),
                    )
            self._post("tick")

        # Final summary
        if self.conn:
            total = self.conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
            done  = self.conn.execute("SELECT COUNT(*) FROM files WHERE status='done'").fetchone()[0]
            errs  = self.conn.execute("SELECT COUNT(*) FROM files WHERE status='error'").fetchone()[0]
            pend  = total - done - errs
        else:
            total = total_files; done, errs, pend = total_files, 0, 0

        self._post("final", done=done, total=total, errors=errs, pending=pend)

# ---------- GUI ----------
class App:
    def __init__(self, root):
        self.root = root
        self.root.title(APP_NAME)
        self.root.geometry("900x600")
        self.progress_q: Queue = Queue()
        self.worker: Optional[threading.Thread] = None
        self.engine: Optional[TransferEngine] = None
        self.banner_shown = False

        # Top: drop zones
        top = ttk.Frame(root, padding=12); top.pack(fill="x")
        self.src_var = tk.StringVar(); self.dst_var = tk.StringVar()
        self._drop_zone(top, "Source", self.src_var, 0)
        self._drop_zone(top, "Destination", self.dst_var, 1)

        # Options
        opts = ttk.LabelFrame(root, text="Options", padding=12); opts.pack(fill="x", padx=12, pady=(0,12))
        self.verify_var = tk.BooleanVar(value=False)
        self.overwrite_var = tk.BooleanVar(value=False)
        self.resume_var = tk.BooleanVar(value=False)

        ttk.Checkbutton(opts, text="Verify hashes (slower)", variable=self.verify_var)\
            .grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(opts, text="Overwrite existing files", variable=self.overwrite_var)\
            .grid(row=0, column=1, sticky="w", padx=(20,0))
        self.cb_resume = ttk.Checkbutton(opts, text="Save progress / Resume log",
                                         variable=self.resume_var, command=self._on_resume_toggle)
        self.cb_resume.grid(row=0, column=2, sticky="w", padx=(20,0))

        # Tight inline chunk row
        chunk_row = ttk.Frame(opts); chunk_row.grid(row=1, column=0, columnspan=3, sticky="w", pady=(8,0))
        ttk.Label(chunk_row, text="Chunk (MiB):").pack(side="left")
        self.chunk_var = tk.IntVar(value=CHUNK_DEFAULT // (1024*1024))
        self.sb_chunk = ttk.Spinbox(chunk_row, from_=1, to=256, textvariable=self.chunk_var, width=6,
                                    command=lambda: self._update_chunk_hint())
        self.sb_chunk.pack(side="left", padx=(6,0))
        self.chunk_var.trace_add('write', lambda *args: self._update_chunk_hint())
        self.chunk_hint = ttk.Label(chunk_row, text="", padding=(6,0)); self.chunk_hint.pack(side="left")
        self._update_chunk_hint()

        # Buttons
        btns = ttk.Frame(root, padding=(12,0)); btns.pack(fill="x")
        self.start_btn = ttk.Button(btns, text="Start", command=self.start); self.start_btn.pack(side="left")
        self.stop_btn = ttk.Button(btns, text="Stop", command=self.stop, state="disabled"); self.stop_btn.pack(side="left", padx=(8,0))
        self.clear_btn = ttk.Button(btns, text="Clear resume log for this pair", command=self.clear_resume); self.clear_btn.pack(side="left", padx=(8,0))

        # Resume panel (hidden unless enabled)
        mpanel = ttk.Frame(root, padding=(12,6)); self.mpanel = mpanel
        ttk.Label(mpanel, text="Resume file:").pack(side="left")
        self.resume_path_lbl = ttk.Label(mpanel, text=""); self.resume_path_lbl.pack(side="left", padx=(6,0))
        self._on_resume_toggle()

        # Progress group
        panel = ttk.LabelFrame(root, text="Progress", padding=12); panel.pack(fill="both", expand=True, padx=12, pady=12)
        self.lbl_summary = ttk.Label(panel, text="Waiting…"); self.lbl_summary.pack(anchor="w", pady=(0,6))
        self.pb_overall = ttk.Progressbar(panel, orient="horizontal", mode="determinate"); self.pb_overall.pack(fill="x")

        sp = ttk.Frame(panel); sp.pack(fill="x", pady=(8,0))
        self.lbl_avg = ttk.Label(sp, text="Avg: 0.0 MB/s"); self.lbl_avg.pack(side="left")
        self.lbl_recent = ttk.Label(sp, text="Recent: 0.0 MB/s"); self.lbl_recent.pack(side="left", padx=(12,0))
        self.lbl_top = ttk.Label(panel, text="Current folder: –"); self.lbl_top.pack(anchor="w", pady=(8,0))
        self.lbl_rel = ttk.Label(panel, text="Current file: –"); self.lbl_rel.pack(anchor="w")

        self.log = tk.Text(panel, height=10, state="disabled"); self.log.pack(fill="both", expand=True, pady=(8,0))
        self._show_banner_in_log()

        # Poller
        self.root.after(100, self._poll_queue)

        # Center on screen
        self.root.update_idletasks()
        w = self.root.winfo_width(); h = self.root.winfo_height()
        sw = self.root.winfo_screenwidth(); sh = self.root.winfo_screenheight()
        x = (sw // 2) - (w // 2); y = (sh // 2) - (h // 2)
        self.root.geometry(f"{w}x{h}+{x}+{y}")

    # --- DnD drop zone ---
    def _drop_zone(self, parent, title, var: tk.StringVar, col: int):
        frame = ttk.LabelFrame(parent, text=title, padding=8)
        frame.grid(row=0, column=col, padx=(0 if col==0 else 8,0), sticky="ew")
        parent.grid_columnconfigure(col, weight=1)
        entry = ttk.Entry(frame, textvariable=var); entry.pack(side="left", fill="x", expand=True)
        ttk.Button(frame, text="Browse…", command=lambda: self._browse(var)).pack(side="left", padx=(6,0))
        if DND_AVAILABLE:
            entry.drop_target_register(DND_FILES)
            entry.dnd_bind("<<Drop>>", lambda e, v=var: self._handle_drop(e, v))

    def _browse(self, var: tk.StringVar):
        d = filedialog.askdirectory(mustexist=True)
        if d: var.set(d)

    def _handle_drop(self, event, var: tk.StringVar):
        data = event.data
        paths, buf, in_brace = [], "", False
        for ch in data:
            if ch == "{": in_brace = True; buf = ""
            elif ch == "}": in_brace = False; paths.append(buf)
            elif ch == " " and not in_brace:
                if buf: paths.append(buf); buf = ""
            else: buf += ch
        if buf: paths.append(buf)
        if paths: var.set(paths[0])

    # --- Options helpers ---
    def _on_resume_toggle(self):
        if self.resume_var.get():
            self.mpanel.pack(fill="x")
            try:
                src = Path(self.src_var.get()); dst = Path(self.dst_var.get())
                if src.exists() and str(dst):
                    self.resume_path_lbl.config(text=str(resume_db_path_for_pair(src, dst)))
            except Exception: pass
        else:
            self.mpanel.pack_forget(); self.resume_path_lbl.config(text="")

    def _update_chunk_hint(self):
        try: v = int(self.chunk_var.get())
        except Exception: v = 8
        if v == 1: msg = "Good for small files"
        elif v == 8: msg = "Good for mix of small files and ISO's"
        elif v >= 16: msg = "Good for huge files"
        else: msg = ""
        self.chunk_hint.config(text=msg)

    # --- Worker control ---
    def start(self):
        src = Path(self.src_var.get().strip() or "")
        dst = Path(self.dst_var.get().strip() or "")
        if not src or not src.exists() or not src.is_dir():
            messagebox.showerror("Error", "Please choose a valid SOURCE folder."); return
        if not dst:
            messagebox.showerror("Error", "Please choose a DESTINATION folder."); return

        if self.banner_shown:
            self._clear_log(); self.banner_shown = False

        use_resume = self.resume_var.get()
        auto_db = resume_db_path_for_pair(src, dst) if use_resume else None
        self.resume_path_lbl.config(text=str(auto_db) if auto_db else "")

        opts = EngineOptions(
            chunk=max(1, int(self.chunk_var.get())) * 1024 * 1024,
            verify=self.verify_var.get(),
            overwrite=self.overwrite_var.get(),
            use_resume=use_resume,
            db_path=auto_db,
        )
        self.engine = TransferEngine(src, dst, opts, self.progress_q)
        self.worker = threading.Thread(target=self.engine.run, daemon=True)

        self._log(f"Starting copy from {src} to {dst}")
        self.start_btn.config(state="disabled"); self.stop_btn.config(state="normal")
        self.worker.start()

    def stop(self):
        if self.engine: self.engine.stop()
        self._log("Stopping…")
        self.start_btn.config(state="normal"); self.stop_btn.config(state="disabled")

    def clear_resume(self):
        src = self.src_var.get().strip(); dst = self.dst_var.get().strip()
        if not src or not dst:
            messagebox.showinfo("Clear resume file", "Select a Source and Destination first."); return
        p = resume_db_path_for_pair(Path(src), Path(dst))
        try:
            if p.exists(): p.unlink(); self._log(f"Cleared resume file for this pair: {p}")
            else: self._log("No resume file exists for this pair.")
        except Exception as e:
            messagebox.showerror("Error", f"Couldn't delete resume file:\n{e}")

    # --- Queue/UI pump ---
    def _poll_queue(self):
        try:
            while True:
                kind, payload = self.progress_q.get_nowait()

                if self.banner_shown and kind == "bytes":
                    self._clear_log(); self.banner_shown = False

                if kind == "error":
                    messagebox.showerror("Error", payload.get("msg","Unknown error"))
                    self._log(f"ERROR: {payload.get('msg','')}")
                    self.stop()

                elif kind == "summary":
                    total = payload.get("total_files", 0)
                    self.pb_overall.config(maximum=max(1,total), value=0)
                    self.lbl_summary.config(text=f"0/{total} done | 0 queued | 0 error")

                elif kind == "panel":
                    self.lbl_rel.config(text=f"Current file: {payload.get('rel','')}")
                    self.lbl_top.config(text=f"Current folder: {payload.get('top','')}")

                elif kind == "speed":
                    self.lbl_avg.config(text=f"Avg: {payload.get('avg',0.0):.1f} MB/s")
                    self.lbl_recent.config(text=f"Recent: {payload.get('recent',0.0):.1f} MB/s")

                elif kind == "tick":
                    if self.engine and self.engine.conn:
                        conn = self.engine.conn
                        total = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
                        done  = conn.execute("SELECT COUNT(*) FROM files WHERE status='done'").fetchone()[0]
                        errs  = conn.execute("SELECT COUNT(*) FROM files WHERE status='error'").fetchone()[0]
                        pend  = total - done - errs
                        self.pb_overall.config(maximum=max(1,total), value=done)
                        self.lbl_summary.config(text=f"{done}/{total} done | {pend} queued | {errs} error")
                    else:
                        cur = self.pb_overall["value"] + 1
                        self.pb_overall.config(value=cur)

                elif kind == "status":
                    self._log(payload.get("msg",""))

                elif kind == "final":
                    done = payload.get("done",0); total = payload.get("total",0)
                    errs = payload.get("errors",0); pend = payload.get("pending",0)
                    self.pb_overall.config(maximum=max(1,total), value=done)
                    self.lbl_summary.config(text=f"Final: {done}/{total} done | {pend} queued | {errs} error")
                    self._log(self.lbl_summary.cget("text"))
                    self.stop()

                self.progress_q.task_done()
        except Empty:
            pass
        self.root.after(100, self._poll_queue)

    # --- Log helpers ---
    def _show_banner_in_log(self):
        banner = (
            "This tool copies files in a smooth, sequential flow.\n"
            "It avoids choking the system or overfilling caches by processing one file at a time,\n"
            "completing each folder in cascaded alphabetical order before moving to the next.\n"
            "Use 'Save progress / resume log' to pause and resume later.\n"
            "Drag and drop source and destination folders and press Start."
        )
        self._clear_log(); self._log(banner); self.banner_shown = True

    def _clear_log(self):
        self.log.config(state="normal"); self.log.delete("1.0", "end"); self.log.config(state="disabled")

    def _log(self, msg: str):
        self.log.config(state="normal"); self.log.insert("end", msg + "\n"); self.log.see("end"); self.log.config(state="disabled")

# ---------- Entrypoint ----------
def main():
    try: signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
    except Exception: pass

    Root = TkinterDnD.Tk if DND_AVAILABLE else tk.Tk
    root = Root()
    try:
        style = ttk.Style(root)
        if sys.platform == "win32": style.theme_use("vista")
    except Exception:
        pass

    App(root)
    root.mainloop()

if __name__ == "__main__":
    main()

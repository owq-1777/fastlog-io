#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Directory-level multi log watcher with rotation handling and state tracking."""

import codecs
import json
import logging
import os
import sys
import time
import signal
from collections import deque
from dataclasses import dataclass
from .log_handle import BaseLogHandler

logger = logging.getLogger(__name__)


def stat_safe(path: str):
    try:
        return os.stat(path)
    except FileNotFoundError:
        return None


def family_of(path_or_name: str) -> str:
    base = os.path.splitext(os.path.basename(path_or_name))[0]
    return base.split('.', 1)[0]


def load_state(path: str):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def save_state(path: str, state: dict):
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(state, f)
    os.replace(tmp, path)


@dataclass
class FileMeta:
    path: str
    dev: int
    ino: int
    size: int
    mtime: int
    mtime_ns: int


class DirCache:
    """Cache log files and refresh only when the directory metadata changes."""

    def __init__(self, dirpath: str):
        self.dir = os.path.abspath(dirpath)
        self.dir_mtime_ns = 0
        self.families: dict[str, list[FileMeta]] = {}
        self.inode_map: dict[tuple[int, int], FileMeta] = {}
        self.bootstrap: dict[tuple[int, int], dict[str, int]] = {}
        self.boot_active_inode: dict[str, tuple[int, int]] = {}

    def _scan(self):
        self.families.clear()
        self.inode_map.clear()
        self.bootstrap.clear()
        self.boot_active_inode.clear()

        try:
            with os.scandir(self.dir) as it:
                for e in it:
                    name = e.name
                    if not name.endswith('.log'):
                        continue
                    try:
                        if not e.is_file(follow_symlinks=False):
                            continue
                        st = e.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    fam = family_of(name)
                    fm = FileMeta(
                        path=e.path,
                        dev=st.st_dev,
                        ino=st.st_ino,
                        size=st.st_size,
                        mtime=int(st.st_mtime),
                        mtime_ns=st.st_mtime_ns,
                    )
                    self.families.setdefault(fam, []).append(fm)
        except FileNotFoundError:
            pass

        for fam, arr in self.families.items():
            arr.sort(key=lambda x: x.mtime)
            latest = max(arr, key=lambda x: x.mtime)
            for fm in arr:
                key = (fm.dev, fm.ino)
                self.inode_map[key] = fm
                self.bootstrap[key] = {'size': fm.size, 'mtime': fm.mtime}
            self.boot_active_inode[fam] = (latest.dev, latest.ino)

    def refresh_if_changed(self) -> bool:
        st = stat_safe(self.dir)
        cur_ns = st.st_mtime_ns if st else 0
        if cur_ns != self.dir_mtime_ns:
            self.dir_mtime_ns = cur_ns
            self._scan()
            return True
        return False

    def ensure_ready(self):
        self.refresh_if_changed()

    def newest_path_of_family(self, fam: str) -> FileMeta | None:
        arr = self.families.get(fam)
        return arr[-1] if arr else None

    def find_by_inode(self, dev: int, ino: int) -> FileMeta | None:
        return self.inode_map.get((dev, ino))


class Utf8LineSplitter:
    """Incrementally decode UTF-8 bytes and yield lines preserving trailing newlines."""

    def __init__(self):
        self.decoder = codecs.getincrementaldecoder('utf-8')(errors='replace')
        self.buf = ''

    def feed(self, b: bytes) -> list[str]:
        if not b:
            return []
        s = self.decoder.decode(b)
        if not s:
            return []
        self.buf += s
        lines = []
        while True:
            idx = self.buf.find('\n')
            if idx == -1:
                break
            lines.append(self.buf[: idx + 1])
            self.buf = self.buf[idx + 1 :]
        return lines

    def flush_final(self) -> str | None:
        tail = self.decoder.decode(b'', final=True)
        if tail:
            self.buf += tail
        if self.buf:
            s = self.buf
            self.buf = ''
            return s
        return None


class FamilyCtx:
    """Hold file handle and offsets for a single log family."""

    __slots__ = ('name', 'f', 'path', 'dev', 'ino', 'last_flush', 'last_persist', 'dirty', 'byte_pos', 'splitter')

    def __init__(self, name: str):
        self.name = name
        self.f = None
        self.path: str | None = None
        self.dev: int | None = None
        self.ino: int | None = None
        self.last_flush = time.time()
        self.last_persist = time.time()
        self.dirty = False
        self.byte_pos = 0
        self.splitter = Utf8LineSplitter()


class MultiLogWatcher:
    def __init__(
        self,
        dirpath: str,
        state_path: str,
        fast_interval: float = 0.05,
        slow_interval: float = 1.0,
        backoff: float = 2.0,
        flush_interval: float = 1.0,
        rr_batch: int = 64,
        state_flush_interval: float = 0.8,
        read_chunk: int = 8192,
        handler: BaseLogHandler | None = None,
    ):
        """Configure directory monitoring, cadence, and cache state.

        Args:
            dirpath: Path to the directory that contains *.log files.
            state_path: Optional override for the watcher state file location.
            fast_interval: Polling interval (seconds) when new data is flowing.
            slow_interval: Maximum polling interval (seconds) after backoff.
            backoff: Exponential factor applied when idling.
            flush_interval: Minimum seconds between stdout flush attempts.
            rr_batch: Maximum lines read per family in one round-robin pass.
            state_flush_interval: Minimum seconds between state persistence writes.
            read_chunk: Size in bytes for each read() call.
            handler: Concrete `BaseLogHandler` that consumes decoded lines.
        """
        self.dir = os.path.abspath(dirpath)
        self.state_path = state_path or os.path.join(self.dir, '.multilogwatch.state.json')
        if handler is None:
            raise ValueError('Log handler must be provided for MultiLogWatcher')
        self.handler = handler

        self.cache = DirCache(self.dir)
        self.cache.ensure_ready()
        self.fast_interval = fast_interval
        self.slow_interval = slow_interval
        self.backoff = backoff
        self.cur_interval = slow_interval

        self.flush_interval = flush_interval
        self.state_flush_interval = state_flush_interval
        self.read_chunk = read_chunk

        self.state = load_state(self.state_path) or {'families': {}}

        self.cold_start = not self.state.get('families')

        if self.cold_start:
            self.bootstrap: dict[tuple[int, int], dict[str, int]] = dict(self.cache.bootstrap)
            self.boot_active_inode: dict[str, tuple[int, int]] = dict(self.cache.boot_active_inode)
        else:
            self.bootstrap = {}
            self.boot_active_inode = {}

        self.families: dict[str, FamilyCtx] = {}
        self.rr_order = deque()
        self.rr_batch = rr_batch

        self._stop = False

        self._last_cache_refresh = time.time()
        self._cache_refresh_min_interval = 0.05
        self._last_state_flush = time.time()

    def _install_signal_handlers(self):
        def _handler(signum, frame):
            self._stop = True

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, _handler)
            except Exception:
                pass

    def _boost(self):
        self.cur_interval = self.fast_interval

    def _idle(self):
        self.cur_interval = min(self.slow_interval, self.cur_interval * self.backoff)

    def _set_fd_meta(self, ctx: FamilyCtx):
        st = os.fstat(ctx.f.fileno())
        ctx.dev, ctx.ino = st.st_dev, st.st_ino

    def _persist_family(self, ctx: FamilyCtx, force: bool = False):
        if not ctx.f:
            return
        now = time.time()
        if not force and not self.cold_start:
            ctx.dirty = True
            return

        pos = ctx.byte_pos
        st = self.state
        st.setdefault('families', {})
        st['families'][ctx.name] = {
            'dev': ctx.dev,
            'ino': ctx.ino,
            'pos': pos,
            'path_hint': ctx.path,
            'encoding': 'utf-8',
        }
        st['updated_at'] = int(now)
        save_state(self.state_path, st)
        ctx.last_persist = now
        ctx.dirty = False

    def _persist_dirty_families(self, force: bool = False):
        now = time.time()
        if not force and (now - self._last_state_flush) < self.state_flush_interval:
            return
        for ctx in self.families.values():
            if ctx.dirty or force:
                try:
                    self._persist_family(ctx, force=True)
                except Exception:
                    pass
        self._last_state_flush = now

    def _open_at(self, ctx: FamilyCtx, path: str, byte_offset: int = 0) -> bool:
        if not os.path.exists(path):
            return False
        prev_path = ctx.path
        prev_dev = ctx.dev
        prev_ino = ctx.ino
        if ctx.f:
            try:
                ctx.f.close()
            except Exception:
                pass
        try:
            ctx.f = open(path, 'rb')
        except FileNotFoundError:
            ctx.f = None
            return False

        self._set_fd_meta(ctx)

        st = stat_safe(path)
        size = st.st_size if st else 0
        if byte_offset > size:
            byte_offset = 0
        ctx.f.seek(byte_offset, os.SEEK_SET)
        ctx.byte_pos = ctx.f.tell()
        ctx.path = path
        ctx.splitter = Utf8LineSplitter()

        self._persist_family(ctx, force=True)
        if prev_dev != ctx.dev or prev_ino != ctx.ino or prev_path != ctx.path:
            logger.info(f"Captured log file for family '{ctx.name}': {ctx.path} (offset={ctx.byte_pos})")
        return True

    def _open_tail(self, ctx: FamilyCtx, path: str) -> bool:
        if not self._open_at(ctx, path, 0):
            return False
        ctx.f.seek(0, os.SEEK_END)
        ctx.byte_pos = ctx.f.tell()
        self._persist_family(ctx, force=True)
        return True

    def _bootstrap_offset_for(self, dev: int, ino: int, fallback_size: int) -> int:
        rec = self.bootstrap.get((dev, ino))
        if not rec:
            return 0
        fm = self.cache.find_by_inode(dev, ino)
        cur_size = fm.size if fm else fallback_size
        return rec['size'] if rec['size'] <= cur_size else 0

    def _ensure_ctx(self, fam: str) -> FamilyCtx:
        if fam not in self.families:
            self.families[fam] = FamilyCtx(fam)
            self.rr_order.append(fam)
        return self.families[fam]

    def _recover_or_start(self):
        os.makedirs(self.dir, exist_ok=True)

        for fam, rec in self.state.get('families', {}).items():
            fm = self.cache.find_by_inode(rec.get('dev'), rec.get('ino'))
            if not fm:
                continue
            ctx = self._ensure_ctx(fam)
            if self._open_at(ctx, fm.path, int(rec.get('pos', 0))):
                self._drain_to_eof(ctx)
                self._switch_to_family_latest(ctx)

        for fam, arr in self.cache.families.items():
            if fam in self.families or not arr:
                continue
            ctx = self._ensure_ctx(fam)
            boot_inode = self.boot_active_inode.get(fam)
            if self.cold_start and boot_inode:
                dev, ino = boot_inode
                fm = self.cache.find_by_inode(dev, ino)
                if fm and self._open_at(ctx, fm.path, self._bootstrap_offset_for(dev, ino, fm.size)):
                    self._drain_to_eof(ctx)
                    self._switch_to_family_latest(ctx)
                    continue
            fm = arr[-1]
            off = self._bootstrap_offset_for(fm.dev, fm.ino, fm.size) if (fm.dev, fm.ino) in self.bootstrap else 0
            self._open_at(ctx, fm.path, off)

        while not self.families and not self._stop:
            time.sleep(self.cur_interval)
            if self.cache.refresh_if_changed():
                for fam, arr in self.cache.families.items():
                    if fam not in self.families and arr:
                        ctx = self._ensure_ctx(fam)
                        fm = arr[-1]
                        off = self._bootstrap_offset_for(fm.dev, fm.ino, fm.size) if (fm.dev, fm.ino) in self.bootstrap else 0
                        self._open_at(ctx, fm.path, off)
                self._boost()
            else:
                self._idle()

        self.cold_start = False

    def _read_lines_once(self, ctx: FamilyCtx, max_lines: int) -> list[str]:
        """Read a chunk and return up to max_lines decoded lines."""
        if not ctx.f:
            return []
        try:
            b = ctx.f.read(self.read_chunk)
        except Exception:
            b = b''
        if not b:
            return []
        ctx.byte_pos += len(b)
        out = self.splitter_feed(ctx, b)
        return out[:max_lines]

    def splitter_feed(self, ctx: FamilyCtx, b: bytes) -> list[str]:
        return ctx.splitter.feed(b)

    def _drain_to_eof(self, ctx: FamilyCtx):
        """Consume the current file to EOF without refreshing directory state."""
        while ctx.f and not self._stop:
            lines = self._read_lines_once(ctx, max_lines=256)
            if not lines:
                tail = ctx.splitter.flush_final()
                if tail is not None:
                    try:
                        self.handler.handle(tail, ctx.name)
                    finally:
                        ctx.dirty = True
                        self._persist_dirty_families()
                break
            for ln in lines:
                try:
                    self.handler.handle(ln, ctx.name)
                finally:
                    ctx.dirty = True
                    self._persist_dirty_families()
                    if time.time() - ctx.last_flush >= self.flush_interval:
                        try:
                            sys.stdout.flush()
                        except Exception:
                            pass
                        ctx.last_flush = time.time()

    def _drain_and_switch(self, ctx: FamilyCtx, fm: FileMeta | None):
        """Drain current file, persist, and switch to the target if provided."""
        if ctx.f:
            self._drain_to_eof(ctx)
            self._persist_family(ctx, force=True)
        if fm:
            key = (fm.dev, fm.ino)
            off = self._bootstrap_offset_for(fm.dev, fm.ino, fm.size) if key in self.bootstrap else 0
            self._open_at(ctx, fm.path, off)

    def _switch_to_family_latest(self, ctx: FamilyCtx):
        fm = self.cache.newest_path_of_family(ctx.name)
        if not fm:
            return
        if ctx.dev == fm.dev and ctx.ino == fm.ino:
            return
        self._drain_and_switch(ctx, fm)

    def _step_family(self, ctx: FamilyCtx) -> bool:
        """Read up to rr_batch lines for a family and report whether anything was read."""
        got = False

        lines = self._read_lines_once(ctx, max_lines=self.rr_batch)
        if lines:
            for ln in lines:
                try:
                    self.handler.handle(ln, ctx.name)
                    got = True
                finally:
                    ctx.dirty = True
                    if time.time() - ctx.last_flush >= self.flush_interval:
                        try:
                            sys.stdout.flush()
                        except Exception:
                            pass
                        ctx.last_flush = time.time()
        else:
            now = time.time()
            if (now - self._last_cache_refresh) >= self._cache_refresh_min_interval:
                changed = self.cache.refresh_if_changed()
                self._last_cache_refresh = now
                if changed:
                    fm_new = self.cache.newest_path_of_family(ctx.name)
                    if (
                        fm_new
                        and (ctx.dev is not None and ctx.ino is not None)
                        and (fm_new.dev != ctx.dev or fm_new.ino != ctx.ino)
                    ):
                        self._drain_and_switch(ctx, fm_new)

        if ctx.f and ctx.path:
            if not os.path.exists(ctx.path):
                self.cache.refresh_if_changed()
                moved = self.cache.find_by_inode(ctx.dev, ctx.ino)
                target = self.cache.newest_path_of_family(ctx.name) if not moved else moved
                if moved:
                    ctx.path = moved.path
                self._drain_and_switch(ctx, target)
                return got

            st_cur = stat_safe(ctx.path)
            if st_cur and st_cur.st_size < ctx.byte_pos:
                fm_cur = FileMeta(
                    ctx.path, st_cur.st_dev, st_cur.st_ino, st_cur.st_size, int(st_cur.st_mtime), st_cur.st_mtime_ns
                )
                self._drain_and_switch(ctx, fm_cur)
                return got

            fm_new = self.cache.newest_path_of_family(ctx.name)
            if fm_new and (fm_new.dev != ctx.dev or fm_new.ino != ctx.ino):
                self._drain_and_switch(ctx, fm_new)

        return got

    def start(self):
        logger.info(
            f'Starting MultiLogWatcher on {self.dir} (state={self.state_path}, mode={"cold-start" if self.cold_start else "resume"})'
        )
        self._install_signal_handlers()
        try:
            self._recover_or_start()

            while not self._stop:
                flush_expired = getattr(self.handler, 'flush_expired', None)
                if callable(flush_expired):
                    try:
                        flush_expired()
                    except Exception:
                        pass

                any_got = False

                if not self.rr_order:
                    time.sleep(self.cur_interval)
                    changed = self.cache.refresh_if_changed()
                    if changed:
                        self._attach_new_families_from_cache()
                        self._boost()
                    else:
                        self._idle()
                else:
                    for _ in range(len(self.rr_order)):
                        fam = self.rr_order[0]
                        self.rr_order.rotate(-1)
                        ctx = self.families.get(fam)
                        if not ctx:
                            continue
                        got = self._step_family(ctx)
                        any_got = any_got or got
                        if self._stop:
                            break

                    self._persist_dirty_families()

                    if not any_got:
                        time.sleep(self.cur_interval)
                        changed = self.cache.refresh_if_changed()
                        if changed:
                            self._attach_new_families_from_cache()
                            self._boost()
                        else:
                            self._idle()
                    else:
                        self._boost()
        finally:
            try:
                self._persist_dirty_families(force=True)
            except Exception:
                pass

            flush = getattr(self.handler, 'flush', None)
            if callable(flush):
                try:
                    flush()
                except Exception:
                    pass
            logger.info(f'Stopped MultiLogWatcher on {self.dir}')

    def _attach_new_families_from_cache(self):
        for fam, arr in self.cache.families.items():
            if fam in self.families or not arr:
                continue
            ctx = self._ensure_ctx(fam)
            fm = arr[-1]
            off = self._bootstrap_offset_for(fm.dev, fm.ino, fm.size) if (fm.dev, fm.ino) in self.bootstrap else 0
            self._open_at(ctx, fm.path, off)

"""Filesystem primitives for the lake: local ``file://`` paths and Hadoop (HDFS) paths.

No PySpark import at module level, so the local branch is usable in pure unit tests
(``FileSystem(None, 'file:///tmp/x')``).

Two operations carry atomicity guarantees and the catalog relies on them:

* ``create_exclusive(path, text)`` publishes a NEW immutable file; it returns ``False`` when the
  file already exists and never overwrites. HDFS: write a temp file, then
  ``FileContext.rename(tmp, dst, Options.Rename.NONE)``, which the NameNode refuses atomically when
  ``dst`` exists (the same mechanism as Delta Lake's HDFS log store). Local: ``os.link`` which
  fails atomically with ``FileExistsError``.
* ``replace(path, text)`` atomically creates or replaces a mutable convenience pointer. HDFS:
  ``FileContext.rename(tmp, dst, Options.Rename.OVERWRITE)``; local: ``os.replace``.
"""
from __future__ import annotations

import json
import os
import shutil
import uuid
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname


def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2, default=str)


class FileSystem:
    def __init__(self, spark, root):
        self.spark = spark
        self.root = root.rstrip('/')
        self.is_local = root.startswith('file:') or '://' not in root
        self.jvm = None if self.is_local else spark._jvm
        if not self.is_local:
            self.conf = spark._jsc.hadoopConfiguration()

    # ------------------------------------------------------------------ paths
    def _local(self, path):
        if not path.startswith('file:'):
            return Path(path)
        raw = urlparse(path).path
        return Path(url2pathname(raw)) if os.name == 'nt' else Path(raw)

    def _hpath(self, path):
        return self.jvm.org.apache.hadoop.fs.Path(path)

    def _hfs(self, path):
        p = self._hpath(path)
        return p.getFileSystem(self.conf), p

    def _file_context(self, hpath):
        return self.jvm.org.apache.hadoop.fs.FileContext.getFileContext(hpath.toUri(), self.conf)

    def _rename_options(self, name):
        rename = getattr(self.jvm.org.apache.hadoop.fs, 'Options$Rename')
        arr = self.spark.sparkContext._gateway.new_array(rename, 1)
        arr[0] = getattr(rename, name)
        return arr

    # ------------------------------------------------------------- inspection
    def exists(self, path):
        if self.is_local:
            return self._local(path).exists()
        fs, p = self._hfs(path)
        return bool(fs.exists(p))

    def makedirs(self, path):
        if self.is_local:
            self._local(path).mkdir(parents=True, exist_ok=True)
        else:
            fs, p = self._hfs(path)
            fs.mkdirs(p)

    def list_names(self, path):
        """Sorted names of direct children (files and directories); [] when absent."""
        if self.is_local:
            p = self._local(path)
            return sorted(e.name for e in p.iterdir()) if p.is_dir() else []
        fs, p = self._hfs(path)
        if not fs.exists(p):
            return []
        return sorted(str(s.getPath().getName()) for s in fs.listStatus(p))

    def child_directories(self, path):
        """(name, full_path) of child directories, no Spark action."""
        if self.is_local:
            p = self._local(path)
            return [(entry.name, path.rstrip('/') + '/' + entry.name)
                    for entry in sorted(p.iterdir()) if entry.is_dir()] if p.exists() else []
        fs, p = self._hfs(path)
        if not fs.exists(p):
            return []
        return [(str(s.getPath().getName()), str(s.getPath().toString()))
                for s in fs.listStatus(p) if s.isDirectory()]

    def data_files(self, path):
        """Recursive [(relative_path, size_bytes)] of data files (skips _SUCCESS, .crc, hidden)."""
        out = []
        if self.is_local:
            base = self._local(path)
            if not base.exists():
                return out
            for f in base.rglob('*'):
                if f.is_file() and not f.name.startswith(('.', '_')):
                    out.append((f.relative_to(base).as_posix(), f.stat().st_size))
            return sorted(out)
        fs, p = self._hfs(path)
        if not fs.exists(p):
            return out
        base = str(fs.getFileStatus(p).getPath().toString()).rstrip('/')
        it = fs.listFiles(p, True)
        while it.hasNext():
            st = it.next()
            name = str(st.getPath().getName())
            if name.startswith(('.', '_')):
                continue
            full = str(st.getPath().toString())
            out.append((full[len(base) + 1:] if full.startswith(base) else full, int(st.getLen())))
        return sorted(out)

    def modification_time(self, path):
        """Seconds since epoch."""
        if self.is_local:
            return self._local(path).stat().st_mtime
        fs, p = self._hfs(path)
        return fs.getFileStatus(p).getModificationTime() / 1000.0

    # --------------------------------------------------------------- mutation
    def rename_directory(self, source, target):
        if self.exists(target):
            raise FileExistsError(target)
        if self.is_local:
            self._local(target).parent.mkdir(parents=True, exist_ok=True)
            self._local(source).rename(self._local(target))
            return
        fs, src = self._hfs(source)
        fs.mkdirs(self._hpath(target).getParent())
        if not fs.rename(src, self._hpath(target)):
            raise RuntimeError('failed to rename directory: ' + source + ' -> ' + target)

    def delete(self, path, recursive=True):
        if self.is_local:
            p = self._local(path)
            if p.is_dir():
                shutil.rmtree(p) if recursive else p.rmdir()
            elif p.exists():
                p.unlink()
            return
        fs, p = self._hfs(path)
        fs.delete(p, bool(recursive))

    def read_text(self, path):
        if self.is_local:
            return self._local(path).read_text(encoding='utf-8')
        fs, p = self._hfs(path)
        stream = fs.open(p)
        scanner = self.jvm.java.util.Scanner(stream, 'UTF-8').useDelimiter('\\A')
        try:
            return scanner.next() if scanner.hasNext() else ''
        finally:
            scanner.close()
            stream.close()

    def read_json(self, path, default=None):
        if not self.exists(path):
            return default
        return json.loads(self.read_text(path))

    def _write_new(self, path, text):
        """Create ``path`` (must not exist) with the full content, durably flushed."""
        if self.is_local:
            target = self._local(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            with open(target, 'x', encoding='utf-8') as fh:
                fh.write(text)
                fh.flush()
                os.fsync(fh.fileno())
            return
        fs, p = self._hfs(path)
        stream = fs.create(p, False)
        try:
            stream.write(bytearray(text.encode('utf-8')))
            stream.hsync()
        finally:
            stream.close()

    def _tmp_for(self, path):
        parent = path.rsplit('/', 1)[0]
        self.makedirs(parent)
        return f'{parent}/.tmp.{uuid.uuid4().hex}'

    def create_exclusive(self, path, text):
        """Atomically publish a new immutable file. Returns False if ``path`` already exists."""
        tmp = self._tmp_for(path)
        self._write_new(tmp, text)
        if self.is_local:
            try:
                os.link(self._local(tmp), self._local(path))
                return True
            except FileExistsError:
                return False
            finally:
                self._local(tmp).unlink(missing_ok=True)
        fs, p = self._hfs(path)
        try:
            self._file_context(p).rename(self._hpath(tmp), p, self._rename_options('NONE'))
            return True
        except Exception as exc:  # py4j wraps FileAlreadyExistsException
            if 'FileAlreadyExistsException' in str(exc) or fs.exists(p):
                return False
            raise
        finally:
            if fs.exists(self._hpath(tmp)):
                fs.delete(self._hpath(tmp), False)

    def replace(self, path, text):
        """Atomically create or replace ``path`` with ``text`` (readers never see a gap)."""
        tmp = self._tmp_for(path)
        self._write_new(tmp, text)
        if self.is_local:
            os.replace(self._local(tmp), self._local(path))
            return
        _, p = self._hfs(path)
        self._file_context(p).rename(self._hpath(tmp), p, self._rename_options('OVERWRITE'))

    def write_json(self, path, obj, overwrite=False):
        text = dumps(obj)
        if overwrite:
            self.replace(path, text)
        elif not self.create_exclusive(path, text):
            raise FileExistsError(path)

import contextlib
import os
import socket
import sqlite3

class DbLock(object):
    
    def __init__(self, db, timeout=5.0):
        self.db = db
        self.timeout = timeout
        db.execute("CREATE TABLE IF NOT EXISTS _lock"
                "(kind STRING PRIMARY KEY, owner STRING);")
        host = socket.getfqdn()
        pid = os.getpid()
        thread = threading.currentThread().ident
        self.ownerId = "host {} pid {} thread {}".format(host, pid, thread)
        self.owned = set()

    def _tryLock(self, kind):
        try:
            self.db.execute("INSERT OR ABORT INTO _lock VALUES (?, ?)",
                    (kind, self.ownerId))
            self.owned.add(kind)
            return True
        except sqlite3.IntegrityError:
            return False

    def acquire(self, kind):
        if kind in self.owned:
            return
        startTime = time.time()
        while time.time ()- startTime < self.timeout:
            if self._tryLock(kind):
                return
            time.sleep(0.5)
        cur = self.db.cursor()
        cur.execute("SELECT owner FROM _lock WHERE kind = ?", (kind,))
        result = cur.fetchall()
        if len(result) == 0 and self._tryLock(kind):
            return
        raise TimeoutError(
                "{} could not acquire lock of kind {} held by {}".format(
                    self.ownerId, kind, result[0][0]))

    def release(self, kind):
        if kind not in self.owned:
            raise RuntimeError(
                    "Trying to release unowned lock of kind {}".format(kind))
        cur = self.db.cursor()
        cur.execute("SELECT owner FROM _lock WHERE kind = ?", (kind,))
        result = cur.fetchall()
        if result[0][0] != self.ownerId:
            raise RuntimeError(
                    "{} tried to release lock of kind {}"
                    "held by another owner {}".format(
                        self.ownerId, kind, result[0][0]))
        self.db.execute("DELETE FROM _lock WHERE kind = ?", (kind,))
        self.owned.remove(kind)

    @contextlib.contextmanager
    def lock(self, kind):
        self.acquire(kind)
        yield
        self.release(kind)

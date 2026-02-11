import queue
import threading
import logging

logger = logging.getLogger(__name__)


class L2JobQueue:
    def __init__(self, maxsize=50):
        self.queue = queue.Queue(maxsize=maxsize)
        self._worker_thread = None
        self._running = False

    # =========================================================
    # START WORKER
    # =========================================================
    def start(self, worker_fn):
        if self._worker_thread:
            return

        self._running = True

        def _run():
            while self._running:
                try:
                    job = self.queue.get()
                    worker_fn(job)
                except Exception:
                    logger.exception("L2 worker failed")
                finally:
                    self.queue.task_done()

        self._worker_thread = threading.Thread(
            target=_run,
            daemon=True
        )
        self._worker_thread.start()

    # =========================================================
    # DOMAIN API (USED BY RUNNER)
    # =========================================================
    def add_job(self, job: dict) -> bool:
        """
        Add diagnostic job to queue.
        Non-blocking. Returns False if queue full.
        """
        try:
            self.queue.put(job, block=False)
            return True
        except queue.Full:
            logger.warning("L2 queue full — job dropped")
            return False

    # =========================================================
    # OPTIONAL STOP
    # =========================================================
    def stop(self):
        self._running = False

 
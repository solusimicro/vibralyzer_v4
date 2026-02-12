import queue
import threading
import logging
import time
from collections import deque

logger = logging.getLogger(__name__)


class L2JobQueue:
    """
    Industrial-grade L2 diagnostic queue.

    Features:
    - Worker pool
    - Circuit breaker
    - Retry policy
    - Queue backpressure handling
    - Health metrics
    """

    def __init__(
        self,
        maxsize=100,
        worker_count=2,
        max_retries=2,
        circuit_fail_threshold=10,
        circuit_reset_seconds=30,
        drop_policy="drop_oldest",  # drop_new / drop_oldest
    ):
        self.queue = queue.Queue(maxsize=maxsize)
        self.worker_count = worker_count
        self.max_retries = max_retries
        self.drop_policy = drop_policy

        self._workers = []
        self._running = False

        # Circuit breaker
        self._fail_count = 0
        self._circuit_open_until = 0
        self._fail_threshold = circuit_fail_threshold
        self._reset_seconds = circuit_reset_seconds

        # Metrics
        self.metrics = {
            "jobs_processed": 0,
            "jobs_failed": 0,
            "jobs_dropped": 0,
            "queue_maxsize": maxsize,
        }

    # =========================================================
    # START
    # =========================================================
    def start(self, worker_fn):
        if self._workers:
            return

        self._running = True

        for i in range(self.worker_count):
            t = threading.Thread(
                target=self._worker_loop,
                args=(worker_fn,),
                daemon=True,
                name=f"L2Worker-{i+1}",
            )
            t.start()
            self._workers.append(t)

        logger.info(f"L2 started with {self.worker_count} workers")

    # =========================================================
    # WORKER LOOP
    # =========================================================
    def _worker_loop(self, worker_fn):
        while self._running:
            try:
                job = self.queue.get(timeout=1)
            except queue.Empty:
                continue

            # Circuit breaker check
            if time.time() < self._circuit_open_until:
                self.metrics["jobs_dropped"] += 1
                self.queue.task_done()
                continue

            try:
                worker_fn(job)
                self.metrics["jobs_processed"] += 1
                self._fail_count = 0  # reset on success

            except Exception:
                self.metrics["jobs_failed"] += 1
                self._fail_count += 1
                logger.exception("L2 worker failed")

                # Retry logic
                retries = job.get("_retries", 0)
                if retries < self.max_retries:
                    job["_retries"] = retries + 1
                    self._safe_put(job)
                else:
                    logger.warning("L2 job permanently failed")

                # Circuit breaker trigger
                if self._fail_count >= self._fail_threshold:
                    self._circuit_open_until = (
                        time.time() + self._reset_seconds
                    )
                    logger.error(
                        "L2 circuit opened due to repeated failures"
                    )

            finally:
                self.queue.task_done()

    # =========================================================
    # SAFE PUT WITH BACKPRESSURE
    # =========================================================
    def _safe_put(self, job):
        try:
            self.queue.put(job, block=False)
        except queue.Full:
            self.metrics["jobs_dropped"] += 1

            if self.drop_policy == "drop_oldest":
                try:
                    self.queue.get_nowait()
                    self.queue.task_done()
                    self.queue.put(job, block=False)
                except Exception:
                    pass
            else:
                logger.warning("L2 queue full — new job dropped")

    # =========================================================
    # PUBLIC API
    # =========================================================
    def add_job(self, job: dict) -> bool:
        try:
            self.queue.put(job, block=False)
            return True
        except queue.Full:
            self.metrics["jobs_dropped"] += 1
            logger.warning("L2 queue full")
            return False

    # Backward compatibility
    def enqueue(self, job: dict) -> bool:
        return self.add_job(job)

    # =========================================================
    # METRICS
    # =========================================================
    def get_status(self):
        return {
            "queue_size": self.queue.qsize(),
            "metrics": dict(self.metrics),
            "circuit_open": time.time() < self._circuit_open_until,
        }

    # =========================================================
    # STOP
    # =========================================================
    def stop(self):
        self._running = False
        for t in self._workers:
            t.join(timeout=2)
        logger.info("L2 stopped cleanly")

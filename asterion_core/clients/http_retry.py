"""HTTP retry helpers for external provider calls."""

import time
from typing import Any, Callable


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failures = 0
        self.last_failure_time = None
        self.state = "closed"

    def call(self, func: Callable, *args, **kwargs) -> Any:
        if self.state == "open":
            if time.time() - self.last_failure_time > self.timeout:
                self.state = "half_open"
            else:
                raise Exception("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            if self.state == "half_open":
                self.state = "closed"
                self.failures = 0
            return result
        except Exception:
            self.failures += 1
            self.last_failure_time = time.time()
            if self.failures >= self.failure_threshold:
                self.state = "open"
            raise


class RetryHttpClient:
    def __init__(
        self,
        client,
        max_retries: int = 3,
        initial_delay: float = 1.0,
        backoff_factor: float = 2.0,
        circuit_breaker: CircuitBreaker | None = None,
        retry_exceptions: tuple[type[Exception], ...] = (Exception,),
    ):
        self.client = client
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.backoff_factor = backoff_factor
        self.circuit_breaker = circuit_breaker
        self.retry_exceptions = retry_exceptions

    def get_json(self, url: str, context: dict = None):
        def _request_with_retry():
            return self._request_with_retry(url, context=context)

        if self.circuit_breaker is not None:
            return self.circuit_breaker.call(_request_with_retry)
        return _request_with_retry()

    def _request_with_retry(self, url: str, *, context: dict | None = None):
        delay = self.initial_delay
        for attempt in range(self.max_retries):
            try:
                return self.client.get_json(url, context=context)
            except self.retry_exceptions:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(delay)
                delay *= self.backoff_factor

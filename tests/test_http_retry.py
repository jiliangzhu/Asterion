import unittest

from asterion_core.clients.http_retry import RetryHttpClient, CircuitBreaker


class _FailingClient:
    def __init__(self, fail_count: int):
        self.fail_count = fail_count
        self.attempts = 0

    def get_json(self, url: str, context: dict = None):
        self.attempts += 1
        if self.attempts <= self.fail_count:
            raise RuntimeError("API error")
        return {"success": True}


class RetryHttpClientTest(unittest.TestCase):
    def test_retry_succeeds_after_failures(self):
        client = _FailingClient(fail_count=2)
        retry_client = RetryHttpClient(client, max_retries=3, initial_delay=0.01)
        result = retry_client.get_json("http://test.com")
        self.assertEqual(result, {"success": True})
        self.assertEqual(client.attempts, 3)

    def test_retry_fails_after_max_retries(self):
        client = _FailingClient(fail_count=5)
        retry_client = RetryHttpClient(client, max_retries=3, initial_delay=0.01)
        with self.assertRaises(RuntimeError):
            retry_client.get_json("http://test.com")
        self.assertEqual(client.attempts, 3)

    def test_retry_client_integrates_with_circuit_breaker(self):
        client = _FailingClient(fail_count=5)
        breaker = CircuitBreaker(failure_threshold=2, timeout=60)
        retry_client = RetryHttpClient(client, max_retries=1, initial_delay=0.01, circuit_breaker=breaker)

        for _ in range(2):
            with self.assertRaises(RuntimeError):
                retry_client.get_json("http://test.com")

        self.assertEqual(breaker.state, "open")
        with self.assertRaises(Exception) as ctx:
            retry_client.get_json("http://test.com")
        self.assertIn("Circuit breaker is OPEN", str(ctx.exception))


class CircuitBreakerTest(unittest.TestCase):
    def test_circuit_opens_after_failures(self):
        cb = CircuitBreaker(failure_threshold=3, timeout=60)

        def failing_func():
            raise RuntimeError("error")

        for _ in range(3):
            with self.assertRaises(RuntimeError):
                cb.call(failing_func)

        self.assertEqual(cb.state, "open")
        with self.assertRaises(Exception) as ctx:
            cb.call(failing_func)
        self.assertIn("Circuit breaker is OPEN", str(ctx.exception))

import pybreaker
import logging
import sys
import time

class CustomCircuitBreaker:
      def __init__(self, failure_threshold=5, recovery_timeout=60, max_attempts=3):
          self.max_attempts = max_attempts
          self.attempt_count = 0
          self.last_failure_count = 0

          self.breaker = pybreaker.CircuitBreaker(
              fail_max=failure_threshold,
              reset_timeout=recovery_timeout,
              exclude=[KeyboardInterrupt],
              name="API_Circuit"
          )

      def call(self, func, *args, **kwargs):
          """Execute function through circuit breaker"""

          # Check if we've hit failure threshold
          if self.breaker.fail_counter >= self.breaker.fail_max:
              self.attempt_count += 1
              if self.attempt_count >= self.max_attempts:
                  logging.critical("Max recovery attempts reached - TERMINATING PROGRAM")
                  sys.exit(1)
              logging.error(f"Circuit breaker open (attempt {self.attempt_count}/{self.max_attempts})")

          try:
              result = self.breaker(func)(*args, **kwargs)
              # Reset attempt counter on success
              if self.attempt_count > 0:
                  logging.info("Circuit breaker recovered - resetting attempt counter")
                  self.attempt_count = 0
              return result

          except pybreaker.CircuitBreakerError:
              logging.error("Circuit breaker is OPEN - waiting for reset")
              time.sleep(self.breaker.reset_timeout)
              raise SystemExit("Circuit breaker blocked call")
from time import time
from typing import Optional


class TimeoutController:
    """A class to control the timeout of a sequence of queries.
    Used to check if the timeout has been reached and to calculate the remaining time.
    """

    def __init__(self, timeout: Optional[float]):
        self.timeout = timeout
        self.start_time = time()

    def check_timeout(self) -> None:
        """Raise a TimeoutError if the timeout has been reached."""
        if self.timeout is not None and time() > (self.start_time + self.timeout):
            raise TimeoutError("Reached timeout executing a query")

    def remaining(self) -> Optional[float]:
        """Return the remaining time before the timeout is reached."""
        return (
            None
            if self.timeout is None
            else max(0.0, self.timeout - (time() - self.start_time))
        )

from typing import Callable, Tuple, Type


def retry(
    fn: Callable,
    max_attempts: int = 3,
    wait_seconds: int = 30,
    retry_exceptions: Tuple[Type[Exception], ...] = (Exception,),
):
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except retry_exceptions as e:
            if attempt < max_attempts:
                print(f"[RETRY] attempt {attempt}/{max_attempts} after error: {e}")
            else:
                raise RuntimeError(f"Failed after {max_attempts} attempts") from e

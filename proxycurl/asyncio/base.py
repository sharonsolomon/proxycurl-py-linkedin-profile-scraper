import asyncio
from asyncio.queues import QueueEmpty
import aiohttp
import json
from proxycurl.config import MAX_WORKERS
from dataclasses import dataclass
from typing import (
    Generic,
    TypeVar,
    List,
    Tuple,
    Callable,
    Dict,
    Optional # Added Optional for value/error
)
import logging

logger = logging.getLogger(__name__)

T = TypeVar('T')
Op = Tuple[Callable, Dict]


@dataclass
class Result(Generic[T]):
    success: bool
    # Value is Optional because it won't exist on failure
    value: Optional[T]
    # Error is Optional because it won't exist on success
    error: Optional[BaseException]
    # Keep your additions for tracking
    phone_number: Optional[str] = None # Make optional if not always present
    email: Optional[str] = None        # Make optional if not always present



class ProxycurlException(Exception):
    """Raised when InternalServerError or network error or request error"""
    pass


class ProxycurlBase:
    api_key: str
    base_url: str
    timeout: int
    max_retries: int
    max_backoff_seconds: int

    def __init__(
        self,
        api_key: str,
        base_url: str,
        timeout: int,
        max_retries: int,
        max_backoff_seconds: int
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.max_backoff_seconds = max_backoff_seconds

    async def request(
        self,
        method: str,
        url: str,
        result_class: Generic[T],
        params: dict = dict(),
        data: dict = dict(),
    ) -> Generic[T]: # Return type is the specific result class (e.g., PersonResult)
        api_endpoint = f'{self.base_url}{url}'
        header_dic = {'Authorization': 'Bearer ' + self.api_key}
        backoff_in_seconds = 1
        last_exception = None # Keep track of the last exception

        for i in range(0, self.max_retries):
            try:
                response_result = None
                status = None
                session_kwargs = {'headers': header_dic, 'timeout': self.timeout}

                async with aiohttp.ClientSession() as session:
                    if method.lower() == 'get':
                        async with session.get(api_endpoint, params=params, **session_kwargs) as response:
                            response_result = await response.read()
                            status = response.status
                    elif method.lower() == 'post':
                         async with session.post(api_endpoint, json=data, **session_kwargs) as response:
                            response_result = await response.read()
                            status = response.status
                    else:
                        # Added to handle potential other methods if library expands
                        raise ValueError(f"Unsupported HTTP method: {method}")

                if status in [200, 202]:
                    response_json = json.loads(response_result)
                    # --- MODIFICATION START ---
                    # Directly attempt to instantiate. If it fails, the exception
                    # will be caught by the outer try/except in this loop iteration
                    # or propagate up if it's the last retry.
                    # We no longer catch the exception here and return the dict.
                    return result_class(**response_json)
                    # --- MODIFICATION END ---
                else:
                    # Raise specific exception for non-2xx status codes
                    raise ProxycurlException(f"HTTP {status}: {response_result.decode('utf-8', errors='ignore')}")

            except (aiohttp.ClientError, asyncio.TimeoutError, ProxycurlException) as e:
                # Catch network errors, timeouts, and specific non-2xx API errors
                logger.warning(f"Request attempt {i+1}/{self.max_retries} failed for {url}. Error: {e}")
                last_exception = e # Store the exception

                # Decide on retry logic based on status or error type
                should_retry = False
                if isinstance(e, ProxycurlException) and status:
                    if status == 429: # Rate limit
                        sleep = (backoff_in_seconds * 2 ** i)
                        await asyncio.sleep(min(self.max_backoff_seconds, sleep))
                        should_retry = True
                    elif status >= 500: # Server errors
                        should_retry = True
                    # Add other retryable status codes if needed
                elif isinstance(e, (aiohttp.ClientError, asyncio.TimeoutError)):
                    # Retry on general network/timeout issues
                    should_retry = True

                if should_retry and i < self.max_retries - 1:
                    await asyncio.sleep(min(self.max_backoff_seconds, (backoff_in_seconds * 2 ** i))) # Exponential backoff
                    continue # Go to the next iteration
                else:
                    raise e # Re-raise the last exception if retries exhausted or not retryable

            except json.JSONDecodeError as e:
                # Catch errors parsing the response body
                logger.error(f"Failed to decode JSON response for {url}. Status: {status}. Response: {response_result}. Error: {e}")
                last_exception = e
                # Generally, don't retry JSON errors unless you expect transient corruption
                raise ProxycurlException(f"Invalid JSON response from API: {e}") from e

            except TypeError as e:
                # --- MODIFICATION START ---
                # Catch errors when **response_json doesn't match result_class fields
                logger.error(f"Failed to map API response to result class for {url}. Status: {status}. Error: {e}. Response: {response_json}")
                last_exception = e
                # Treat this as a non-retryable error for this specific call
                raise ProxycurlException(f"API response structure mismatch: {e}") from e
                # --- MODIFICATION END ---

            except Exception as e:
                # Catch any other unexpected error during the request process
                logger.exception(f"Unexpected error during request to {url}. Attempt {i+1}/{self.max_retries}. Error: {e}")
                last_exception = e
                # Retry potentially unexpected transient errors? Maybe, depending on policy.
                if i < self.max_retries - 1:
                     await asyncio.sleep(min(self.max_backoff_seconds, (backoff_in_seconds * 2 ** i)))
                     continue
                else:
                    raise ProxycurlException(f"Unexpected error after retries: {e}") from e

        # This part should ideally not be reached if the loop always raises or returns
        # But as a fallback, raise the last known exception
        if last_exception:
            raise last_exception
        else:
            # Should not happen if max_retries >= 1
            raise ProxycurlException("Request failed after retries for unknown reasons.")


async def do_bulk(
    ops: List[Op],
    max_workers: int = MAX_WORKERS
) -> List[Result]:
    """Bulk operation (docstring unchanged)"""

    results = [None for _ in range(len(ops))]
    queue = asyncio.Queue()

    # Pre-populate queue with (index, operation_tuple)
    for index, op_tuple in enumerate(ops):
        await queue.put((index, op_tuple))

    # Start workers
    workers = [
        asyncio.create_task(_worker(queue, results))
        for _ in range(max_workers)
    ]

    # Wait for queue to be processed
    await queue.join() # Wait until queue.task_done() has been called for all items

    # Cancel workers (optional, but good practice)
    for w in workers:
        w.cancel()
    # Wait for workers to finish cleanup after cancellation
    await asyncio.gather(*workers, return_exceptions=True)

    # Ensure all results are populated (handle potential edge cases where a worker died unexpectedly)
    # This might be overkill if _worker is robust, but adds safety.
    for i in range(len(results)):
        if results[i] is None:
            # This indicates a worker failed catastrophically without setting a result.
            # Try to get original op for context, might be tricky if queue is empty.
            original_op_info = ops[i][1] if i < len(ops) else {}
            phone = original_op_info.get('phone_number')
            email = original_op_info.get('email')
            logger.error(f"Result missing for index {i}. Worker likely crashed. Input: phone={phone}, email={email}")
            results[i] = Result(False, None, ProxycurlException(f"Worker failed for index {i}"), phone, email)


    return results


async def _worker(queue: asyncio.Queue, results: list):
    while True:
        try:
            # Get a job from the queue
            index, op = await queue.get() # Use await queue.get() for proper async waiting

            # --- MODIFICATION: Extract input identifiers *before* the API call ---
            op_func, op_params = op
            phone_number = op_params.get('phone_number')
            email = op_params.get('email')
            # --- END MODIFICATION ---

            try:
                # Perform the API call, which eventually calls the modified request()
                response_object = await op_func(**op_params) # Should now consistently return the expected object or raise Exception

                # If request() was successful and returned the expected object
                results[index] = Result(
                    success=True,
                    value=response_object,
                    error=None,
                    phone_number=phone_number,
                    email=email
                )

            except Exception as e:
                # Catches any exception raised by request() (network, API error, JSON error, TypeError from mapping)
                # or any other error during the op_func call.
                logger.warning(f"Operation failed for index {index} (phone={phone_number}, email={email}). Error: {e}")
                results[index] = Result(
                    success=False,
                    value=None,
                    error=e, # Store the actual exception
                    phone_number=phone_number,
                    email=email
                )
            finally:
                # Important: Signal that the task from the queue is done
                queue.task_done()

        except asyncio.CancelledError:
            # Allow the worker to exit cleanly if cancelled
            break
        except Exception as e:
            # Catch unexpected errors in the worker loop itself (e.g., queue issues)
            logger.exception(f"Critical error in worker: {e}")
            # Avoid breaking the loop if possible, but log it seriously.
            # If queue.get() fails permanently, the loop might break anyway.
            # Ensure task_done is called if an item was retrieved but processing failed badly.
            # This path needs careful consideration based on desired robustness.
            # For now, we log and continue/break depending on the error type.
            # If it was related to queue.get(), breaking might be necessary.
            # If it happened after getting the item, we might try to signal task_done if applicable.
            # Let's assume for now that critical errors might cause the worker to stop.
            break

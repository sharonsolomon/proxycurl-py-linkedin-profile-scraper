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
    Optional # Keep Optional
)
import logging

logger = logging.getLogger(__name__)

T = TypeVar('T')
# op is a tuple: (callable_api_function, parameters_dict)
# e.g., (proxycurl.linkedin.person.resolve_by_phone, {'phone_number': '...', 'email': None})
# or    (proxycurl.linkedin.person.resolve_by_email, {'phone_number': None, 'email': '...'})
Op = Tuple[Callable, Dict]


@dataclass
class Result(Generic[T]):
    success: bool
    value: Optional[T] # Value exists on success
    error: Optional[BaseException] # Error exists on failure
    # These will store the identifier used for THIS specific operation attempt
    phone_number: Optional[str] = None
    email: Optional[str] = None

class ProxycurlException(Exception):
    """Raised when InternalServerError or network error or request error"""
    pass

class ProxycurlBase:
    # ... (Constructor __init__ remains the same) ...
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

    # ... (request method remains the same as the improved version from the previous step) ...
    # It correctly handles API calls, retries, and raises exceptions on failure
    # including the TypeError -> ProxycurlException mapping if response structure mismatches.
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
            response_result = None
            status = None
            response_json = None # Initialize response_json here
            try:
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
                        raise ValueError(f"Unsupported HTTP method: {method}")

                if status in [200, 202]:
                    response_json = json.loads(response_result)
                    # Directly attempt to instantiate. If it fails, the exception
                    # will be caught by the outer try/except in this loop iteration.
                    return result_class(**response_json)
                else:
                    # Raise specific exception for non-2xx status codes
                    raise ProxycurlException(f"HTTP {status}: {response_result.decode('utf-8', errors='ignore')}")

            except (aiohttp.ClientError, asyncio.TimeoutError, ProxycurlException) as e:
                logger.warning(f"Request attempt {i+1}/{self.max_retries} failed for {url}. Error: {e}")
                last_exception = e
                should_retry = False
                # Simplified retry logic based on status if available
                current_status = status if status else (e.response.status if hasattr(e, 'response') and hasattr(e.response, 'status') else None)

                if isinstance(e, ProxycurlException) and current_status:
                    if current_status == 429: # Rate limit
                        sleep = (backoff_in_seconds * 2 ** i)
                        await asyncio.sleep(min(self.max_backoff_seconds, sleep))
                        should_retry = True
                    elif current_status >= 500: # Server errors
                        should_retry = True
                elif isinstance(e, (aiohttp.ClientError, asyncio.TimeoutError)):
                     # Retry on general network/timeout issues
                    should_retry = True

                if should_retry and i < self.max_retries - 1:
                    # Apply backoff only if retrying
                    await asyncio.sleep(min(self.max_backoff_seconds, (backoff_in_seconds * 2 ** i)))
                    continue
                else:
                    raise # Re-raise the last exception if retries exhausted or not retryable

            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON response for {url}. Status: {status}. Response: {response_result}. Error: {e}")
                last_exception = e
                raise ProxycurlException(f"Invalid JSON response from API: {e}") from e

            except TypeError as e:
                # Catch errors when **response_json doesn't match result_class fields
                logger.error(f"Failed to map API response to result class for {url}. Status: {status}. Error: {e}. Response: {response_json}")
                last_exception = e
                raise ProxycurlException(f"API response structure mismatch: {e}") from e

            except Exception as e:
                logger.exception(f"Unexpected error during request to {url}. Attempt {i+1}/{self.max_retries}. Error: {e}")
                last_exception = e
                if i < self.max_retries - 1:
                     await asyncio.sleep(min(self.max_backoff_seconds, (backoff_in_seconds * 2 ** i)))
                     continue
                else:
                    # Use f-string for better error message formatting
                    raise ProxycurlException(f"Unexpected error after {self.max_retries} retries: {e}") from e

        # Fallback raise if loop finishes without returning or raising explicitly
        # (Should ideally not be reached with proper loop control)
        raise last_exception if last_exception else ProxycurlException("Request failed after retries for unknown reasons.")


# ... (do_bulk function remains the same as the improved version) ...
# It correctly handles queueing, workers, cancellation, and checking for missing results.
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
    for i in range(len(results)):
        if results[i] is None:
            original_op_info = ops[i][1] if i < len(ops) else {}
            phone = original_op_info.get('phone_number')
            email = original_op_info.get('email')
            logger.error(f"Result missing for index {i}. Worker likely crashed. Input: phone={phone}, email={email}")
            # Populate with an error result
            results[i] = Result(
                success=False,
                value=None,
                error=ProxycurlException(f"Worker failed unexpectedly for index {i}"),
                phone_number=phone, # Store identifiers even for crash
                email=email
            )

    return results


async def _worker(queue: asyncio.Queue, results: list):
    """Worker coroutine that processes jobs from the queue."""
    while True:
        phone_number_for_result = None # Initialize identifier for this job
        email_for_result = None      # Initialize identifier for this job
        index = -1                   # Initialize index
        op = None                    # Initialize op

        try:
            # Get a job from the queue (index, (api_func, params_dict))
            index, op = await queue.get()
            op_func, op_params = op

            # --- MODIFICATION START ---
            # Determine the identifier used for THIS specific operation
            # We expect op_params to contain EITHER phone_number OR email
            phone_number_for_result = op_params.get('phone_number')
            email_for_result = op_params.get('email')

            # Optional sanity check: ensure only one identifier is present
            if not ((phone_number_for_result is not None) ^ (email_for_result is not None)):
                 logger.warning(
                    f"Worker (index {index}): Operation parameters have unexpected identifiers. "
                    f"Expected exactly one of phone_number/email. Got: "
                    f"phone={phone_number_for_result}, email={email_for_result}. Proceeding anyway."
                 )
            # --- MODIFICATION END ---

            try:
                # Perform the actual API call using the specific function and its params
                # request() is called within op_func implementation
                response_object = await op_func(**op_params)

                # Success path: API call succeeded and response parsed correctly by request()
                results[index] = Result(
                    success=True,
                    value=response_object,
                    error=None,
                    phone_number=phone_number_for_result, # Use the specific identifier for this op
                    email=email_for_result               # Use the specific identifier for this op
                )

            except Exception as e:
                # Failure path: Catches any exception from op_func -> request()
                # (e.g., network error, timeout, API error status, JSON decode error, structure mismatch error)
                logger.warning(f"Worker (index {index}): Operation failed. Identifier: phone={phone_number_for_result}, email={email_for_result}. Error: {type(e).__name__} - {e}")
                results[index] = Result(
                    success=False,
                    value=None,
                    error=e, # Store the actual exception for later inspection
                    phone_number=phone_number_for_result, # Still store the identifier that failed
                    email=email_for_result               # Still store the identifier that failed
                )

        except asyncio.CancelledError:
            # Allow the worker to exit cleanly if cancelled
            logger.debug("Worker cancelled.")
            break # Exit the loop

        except Exception as e:
            # Catch unexpected errors *within the worker loop itself* (e.g., queue errors)
            logger.exception(f"Critical error in worker loop (index {index}): {e}")
            # If we got an item but failed critically processing it, try to record an error
            if index != -1 and op is not None and results[index] is None:
                 # Attempt to get identifiers again, might fail if op is malformed
                 op_func, op_params = op
                 phone_number_for_result = op_params.get('phone_number')
                 email_for_result = op_params.get('email')
                 results[index] = Result(False, None, ProxycurlException(f"Worker critical error: {e}"), phone_number_for_result, email_for_result)
            # Decide whether to break or continue based on error severity
            break # Let's break on critical worker errors for now

        finally:
            # Crucial: Ensure task_done is called IF an item was retrieved from the queue,
            # regardless of whether processing succeeded or failed.
            if op is not None:
                try:
                    queue.task_done()
                except ValueError:
                    # Can happen if task_done() is called too many times (shouldn't occur with this logic)
                    # or potentially if the queue is already shut down.
                    logger.warning(f"Worker (index {index}): queue.task_done() called inappropriately.")
                except Exception as e:
                    logger.error(f"Worker (index {index}): Error calling queue.task_done(): {e}")

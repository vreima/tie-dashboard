import asyncio
import time
import random
from types import TracebackType


class RateLimiter:
    def __init__(self, amount: int, rate: float):
        self._amount = amount
        self._rate = rate
        self._timestamps = [0.0]

    async def wait(self) -> None:
        """
        Wait if there's need to limit the rate, otherwise return instantly.
        """
        nth_last_ts = self._timestamps[-self._amount :][0]
        time_since_nth_ping = time.monotonic() - nth_last_ts

        if time_since_nth_ping < self._rate:
            await asyncio.sleep(self._rate - time_since_nth_ping)

    def ping(self) -> None:
        """
        Notify RateLimiter instance that an event has occured.
        """
        self._timestamps.append(time.monotonic())


async def actual_get(request, result):
    print(f"sending {request}...")
    value = random.random() * 5.0
    await asyncio.sleep(value)
    await result.put(f"valmis: {value}")


async def worker(queue):
    n = 10
    s = 1.0
    times = [0]

    print("worker")
    while True:
        request, result = await queue.get()

        nth_last = times[-n:][0]
        delta = time.monotonic() - nth_last
        if delta < s:
            x = s - delta
            print(f"{delta:.3f} < {s:.1f}! nukutaan {x:.3f}s... ", end="")
            await asyncio.sleep(x)
            print("ok")
        else:
            print(f"no need to wait: {delta:.3f}")

        times.append(time.monotonic())
        asyncio.create_task(actual_get(request, result))
        # ... fire and forget ...
        queue.task_done()


class Limiter:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.worker = None

    async def __aenter__(self):
        self.worker = asyncio.create_task(worker(self.queue))
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ):
        if self.worker:
            self.worker.cancel()

    async def get(self, request):
        print(f"getting {request}")
        result_queue = asyncio.Queue()
        await self.queue.put((request, result_queue))
        print(await result_queue.get())


async def f():
    print("A")
    async with Limiter() as l:
        print("B")
        gets = [asyncio.create_task(l.get(f"req {i}")) for i in range(25)]
        print("D")
        await asyncio.gather(*gets)

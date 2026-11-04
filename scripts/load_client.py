"""独立 aiohttp 压测客户端：从 agentgate(Python 版) scripts/load_test.py 抽取的加载核心。

口径与 Python 版逐字对齐（同一把尺子量两个网关）：
- c=1 串行 N 请求 → 每请求开销的合法口径（无排队）；
- c=并发容量组 → 吞吐 / 分位数 / TTFT；
- 附加开销 = 过网关中位 − 直连基线中位（客户端成本在差值中抵消）。

用法：
    python load_client.py --target http://127.0.0.1:8080 --mode c1 --requests 100
    python load_client.py --target http://127.0.0.1:8080 --mode capacity --requests 1000 --concurrency 50
    （加 --key sk-xxx 打带鉴权的网关；打裸 mock 不带）
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time

import aiohttp


def _pctl(sorted_xs: list[float], p: float) -> float:
    if not sorted_xs:
        return 0.0
    idx = min(len(sorted_xs) - 1, round(p / 100 * (len(sorted_xs) - 1)))
    return sorted_xs[idx]


def _fmt(xs: list[float]) -> str:
    if not xs:
        return "-"
    s = sorted(xs)
    return f"P50={_pctl(s,50):.1f} P95={_pctl(s,95):.1f} P99={_pctl(s,99):.1f} mean={statistics.mean(s):.1f}"


async def run_load(target: str, n: int, concurrency: int, stream: bool, key: str | None):
    headers = {"Authorization": f"Bearer {key}"} if key else {}
    body = {"model": "mock-model", "messages": [{"role": "user", "content": "hi" * 10}]}
    if stream:
        body["stream"] = True

    latencies: list[float] = []
    ttfts: list[float] = []
    statuses: dict[int, int] = {}
    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=0, limit_per_host=0)

    async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=30)) as session:

        async def one():
            async with sem:
                t0 = time.perf_counter()
                try:
                    if stream:
                        ttft = None
                        async with session.post(f"{target}/v1/chat/completions", json=body, headers=headers) as resp:
                            statuses[resp.status] = statuses.get(resp.status, 0) + 1
                            async for _ in resp.content.iter_any():
                                if ttft is None:
                                    ttft = (time.perf_counter() - t0) * 1000
                        if ttft is not None:
                            ttfts.append(ttft)
                    else:
                        async with session.post(f"{target}/v1/chat/completions", json=body, headers=headers) as resp:
                            statuses[resp.status] = statuses.get(resp.status, 0) + 1
                            await resp.read()
                    latencies.append((time.perf_counter() - t0) * 1000)
                except Exception:
                    statuses[-1] = statuses.get(-1, 0) + 1

        t = time.perf_counter()
        await asyncio.gather(*[one() for _ in range(n)])
        duration = time.perf_counter() - t

    return duration, latencies, ttfts, statuses


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True)
    parser.add_argument("--mode", choices=["c1", "capacity"], required=True)
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--stream-ratio", type=float, default=0.3)
    parser.add_argument("--key", default=None)
    parser.add_argument("--label", default="")
    args = parser.parse_args()

    if args.mode == "c1":
        dur, lat, _, st = await run_load(args.target, args.requests, 1, False, args.key)
        print(f"[{args.label or args.target}] c=1 n={args.requests} "
              f"{_fmt(lat)} statuses={st} ({args.requests/dur:.0f} req/s)")
    else:
        n_stream = int(args.requests * args.stream_ratio)
        n_plain = args.requests - n_stream
        d_dur, d_lat, _, d_st = await run_load(args.target, n_plain, args.concurrency, False, args.key)
        s_dur, s_lat, s_ttft, s_st = await run_load(args.target, n_stream, args.concurrency, True, args.key)
        print(f"[{args.label or args.target}] c={args.concurrency} 非流式 n={n_plain} "
              f"{n_plain/d_dur:.0f} req/s {_fmt(d_lat)} st={d_st}")
        print(f"[{args.label or args.target}] c={args.concurrency} 流式 n={n_stream} "
              f"{(n_stream/s_dur if s_dur else 0):.0f} req/s TTFT {_fmt(s_ttft)} st={s_st}")


if __name__ == "__main__":
    asyncio.run(main())

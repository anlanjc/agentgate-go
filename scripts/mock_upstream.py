"""独立 SSE mock 上游：模拟 OpenAI /v1/chat/completions（流式 20 chunk + [DONE] / 非流式 JSON）。

双语言对照压测用：Python 版网关（in-process mock）与 Go 版网关（本独立进程）
吃同一个响应形态，客户端差值口径才可比。

用法：python mock_upstream.py --port 9301
"""

from __future__ import annotations

import argparse

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI()

_CHUNKS = 20


@app.post("/v1/chat/completions")
async def chat(request: Request):
    body = await request.json()
    if body.get("stream"):
        async def gen():
            for i in range(_CHUNKS):
                yield f'data: {{"id":"cmpl-mock","choices":[{{"delta":{{"content":"tok{i}"}}}}]}}\n\n'
            yield "data: [DONE]\n\n"

        return StreamingResponse(gen(), media_type="text/event-stream")
    return JSONResponse({"id": "cmpl-mock", "choices": [{"message": {"content": "ok" * 10}}], "usage": {"prompt_tokens": 10, "completion_tokens": 10}})


@app.get("/healthz")
async def health():
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=9301)
    args = parser.parse_args()
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="error", access_log=False)

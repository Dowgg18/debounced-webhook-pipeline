"""Fire a realistic burst at the demo and watch four deliveries become one call.

    python demo/burst.py
"""

from __future__ import annotations

import asyncio
import sys
import uuid

import httpx

URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000/webhook"
CHAT = f"chat-{uuid.uuid4().hex[:6]}"

# How people actually type: fragments, fast, plus one duplicate delivery because
# the provider did not see our 200 in time.
SCRIPT = [
    ("hi", 0.0),
    ("saw the listing", 0.4),
    ("is it still available", 0.5),
    ("?", 0.3),
]


async def main() -> None:
    async with httpx.AsyncClient(timeout=10) as client:
        sent = []
        for text, gap in SCRIPT:
            await asyncio.sleep(gap)
            message_id = uuid.uuid4().hex
            body = {
                "type": "message.received",
                "timestamp": None,
                "message": {"id": message_id, "chat": CHAT, "text": text},
            }
            r = await client.post(URL, json=body)
            sent.append(body)
            print(f"-> {text!r} {r.json()}")

        # Provider retry of the second message.
        r = await client.post(URL, json=sent[1])
        print(f"-> duplicate of {sent[1]['message']['text']!r} {r.json()}")

    print(f"\nconversation {CHAT}: 5 deliveries sent, expect ONE handler call with 4 messages")
    print("watch the uvicorn log.")


if __name__ == "__main__":
    asyncio.run(main())

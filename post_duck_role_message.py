"""Post the duck role onboarding message. Usage: python post_duck_role_message.py [channel_id]"""

import asyncio
import sys

from fishin_tiffin.post_duck_role_message import post_duck_role_message


def _parse_args(argv: list[str]) -> int | None:
    if len(argv) not in (1, 2):
        raise ValueError("Usage: python post_duck_role_message.py [channel_id]")
    return int(argv[1]) if len(argv) == 2 else None


if __name__ == "__main__":
    try:
        channel_id = _parse_args(sys.argv)
    except ValueError as exc:
        print(exc)
        raise SystemExit(1) from exc
    asyncio.run(post_duck_role_message(channel_id))

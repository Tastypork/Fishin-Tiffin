"""
Keish's ENERGY — rare proc, short window with no !duck cooldown (display uses half of real remaining).
"""

from .duck_clock import utc_ts
from .paths import ASSETS_DIR

BLESSING_PROB = 0.01
BLESSING_DURATION_SECONDS = 20
BLESSING_DISPLAY_SECONDS = BLESSING_DURATION_SECONDS // 2
KEISH_ENERGY_GIF_PATH = ASSETS_DIR / "keish_energy.gif"

KEISH_PROC_TITLE = "A wild Keish appears! SPAM DUCK!!!"
KEISH_SNAG_TITLE = "😮 Keish Snags A Duck From The Sky For You!"
KEISH_SADDLE_TITLE = "✨😮 Keish Saddles A Shiny Duck For You! ✨"


def blessing_proc_description() -> str:
    return (
        "The water stills—then the **spirit of thrill** sparks along your spine. "
        f"**No `!duck` cooldowns** for the next **{BLESSING_DISPLAY_SECONDS} seconds**; the pond can't hold you back."
    )


class KeishEnergy:
    """Runtime state for Keish blessing windows."""

    __slots__ = ("_until",)

    def __init__(self) -> None:
        self._until: dict[str, int] = {}

    def active(self, user_id: str) -> bool:
        until = self._until.get(user_id)
        if until is None:
            return False
        now = utc_ts()
        if now >= until:
            del self._until[user_id]
            return False
        return True

    def remaining(self, user_id: str) -> int:
        until = self._until.get(user_id)
        if until is None:
            return 0
        return max(0, until - utc_ts())

    def remaining_display(self, user_id: str) -> int:
        return self.remaining(user_id) // 2

    def grant(self, user_id: str) -> None:
        self._until[user_id] = utc_ts() + BLESSING_DURATION_SECONDS

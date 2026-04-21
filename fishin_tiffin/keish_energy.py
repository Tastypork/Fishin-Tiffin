"""
Keish's ENERGY — rare proc, then three special !duck pulls (each adds several new ducks).
"""

import random
from pathlib import Path

from .paths import ASSETS_DIR

KEISH_ENERGY_DIR = ASSETS_DIR / "keish_energy"
KEISH_ENERGY_GIF_PATH = KEISH_ENERGY_DIR / "keish_energy.gif"

KEISH_FLOCK_ROLLS = 3
KEISH_FLOCK_MIN = 3
KEISH_FLOCK_MAX = 7

KEISH_SUCCESS_FILENAMES = (
    "keish_success.png",
    "keish_success1.png",
    "keish_success2.png",
)

KEISH_PROC_TITLE = "A wild Keish appears!"
KEISH_FLOCK_CATCH_TITLE = "Keish snags a flock of ducks for you!"


def keish_proc_description() -> str:
    return (
        "The pond **boils with fortune**—**he's hauling** a storm of catches for you. "
    )


def pick_keish_success_image_path() -> Path | None:
    """One of the Keish success PNGs at random, if any exist on disk."""
    existing = [KEISH_ENERGY_DIR / name for name in KEISH_SUCCESS_FILENAMES if (KEISH_ENERGY_DIR / name).is_file()]
    if not existing:
        return None
    return random.choice(existing)


class KeishEnergy:
    """Runtime state: remaining flock pulls after a Keish proc."""

    __slots__ = ("_rolls_left",)

    def __init__(self) -> None:
        self._rolls_left: dict[str, int] = {}

    def active(self, user_id: str) -> bool:
        return self._rolls_left.get(user_id, 0) > 0

    def flock_rolls_remaining(self, user_id: str) -> int:
        return self._rolls_left.get(user_id, 0)

    def grant(self, user_id: str) -> None:
        self._rolls_left[user_id] = KEISH_FLOCK_ROLLS

    def consume_one_roll(self, user_id: str) -> None:
        left = self._rolls_left.get(user_id, 0)
        if left <= 0:
            return
        if left <= 1:
            self._rolls_left.pop(user_id, None)
        else:
            self._rolls_left[user_id] = left - 1

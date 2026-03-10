"""
Reference registry package.

Exports:
- n-alkanes helpers (real implementation today)
- placeholders for other families (steranes, hopanes, fatty_acids, terpanes, phenanthrenes)
"""

from .ref_n_alkanes import (
    ensure_ref_tables as ensure_ref_n_alkanes,
    seed_from_csvs as seed_n_alkanes_from_csvs,
)

# Placeholders (no seeding yet) — each module exposes `ensure_ref_tables(conn)`
from .steranes import ensure_ref_tables as ensure_ref_steranes
from .hopanes import ensure_ref_tables as ensure_ref_hopanes
from .fatty_acids import ensure_ref_tables as ensure_ref_fatty_acids
from .terpanes import ensure_ref_tables as ensure_ref_terpanes
from .phenanthrenes import ensure_ref_tables as ensure_ref_phenanthrenes

__all__ = [
    "ensure_ref_n_alkanes",
    "seed_n_alkanes_from_csvs",
    "ensure_ref_steranes",
    "ensure_ref_hopanes",
    "ensure_ref_fatty_acids",
    "ensure_ref_terpanes",
    "ensure_ref_phenanthrenes",
]
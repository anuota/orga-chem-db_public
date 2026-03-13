import logging
import re
import unicodedata
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

# your one-shot map from earlier
ANALYSIS_FRACTION_MAP = {
    # Whole oil
    "whole_oil": "whole crude oil",
    "whole_oil_gc": "whole crude oil",
    "wo": "whole crude oil",
    # Aliphatic
    "hopanes": "aliphatic",
    "steranes": "aliphatic",
    "n-alkanes": "aliphatic",
    "alkanes": "aliphatic",
    "n_alkanes_isoprenoids": "aliphatic",
    "n-alkanes,isoprenoids": "aliphatic",
    "terpanes": "aliphatic",
    "tricyclic_terpanes": "aliphatic",
    "tricylcic_terpanes": "aliphatic",
    "diamondoids": "aliphatic",
    "diamandoids": "aliphatic",
    "biomarkers": "aliphatic",
    "biomarkers_aliphatic": "aliphatic",
    "norcholestanes": "aliphatic",
    # Aromatic
    "naphthalenes": "aromatic",
    "naphtalenes": "aromatic",
    "phenanthrenes": "aromatic",
    "phenenthrenes": "aromatic",
    "dibenzothiophenes": "aromatic",
    "thiophenes": "aromatic",
    "fluorenes": "aromatic",
    "flourenes": "aromatic",
    "biphenyls": "aromatic",
    "bipheniles": "aromatic",
    "alkylbenzenes": "aromatic",
    "aromatic_steroids": "aromatic",
    "aromatic steroids": "aromatic",
    "triaromatic_steroids": "aromatic",
    "triaromatic steroids": "aromatic",
    # NSO
    "carbazoles": "NSO",
    "alcohols": "NSO",
    "fatty_acids": "NSO",
    "fatty acids": "NSO",
    "fames": "NSO",
    "ebfas": "NSO",
    "etherlipids": "NSO",
    "archaeolipids": "NSO",
    "archaelipids": "NSO",
}

_CANON_EXTRA = {
    # hard alias corrections before fuzzy
    "wo": "whole_oil",
    "whole oil": "whole_oil",
    "whole_oil": "whole_oil",
    "whole_oil_gc": "whole_oil",
    "whole oil gc": "whole_oil",
    "tricyclic terpanes": "tricyclic_terpanes",
    "tricylcic terpanes": "tricyclic_terpanes",
    "flourenes": "fluorenes",
    "bipheniles": "biphenyls",
    "naphtalenes": "naphthalenes",
    "phenenthrenes": "phenanthrenes",
    "archaelipids": "archaeolipids",
    "fatty acids": "fatty_acids",
    "fattyacids": "fatty_acids",
    "aromaticsteroids": "aromatic_steroids",
    "n-alkanes": "alkanes",
    "n_alkanes": "alkanes",
    "alkanes": "alkanes",
    "n_alkanes_isoprenoids": "alkanes",
    "n-alkanes,isoprenoids": "alkanes",
}

# Regex for stripping GC instrument prefixes from filenames
_INSTRUMENT_RE = re.compile(r"^(?:gcfid|gcmrmms|gcms)[_\s-]+", re.IGNORECASE)
# Regex for stripping chromatographic section/fraction prefixes
_SECTION_RE = re.compile(
    r"^(?:aliphatic|aromatic|nso(?:silyl)?)[_\s-]+", re.IGNORECASE
)


def _basic_normalize(name: str) -> str:
    s = name.strip().lower()
    s = s.replace("(", " ").replace(")", " ")
    s = re.sub(r"[_\-\s]+", "_", s)
    s = re.sub(r"(_?combined|_?concentration)$", "", s)  # drop suffixes
    s = s.strip("_")
    return s


def derive_table_from_filename(path: str) -> str:
    """Derive a canonical table name from a CSV filename.

    Handles both old-style names (``Hopanes_combined (Area).csv``) and
    new GC-prefixed names (``GCMS-aromatic_naphthalenes_combined (Area).csv``).

    Steps:
      1. Strip extension and ``_combined`` marker.
      2. Strip trailing data-type marker ``(Area)`` / ``(Concentration)``.
      3. Strip GC instrument prefix (GCFID-, GCMRMMS-, GCMS-).
      4. Strip section/fraction prefix (aliphatic_, aromatic_, nso_, nsosilyl_).
      5. Normalise via :func:`normalize_analysis`.
    """
    import os

    name = os.path.basename(path)
    stem = name.rsplit(".", 1)[0]
    # Drop _combined marker
    base = stem.split("_combined", 1)[0]
    # Drop trailing (Area) / (Concentration) if _combined was absent
    base = re.sub(r"\s*\((?:Area|Concentration|concentration)\)\s*$", "", base)
    # Strip instrument prefix  e.g. "GCFID-" / "GCMS-" / "GCMRMMS-"
    base = _INSTRUMENT_RE.sub("", base)
    # Strip section prefix     e.g. "aliphatic_" / "aromatic_" / "NSO_"
    base = _SECTION_RE.sub("", base)
    # Standard char cleanup
    base = (
        base.replace("(", " ")
        .replace(")", " ")
        .replace("-", " ")
        .replace("N-", "")
        .replace("n-", "")
        .strip()
        .lower()
        .replace(" ", "_")
    )
    return normalize_analysis(base)


def _fuzzy_best_match(
    key: str, choices: Iterable[str], threshold: float = 0.85
) -> Optional[str]:
    """
    Return best fuzzy match >= threshold (0..1 range). None if no good match.
    Tries rapidfuzz (fast, good) -> difflib (stdlib fallback).
    """
    try:
        # Prefer rapidfuzz if available
        from rapidfuzz import fuzz, process

        result = process.extractOne(
            key, choices, scorer=fuzz.WRatio, score_cutoff=int(threshold * 100)
        )
        return result[0] if result else None
    except Exception:
        # Fallback to difflib
        import difflib

        # difflib returns 0..1 similarity via SequenceMatcher
        best = None
        best_score = 0.0
        for c in choices:
            score = difflib.SequenceMatcher(None, key, c).ratio()
            if score > best_score:
                best, best_score = c, score
        return best if best and best_score >= threshold else None


def normalize_analysis(name: str, known_keys: Optional[Iterable[str]] = None) -> str:
    """
    Normalize an analysis label/filename to a canonical key used in ANALYSIS_FRACTION_MAP.
    Applies:
      1) string cleanup,
      2) hard alias fixes,
      3) fuzzy match against known keys (defaults to ANALYSIS_FRACTION_MAP.keys()).
    Never raises; returns the cleaned key if no good fuzzy match exists.
    """
    cleaned = _basic_normalize(name)
    # Hard alias pass
    if cleaned in _CANON_EXTRA:
        return _CANON_EXTRA[cleaned]

    # Exact hit?
    keys = (
        list(known_keys)
        if known_keys is not None
        else list(ANALYSIS_FRACTION_MAP.keys())
    )
    if cleaned in keys:
        return cleaned

    # Fuzzy match
    best = _fuzzy_best_match(cleaned, keys, threshold=0.86)
    return best or cleaned


# --- Fraction helpers & consistency checks ---
FRACTION_CODE_TO_LABEL = {
    "0": "whole extract from rock",
    "1": "aliphatic",
    "2": "aromatic",
    "3": "NSO",
}

LABEL_TO_FRACTION_CODE = {v: k for k, v in FRACTION_CODE_TO_LABEL.items()}


def _strip_accents(text: str) -> str:
    """ASCII-fold diacritics (e.g., 'Öl' -> 'Oil')."""
    return "".join(
        ch
        for ch in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(ch)
    )


# Phrase-level fixes are applied before token replacements
_TYPE_PHRASES = {
    "standard öl": "standard oil",
    "standard ol": "standard oil",
}

# Token-level fixes (apply with word boundaries after folding accents)
_TYPE_FIXES = {
    # Mojibake/encoding
    "standardöl": "standard oil",
    "standardol": "standard oil",
    "öl": "oil",
    "Ă–l": "oil",
    "Ã–l": "oil",
    "ol": "oil",
    "oel": "oil",
    # German -> English
    "gestein": "rock",
    "muttergestein": "source rock",
    "kern": "core",
    "feststoff": "solid",
    "schiefer": "shale",
}


def normalize_type_label(val: object) -> str | None:
    """
    Normalize CSV 'Type' values:
      - Fix mojibake (e.g., 'StandardĂ¶l' → 'standard oil')
      - Translate common German terms (e.g., 'Gestein' → 'rock')
      - Lowercase and trim
    Returns None for empty/None.
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None

    # Common mojibake repairs for ö / Ö before lowercasing
    s = s.replace("Ă¶", "o").replace("Ã¶", "o").replace("Ă–", "o").replace("Ã–", "o")

    s_low = s.lower()

    # Phrase-level fixes first (work on non–accent-stripped to keep spaces)
    for phrase, repl in _TYPE_PHRASES.items():
        if phrase in s_low:
            s_low = s_low.replace(phrase, repl)

    # Accent fold for robust matching (öl -> ol)
    s_fold = _strip_accents(s_low)

    # Word-boundary replacements
    def _wb_sub(text: str, pat: str, repl: str) -> str:
        return re.sub(rf"\b{re.escape(pat)}\b", repl, text)

    out = s_fold
    for k, v in _TYPE_FIXES.items():
        out = _wb_sub(out, _strip_accents(k), v)

    # Final cleanup & lowercase
    out = " ".join(out.split()).strip().lower()
    # Collapse any lingering 'ol' to 'oil'
    if out == "ol":
        out = "oil"
    return out or s_low


def explicit_fraction_from_sample(
    raw_sample: str | None,
) -> tuple[str | None, str | None]:
    """
    If the SampleNumber explicitly encodes a leading fraction code after GXXXXXX
    (e.g., "G003200-1", "G003200-2"), return (label, code). Otherwise (None, None).
    The function is tolerant to en/em-dash and spacing: "G003200 – 2".
    """
    if raw_sample is None:
        return None, None
    s = str(raw_sample).strip()
    m = re.search(r"(G\d{6})", s, flags=re.IGNORECASE)
    if not m:
        return None, None
    suffix = (s[m.end() :] or "").strip()
    suffix_norm = suffix.replace("–", "-").replace("—", "-").replace("-", "-")
    m2 = re.match(r"^-([0-3])", suffix_norm)
    if not m2:
        return None, None
    code = m2.group(1)
    return FRACTION_CODE_TO_LABEL.get(code), code


def inferred_fraction_from_analysis(analysis: str | None) -> str | None:
    """
    Infer default fraction label from an analysis/table name using ANALYSIS_FRACTION_MAP
    with normalization+fuzzy matching. Returns the label or None if unknown.
    """
    if not analysis:
        return None
    key = normalize_analysis(str(analysis))
    return ANALYSIS_FRACTION_MAP.get(key)


def check_fraction_consistency(
    raw_sample: str | None, analysis: str | None
) -> tuple[bool | None, str | None, str | None, str | None]:
    explicit_label, explicit_code = explicit_fraction_from_sample(raw_sample)
    inferred_label = inferred_fraction_from_analysis(analysis)
    if explicit_label is None:
        return None, None, inferred_label, None
    if inferred_label is None:
        return None, explicit_label, None, explicit_code
    return (
        (explicit_label == inferred_label),
        explicit_label,
        inferred_label,
        explicit_code,
    )


def _clean_cell(val):
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s.lower() in {"na", "n.a.", "n.a", "null"}:
        return None
    # collapse thousands separators like "412 219" → "412219"
    if re.match(r"^\d[\d\s.,]*$", s):
        s2 = s.replace(" ", "")
        try:
            if s2.count(",") > 0 and s2.count(".") == 0:
                s2 = s2.replace(",", ".")
            return float(s2)
        except Exception:
            return s
    return s


def _parse_date_like(val) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    fmts = [
        "%Y-%m-%d",
        "%d.%m.%Y",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%m-%d-%Y",
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f).strftime("%Y-%m-%d")
        except Exception:
            pass
    return None


def normalize_sample_number(raw: str | None) -> str | None:
    """
    Extract canonical sample id: first occurrence of G followed by 6 digits (case-insensitive).
    e.g. 'G000392-1a' -> 'G000392', 'g001953 (dup)' -> 'G001953'.
    """
    if raw is None:
        return None
    m = re.search(r"(G\d{6})", str(raw), flags=re.IGNORECASE)
    return m.group(1).upper() if m else None


def extract_base_fraction_notes(
    raw: str | None, analysis: str | None = None
) -> tuple[str | None, str | None, str | None]:
    """
    From a raw SampleNumber (e.g., 'G003209-1', 'G003209_2', 'G003209 wdh', 'G003209-1a'),
    return (base_id, fraction_label, notes) where:
      - base_id is canonical 'GXXXXXX'
      - fraction_label derives from leading dash code just after the base:
          no -N  -> "whole crude oil" (unless inferred from `analysis`)
          -0    -> "whole extract from rock"
          -1    -> "aliphatic"
          -2    -> "aromatic"
          -3    -> "NSO"
      - notes collects remaining suffix tokens (e.g., '_1', 'wdh', trailing letters)
    If there is no explicit -N code and no notes, infer fraction from the analysis/table name
    using ANALYSIS_FRACTION_MAP (after normalize_analysis).

    See also: explicit_fraction_from_sample(), inferred_fraction_from_analysis(), check_fraction_consistency().
    """
    if raw is None:
        return None, None, None
    s = str(raw).strip()
    m = re.search(r"(G\d{6})", s, flags=re.IGNORECASE)
    if not m:
        return None, None, None

    base = m.group(1).upper()
    suffix = (s[m.end() :] or "").strip()

    # Default fraction
    fraction = "whole crude oil"
    notes_tokens: list[str] = []

    # Normalize dash variants
    suffix_norm = suffix.replace("–", "-").replace("—", "-").replace("-", "-")

    # If suffix begins with -[0-3], set fraction and strip that part
    m2 = re.match(r"^-([0-3])", suffix_norm)
    consumed = 0
    if m2:
        code = m2.group(1)
        consumed = m2.end()
        if code == "0":
            fraction = "whole extract from rock"
        elif code == "1":
            fraction = "aliphatic"
        elif code == "2":
            fraction = "aromatic"
        elif code == "3":
            fraction = "NSO"

    # Remaining tail after optional leading -N
    tail = suffix_norm[consumed:].strip()
    if tail:
        cleaned = re.sub(r"[\s,;]+", " ", tail)
        tokens = [t for t in re.split(r"\s+", cleaned) if t]
        notes_tokens.extend(tokens)

    notes = " ".join(notes_tokens) if notes_tokens else None

    # If no explicit -N and no notes, infer from normalized analysis/table name
    if not m2 and notes is None and analysis:
        analysis_key = normalize_analysis(str(analysis))
        inferred = ANALYSIS_FRACTION_MAP.get(analysis_key)
        if inferred:
            fraction = inferred

    return base, fraction, notes

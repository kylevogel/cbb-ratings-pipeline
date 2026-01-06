import os
import re
import unicodedata
import pandas as pd

TEAM_ALIAS_PATH = "team_alias.csv"


def _strip_diacritics(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def _normalize_text(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x)
    s = s.replace("\u2019", "'").replace("\u2018", "'").replace("\u201c", '"').replace("\u201d", '"')
    s = _strip_diacritics(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _clean_bpi_team_name(raw) -> str:
    s = _normalize_text(raw)
    if not s:
        return s
    s = re.sub(r"\bA&\b", "A&M", s)
    s_nospace = s.replace(" ", "")
    if len(s_nospace) >= 6 and len(s_nospace) % 2 == 0:
        half = len(s_nospace) // 2
        if s_nospace[:half].upper() == s_nospace[half:].upper():
            return s_nospace[:half]
    m = re.match(r"^(.*?)([A-Z][A-Z0-9&'.-]{1,6})$", s)
    if m:
        base = m.group(1).strip()
        if len(base) >= 3:
            s = base
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _clean_generic_team_name(raw) -> str:
    s = _normalize_text(raw)
    if not s:
        return s
    s = re.sub(r"\s+", " ", s).strip()
    return s


def load_team_alias(path: str = TEAM_ALIAS_PATH) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    return df


def standardize_team_names(df: pd.DataFrame, source_col: str, source: str, alias_df: pd.DataFrame | None = None) -> pd.DataFrame:
    out = df.copy()
    if source_col not in out.columns:
        raise ValueError(f"Column '{source_col}' not found in df")
    if alias_df is None:
        alias_df = load_team_alias()

    if source.lower() == "bpi":
        clean_fn = _clean_bpi_team_name
    else:
        clean_fn = _clean_generic_team_name

    if not alias_df.empty:
        canonical_col = None
        for cand in ["team", "canonical_team", "canonical"]:
            if cand in alias_df.columns:
                canonical_col = cand
                break
        if canonical_col is None:
            canonical_col = alias_df.columns[0]
        source_col_in_alias = source if source in alias_df.columns else None
        name_to_canonical: dict[str, str] = {}
        for _, row in alias_df.iterrows():
            canonical = _clean_generic_team_name(row.get(canonical_col, ""))
            if not canonical:
                continue
            name_to_canonical[_clean_generic_team_name(canonical).lower()] = canonical
            if source_col_in_alias:
                raw_alias = row.get(source_col_in_alias, "")
                if raw_alias is not None and not (isinstance(raw_alias, float) and pd.isna(raw_alias)):
                    alias_clean = clean_fn(raw_alias)
                    if alias_clean:
                        name_to_canonical[alias_clean.lower()] = canonical
    else:
        name_to_canonical = {}

    cleaned = out[source_col].apply(clean_fn)
    out["team"] = cleaned.apply(lambda s: name_to_canonical.get(s.lower(), s))
    return out

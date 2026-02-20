import re
import unicodedata
import pandas as pd

MONTH_RE = re.compile(r"^\d{4}M\d{2}$", re.IGNORECASE)

def get_conn():
    # Puedes mover aquí tu get_conn o importarlo desde db
    from etl.db import get_conn as db_get_conn
    return db_get_conn()

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"\s+", " ", s)
    return s

def parse_month(col: str):
    # Convierte "2025M01" en (2025, 1, 1)
    y = int(col[:4])
    m = int(col[-2:])
    t = (m - 1) // 3 + 1
    return y, m, t

def to_number(x):
    if pd.isna(x):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return None
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return None

def find_month_columns(df):
    month_cols = []
    for i in range(min(30, df.shape[0])):
        for j in range(df.shape[1]):
            val = df.iloc[i, j]
            if isinstance(val, str) and MONTH_RE.match(val.strip()):
                month_cols.append((j, val.strip()))
        if month_cols:
            break
    return month_cols
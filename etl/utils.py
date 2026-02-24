import re
import unicodedata
import pandas as pd

MONTH_RE = re.compile(r"\d{4}M\d{2}", re.IGNORECASE)

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"\s+", " ", s)
    return s

def parse_month(col: str):
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
    # Busca en todo el archivo, sin límite de 30 filas
    for i in range(df.shape[0]):
        for j in range(df.shape[1]):
            val = df.iloc[i, j]
            if isinstance(val, str):
                # Quitamos espacios y buscamos el patrón
                limpio = val.strip().replace(" ", "")
                match = MONTH_RE.search(limpio)
                if match:
                    # Guardamos la versión limpia (ej: '2023M11')
                    month_cols.append((j, match.group(0)))
        
        if month_cols:
            break
            
    return month_cols

def month_name_es(m: int) -> str:
    names = ["enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre"]
    return names[m-1] if 1 <= m <= 12 else ""

def first_day_of_month(y: int, m: int):
    # devolvemos string YYYY-MM-01 (DATE lo parsea bien)
    return f"{y:04d}-{m:02d}-01"
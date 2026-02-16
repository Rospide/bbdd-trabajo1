import re
from pathlib import Path
import pandas as pd
from etl.db import get_conn

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_FILE = BASE_DIR / "data" / "23988.xlsx"

YEAR_RE = re.compile(r"^\d{4}$")

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

def load_excel():
    return pd.read_excel(DATA_FILE, sheet_name=0, header=None)

def find_year_columns(df):
    year_cols = []
    for i in range(min(30, df.shape[0])):
        for j in range(df.shape[1]):
            val = df.iloc[i, j]

            # ignorar NaN
            if pd.isna(val):
                continue

            # años como número (2025.0, etc.)
            if isinstance(val, (int, float)):
                v = int(val)
                if 1900 <= v <= 2100:
                    year_cols.append((j, str(v)))

            # años como texto ("2025")
            elif isinstance(val, str) and YEAR_RE.match(val.strip()):
                year_cols.append((j, val.strip()))

        if year_cols:
            break
    return year_cols

def extract_rows(df):
    year_cols = find_year_columns(df)
    if not year_cols:
        raise RuntimeError("No he encontrado columnas de año (2025, 2024...). Revisa el Excel.")

    records = []
    col0 = 0

    metrics_map = {
        "dato base": "numero_turistas",
        "tasa de variación anual": "variacion_anual",
    }

    current_ccaa = None
    current_metric = None

    for i in range(df.shape[0]):
        cell = df.iloc[i, col0]

        if isinstance(cell, str):
            s = cell.strip()
            s_low = s.lower()

            # Métrica
            if s_low in metrics_map:
                current_metric = metrics_map[s_low]
            else:
                # Detectar comunidad: suele empezar con "01 Andalucia" etc.
                if re.match(r"^\d{2}\s+", s):
                    current_ccaa = s
                    current_metric = None

        if current_ccaa and current_metric:
            for j, year_str in year_cols:
                val = to_number(df.iloc[i, j])
                if val is None:
                    continue
                records.append({
                    "comunidad": current_ccaa,
                    "anio": int(year_str),
                    "metric": current_metric,
                    "value": val
                })

    tidy = pd.DataFrame(records)
    if tidy.empty:
        raise RuntimeError("No he podido extraer registros del Excel de comunidades.")

    out = tidy.pivot_table(
        index=["comunidad", "anio"],
        columns="metric",
        values="value",
        aggfunc="first"
    ).reset_index()

    return out

def upsert_dim_tiempo(cur, anio):
    # Para datos anuales: mes NULL
    cur.execute(
        "INSERT INTO dim_tiempo (anio, mes, trimestre) VALUES (%s, NULL, NULL) "
        "ON DUPLICATE KEY UPDATE anio=VALUES(anio)",
        (anio,)
    )
    cur.execute("SELECT id_tiempo FROM dim_tiempo WHERE anio=%s AND mes IS NULL", (anio,))
    return cur.fetchone()[0]

def upsert_dim_comunidad(cur, nombre):
    cur.execute(
        "INSERT INTO dim_comunidad (nombre_comunidad) VALUES (%s) "
        "ON DUPLICATE KEY UPDATE nombre_comunidad=VALUES(nombre_comunidad)",
        (nombre,)
    )
    cur.execute("SELECT id_comunidad FROM dim_comunidad WHERE nombre_comunidad=%s", (nombre,))
    return cur.fetchone()[0]

def upsert_hecho_comunidad(cur, id_tiempo, id_comunidad, row):
    cur.execute(
        "INSERT INTO hecho_turismo_comunidad "
        "(id_tiempo, id_comunidad, numero_turistas, variacion_anual) "
        "VALUES (%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE "
        "numero_turistas=VALUES(numero_turistas), "
        "variacion_anual=VALUES(variacion_anual)",
        (
            id_tiempo,
            id_comunidad,
            int(row["numero_turistas"]) if pd.notna(row.get("numero_turistas")) else None,
            float(row["variacion_anual"]) if pd.notna(row.get("variacion_anual")) else None,
        )
    )

def main():
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"No encuentro el Excel en: {DATA_FILE}")

    df_raw = load_excel()
    df = extract_rows(df_raw)

    conn = get_conn()
    try:
        cur = conn.cursor(buffered=True)
        inserted = 0

        for _, r in df.iterrows():
            id_tiempo = upsert_dim_tiempo(cur, int(r["anio"]))
            id_comunidad = upsert_dim_comunidad(cur, str(r["comunidad"]))
            upsert_hecho_comunidad(cur, id_tiempo, id_comunidad, r)
            inserted += 1

        conn.commit()
        print(f"OK: cargadas/actualizadas {inserted} filas en hecho_turismo_comunidad")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
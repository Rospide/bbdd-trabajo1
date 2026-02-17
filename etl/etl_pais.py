import re
import unicodedata
from pathlib import Path
import pandas as pd
from etl.db import get_conn

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_FILE = BASE_DIR / "data" / "10822.xlsx"

MONTH_RE = re.compile(r"^\d{4}M\d{2}$", re.IGNORECASE)


def normalize_text(s: str) -> str:
    """
    Normaliza textos para comparar:
    - minúsculas
    - quita tildes/acentos (NFD)
    - colapsa espacios
    """
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")  # quita acentos
    s = re.sub(r"\s+", " ", s)  # colapsa espacios
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
    # Formato típico ES: miles con punto y decimales con coma
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return None


def load_excel():
    return pd.read_excel(DATA_FILE, sheet_name=0, header=None)


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


def extract_rows(df):
    month_cols = find_month_columns(df)
    if not month_cols:
        raise RuntimeError("No he encontrado columnas tipo 2025M12. Revisa el Excel.")

    col0 = 0
    records = []

    # Claves normalizadas (sin tildes)
    metrics_map = {
        "dato base": "numero_turistas",
        "tasa de variacion anual": "variacion_anual",
        "acumulado en lo que va de ano": "acumulado",
        "tasa de variacion acumulada": "variacion_acumulada",
    }

    current_country = None
    current_metric = None

    for i in range(df.shape[0]):
        cell = df.iloc[i, col0]

        if isinstance(cell, str):
            s = cell.strip()
            s_low = normalize_text(s)

            # 1) ¿Es fila de métrica? (robusto: coincide exacto o por "contiene")
            matched_metric = None
            for key, metric_name in metrics_map.items():
                if key == s_low or key in s_low:
                    matched_metric = metric_name
                    break

            if matched_metric:
                current_metric = matched_metric
            else:
                # 2) ¿Es cabecera de país?
                nxt = df.iloc[i + 1, col0] if i + 1 < df.shape[0] else None
                if isinstance(nxt, str) and normalize_text(nxt) == "dato base":
                    current_country = s
                    current_metric = None

        # 3) Si ya tenemos país + métrica, leemos los valores de meses de ESTA MISMA FILA
        if current_country and current_metric:
            for j, colname in month_cols:
                val = to_number(df.iloc[i, j])
                if val is None:
                    continue
                anio, mes, trimestre = parse_month(colname)
                records.append(
                    {
                        "pais": current_country,
                        "anio": anio,
                        "mes": mes,
                        "trimestre": trimestre,
                        "metric": current_metric,
                        "value": val,
                    }
                )

    tidy = pd.DataFrame(records)
    if tidy.empty:
        raise RuntimeError("No he podido extraer registros del Excel. Puede que la estructura difiera.")

    out = (
        tidy.pivot_table(
            index=["pais", "anio", "mes", "trimestre"],
            columns="metric",
            values="value",
            aggfunc="first",
        )
        .reset_index()
    )

    return out


def upsert_dim_tiempo(cur, anio, mes, trimestre):
    cur.execute(
        "INSERT INTO dim_tiempo (anio, mes, trimestre) VALUES (%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE trimestre=VALUES(trimestre)",
        (anio, mes, trimestre),
    )
    cur.execute("SELECT id_tiempo FROM dim_tiempo WHERE anio=%s AND mes=%s", (anio, mes))
    return cur.fetchone()[0]


def upsert_dim_pais(cur, nombre):
    cur.execute(
        "INSERT INTO dim_pais (nombre_pais) VALUES (%s) "
        "ON DUPLICATE KEY UPDATE nombre_pais=VALUES(nombre_pais)",
        (nombre,),
    )
    cur.execute("SELECT id_pais FROM dim_pais WHERE nombre_pais=%s", (nombre,))
    return cur.fetchone()[0]


def upsert_hecho_pais(cur, id_tiempo, id_pais, row):
    cur.execute(
        "INSERT INTO hecho_turismo_pais "
        "(id_tiempo, id_pais, numero_turistas, variacion_anual, acumulado, variacion_acumulada) "
        "VALUES (%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE "
        "numero_turistas=VALUES(numero_turistas), "
        "variacion_anual=VALUES(variacion_anual), "
        "acumulado=VALUES(acumulado), "
        "variacion_acumulada=VALUES(variacion_acumulada)",
        (
            id_tiempo,
            id_pais,
            int(row["numero_turistas"]) if pd.notna(row.get("numero_turistas")) else None,
            float(row["variacion_anual"]) if pd.notna(row.get("variacion_anual")) else None,
            int(row["acumulado"]) if pd.notna(row.get("acumulado")) else None,
            float(row["variacion_acumulada"]) if pd.notna(row.get("variacion_acumulada")) else None,
        ),
    )


def main():
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"No encuentro el Excel en: {DATA_FILE}")

    df_raw = load_excel()
    df = extract_rows(df_raw)

    conn = get_conn()
    try:
        cur = conn.cursor(buffered=True)  # <- recomendado
        inserted = 0

        for _, r in df.iterrows():
            id_tiempo = upsert_dim_tiempo(cur, int(r["anio"]), int(r["mes"]), int(r["trimestre"]))
            id_pais = upsert_dim_pais(cur, str(r["pais"]))
            upsert_hecho_pais(cur, id_tiempo, id_pais, r)
            inserted += 1

        conn.commit()
        print(f"OK: cargadas/actualizadas {inserted} filas en hecho_turismo_pais")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
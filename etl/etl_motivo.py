from pathlib import Path
import pandas as pd
from etl.db import get_conn
from etl.utils import normalize_text, parse_month, to_number, find_month_columns, month_name_es, first_day_of_month

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_FILE = BASE_DIR / "data" / "13864.xlsx"

def load_excel():
    return pd.read_excel(DATA_FILE, sheet_name=0, header=None)

def extract_rows(df):
    month_cols = find_month_columns(df)
    if not month_cols:
        raise RuntimeError("No he encontrado columnas tipo 2025M12 (MOTIVO).")

    col0 = 0
    records = []
    metrics_map = {
        "dato base": "numero_turistas",
        "tasa de variacion anual": "variacion_anual",
        "acumulado en lo que va de ano": "acumulado",
        "tasa de variacion acumulada": "variacion_acumulada",
    }

    current_motivo = None
    current_metric = None

    for i in range(df.shape[0]):
        cell = df.iloc[i, col0]
        if isinstance(cell, str):
            s_low = normalize_text(cell)
            matched_metric = None
            for key, metric_name in metrics_map.items():
                if key == s_low or key in s_low:
                    matched_metric = metric_name
                    break

            if matched_metric:
                current_metric = matched_metric
            else:
                nxt = df.iloc[i + 1, col0] if i + 1 < df.shape[0] else None
                if isinstance(nxt, str) and normalize_text(nxt) == "dato base":
                    current_motivo = cell.strip()
                    current_metric = None

        if current_motivo and current_metric:
            for j, colname in month_cols:
                val = to_number(df.iloc[i, j])
                if val is None: continue
                anio, mes, trimestre = parse_month(colname)
                records.append({
                    "motivo": current_motivo, "anio": anio, "mes": mes,
                    "trimestre": trimestre, "metric": current_metric, "value": val
                })

    tidy = pd.DataFrame(records)
    if tidy.empty: raise RuntimeError("No he podido extraer registros (MOTIVO).")

    out = tidy.pivot_table(index=["motivo", "anio", "mes", "trimestre"],
                           columns="metric", values="value", aggfunc="first").reset_index()
    return out

def ensure_dummy_records(cur):
    cur.execute("SET sql_mode = '';")
    cur.execute("INSERT INTO dim_pais (id_pais, nombre_pais) SELECT 0, 'No aplica' WHERE NOT EXISTS (SELECT 1 FROM dim_pais WHERE id_pais = 0);")
    cur.execute("INSERT INTO dim_comunidad (id_comunidad, nombre_comunidad) SELECT 0, 'No aplica' WHERE NOT EXISTS (SELECT 1 FROM dim_comunidad WHERE id_comunidad = 0);")
    cur.execute("INSERT INTO dim_duracion (id_duracion, descripcion_duracion) SELECT 0, 'No aplica' WHERE NOT EXISTS (SELECT 1 FROM dim_duracion WHERE id_duracion = 0);")

def upsert_dim_tiempo(cur, anio, mes, trimestre):
    desc = month_name_es(mes)
    fecha = first_day_of_month(anio, mes)
    cur.execute(
        "INSERT INTO dim_tiempo (anio, mes, trimestre, descripcion_mes, fecha_inicio_mes) VALUES (%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE descripcion_mes=VALUES(descripcion_mes)", (anio, mes, trimestre, desc, fecha)
    )
    cur.execute("SELECT id_tiempo FROM dim_tiempo WHERE anio=%s AND mes=%s", (anio, mes))
    return cur.fetchone()[0]

def upsert_dim_motivo(cur, nombre):
    cur.execute(
        "INSERT INTO dim_motivo (nombre_motivo) VALUES (%s) "
        "ON DUPLICATE KEY UPDATE nombre_motivo=VALUES(nombre_motivo)", (nombre,)
    )
    cur.execute("SELECT id_motivo FROM dim_motivo WHERE nombre_motivo=%s", (nombre,))
    return cur.fetchone()[0]

def upsert_hecho(cur, id_tiempo, id_motivo, row):
    cur.execute(
        "INSERT INTO hecho_turismo (id_tiempo, id_pais, id_comunidad, id_motivo, id_duracion, numero_turistas, variacion_anual, acumulado, variacion_acumulada) "
        "VALUES (%s, 0, 0, %s, 0, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE "
        "numero_turistas=VALUES(numero_turistas), variacion_anual=VALUES(variacion_anual), acumulado=VALUES(acumulado), variacion_acumulada=VALUES(variacion_acumulada)",
        (id_tiempo, id_motivo, 
         row.get("numero_turistas") if pd.notna(row.get("numero_turistas")) else None,
         row.get("variacion_anual") if pd.notna(row.get("variacion_anual")) else None,
         row.get("acumulado") if pd.notna(row.get("acumulado")) else None,
         row.get("variacion_acumulada") if pd.notna(row.get("variacion_acumulada")) else None)
    )

def main():
    df = extract_rows(load_excel())
    conn = get_conn()
    try:
        cur = conn.cursor(buffered=True)
        ensure_dummy_records(cur)
        inserted = 0
        for _, r in df.iterrows():
            id_tiempo = upsert_dim_tiempo(cur, int(r["anio"]), int(r["mes"]), int(r["trimestre"]))
            id_motivo = upsert_dim_motivo(cur, str(r["motivo"]))
            upsert_hecho(cur, id_tiempo, id_motivo, r)
            inserted += 1
        conn.commit()
        print(f"OK: Cargadas {inserted} filas (MOTIVO) en hecho_turismo")
    except Exception as e:
        conn.rollback()
        print("Error:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
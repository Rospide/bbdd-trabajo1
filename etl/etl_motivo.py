from pathlib import Path
import pandas as pd
from etl.db import get_conn
# Importamos las utilidades para no reescribirlas
from etl.utils import normalize_text, parse_month, to_number, find_month_columns

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_FILE = BASE_DIR / "data" / "13864.xlsx" # Archivo de Motivos

def load_excel():
    return pd.read_excel(DATA_FILE, sheet_name=0, header=None)

def extract_rows(df):
    month_cols = find_month_columns(df)
    if not month_cols:
        raise RuntimeError("No he encontrado columnas tipo 2025M12.")

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
            s = cell.strip()
            s_low = normalize_text(s)

            # Detectar Métrica
            matched_metric = None
            for key, metric_name in metrics_map.items():
                if key == s_low or key in s_low:
                    matched_metric = metric_name
                    break
            
            if matched_metric:
                current_metric = matched_metric
            else:
                # Detectar Motivo (Miramos si la siguiente fila es "dato base")
                nxt = df.iloc[i + 1, col0] if i + 1 < df.shape[0] else None
                if isinstance(nxt, str) and normalize_text(nxt) == "dato base":
                    current_motivo = s # Ej: "Ocio, recreo y vacaciones"
                    current_metric = None

        if current_motivo and current_metric:
            for j, colname in month_cols:
                val = to_number(df.iloc[i, j])
                if val is None: continue
                
                anio, mes, trimestre = parse_month(colname)
                records.append({
                    "motivo": current_motivo,
                    "anio": anio,
                    "mes": mes,
                    "trimestre": trimestre,
                    "metric": current_metric,
                    "value": val,
                })

    tidy = pd.DataFrame(records)
    if tidy.empty: return tidy # Retorna vacío si falla
    
    return tidy.pivot_table(
        index=["motivo", "anio", "mes", "trimestre"],
        columns="metric", values="value", aggfunc="first"
    ).reset_index()

def upsert_dim_motivo(cur, nombre):
    cur.execute(
        "INSERT INTO dim_motivo (nombre_motivo) VALUES (%s) "
        "ON DUPLICATE KEY UPDATE nombre_motivo=VALUES(nombre_motivo)", (nombre,)
    )
    cur.execute("SELECT id_motivo FROM dim_motivo WHERE nombre_motivo=%s", (nombre,))
    return cur.fetchone()[0]

def upsert_hecho_motivo(cur, id_tiempo, id_motivo, row):
    cur.execute(
        "INSERT INTO hecho_turismo_motivo (id_tiempo, id_motivo, numero_turistas, variacion_anual, acumulado, variacion_acumulada) "
        "VALUES (%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE numero_turistas=VALUES(numero_turistas)", # ... simplificado update
        (
            id_tiempo, id_motivo,
            int(row.get("numero_turistas")) if pd.notna(row.get("numero_turistas")) else None,
            float(row.get("variacion_anual")) if pd.notna(row.get("variacion_anual")) else None,
            int(row.get("acumulado")) if pd.notna(row.get("acumulado")) else None,
            float(row.get("variacion_acumulada")) if pd.notna(row.get("variacion_acumulada")) else None,
        )
    )

def main():
    if not DATA_FILE.exists(): raise FileNotFoundError(f"Falta {DATA_FILE}")
    df = extract_rows(load_excel())
    conn = get_conn()
    try:
        cur = conn.cursor()
        for _, r in df.iterrows():
            # Nota: upsert_dim_tiempo debe importarse de utils o definirse aquí igual que en pais
            from etl.etl_pais import upsert_dim_tiempo 
            id_tiempo = upsert_dim_tiempo(cur, int(r["anio"]), int(r["mes"]), int(r["trimestre"]))
            id_motivo = upsert_dim_motivo(cur, str(r["motivo"]))
            upsert_hecho_motivo(cur, id_tiempo, id_motivo, r)
        conn.commit()
        print("OK: Motivos cargados.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
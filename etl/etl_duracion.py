from pathlib import Path
import pandas as pd
from etl.db import get_conn
from etl.utils import normalize_text, parse_month, to_number, find_month_columns

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_FILE = BASE_DIR / "data" / "14290.xlsx" # Archivo de Duración

def extract_rows(df):
    month_cols = find_month_columns(df)
    if not month_cols:
        raise RuntimeError("No se encontraron columnas de fecha en el Excel de Duración.")

    col0 = 0
    records = []
    metrics_map = {
        "dato base": "numero_turistas",
        "tasa de variacion anual": "variacion_anual",
        "acumulado en lo que va de ano": "acumulado",
        "tasa de variacion acumulada": "variacion_acumulada"
    }
    
    current_duracion = None
    current_metric = None
    
    for i in range(df.shape[0]):
        cell = df.iloc[i, col0]
        if isinstance(cell, str):
            s_low = normalize_text(cell)
            matched_metric = None
            for key, val in metrics_map.items():
                if key in s_low:
                    matched_metric = val
                    break
            
            if matched_metric:
                current_metric = matched_metric
            else:
                nxt = df.iloc[i+1, col0] if i+1 < df.shape[0] else None
                if isinstance(nxt, str) and "dato base" in normalize_text(nxt):
                    current_duracion = cell.strip()
                    current_metric = None
        
        if current_duracion and current_metric:
            for j, colname in month_cols:
                val = to_number(df.iloc[i, j])
                if val is None: continue
                y, m, t = parse_month(colname)
                records.append({
                    "duracion": current_duracion,
                    "anio": y,
                    "mes": m,
                    "trimestre": t,
                    "metric": current_metric,
                    "value": val
                })

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records).pivot_table(
        index=["duracion", "anio", "mes", "trimestre"],
        columns="metric",
        values="value",
        aggfunc="first"
    ).reset_index()

def clean_val(val):
    """Convierte NaN de Pandas en None de Python para que MySQL lo entienda como NULL"""
    if pd.isna(val):
        return None
    return val

def main():
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Falta el archivo: {DATA_FILE}")
    
    print(f"Leyendo archivo: {DATA_FILE.name}...")
    df_raw = pd.read_excel(DATA_FILE, header=None)
    df = extract_rows(df_raw)
    
    if df.empty:
        print("⚠ No se encontraron datos válidos en Duración.")
        return

    conn = get_conn()
    cur = conn.cursor()
    try:
        inserted = 0
        for _, r in df.iterrows():
            # 1. Upsert Tiempo
            cur.execute(
                "INSERT INTO dim_tiempo (anio, mes, trimestre) VALUES (%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE anio=anio",
                (r.anio, r.mes, r.trimestre)
            )
            cur.execute("SELECT id_tiempo FROM dim_tiempo WHERE anio=%s AND mes=%s", (r.anio, r.mes))
            id_tiempo = cur.fetchone()[0]

            # 2. Upsert Duracion
            cur.execute(
                "INSERT INTO dim_duracion (descripcion_duracion) VALUES (%s) "
                "ON DUPLICATE KEY UPDATE descripcion_duracion=descripcion_duracion",
                (r.duracion,)
            )
            cur.execute("SELECT id_duracion FROM dim_duracion WHERE descripcion_duracion=%s", (r.duracion,))
            id_duracion = cur.fetchone()[0]

            # 3. Upsert Hecho (USANDO clean_val PARA EVITAR EL ERROR DE NAN)
            cur.execute("""
                INSERT INTO hecho_turismo_duracion 
                (id_tiempo, id_duracion, numero_turistas, variacion_anual, acumulado, variacion_acumulada) 
                VALUES (%s,%s,%s,%s,%s,%s) 
                ON DUPLICATE KEY UPDATE numero_turistas=VALUES(numero_turistas)
            """, (
                id_tiempo, 
                id_duracion, 
                clean_val(r.get("numero_turistas")), 
                clean_val(r.get("variacion_anual")), 
                clean_val(r.get("acumulado")), 
                clean_val(r.get("variacion_acumulada"))
            ))
            inserted += 1
            
        conn.commit()
        print(f"OK: cargadas/actualizadas {inserted} filas en Duración.")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error insertando datos de Duración: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
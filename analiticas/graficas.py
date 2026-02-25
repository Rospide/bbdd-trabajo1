from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

from etl.db import get_conn

# Configuración de carpetas
BASE_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = BASE_DIR / "analytics" / "out"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def query_df(sql: str, params=None) -> pd.DataFrame:
    conn = get_conn()
    try:
        # Pandas avisa a veces sobre pasar la conexión directa, pero con mysql-connector es seguro.
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

def grafica_top_paises_historicos():
    """1. Top 10 países emisores históricos (Gráfico de barras)"""
    sql = """
    SELECT p.nombre_pais, SUM(h.numero_turistas) AS total_turistas
    FROM hecho_turismo h
    JOIN dim_pais p ON h.id_pais = p.id_pais
    WHERE h.id_pais != 0 AND p.nombre_pais NOT LIKE '%Total%'
    GROUP BY p.nombre_pais
    ORDER BY total_turistas DESC
    LIMIT 10;
    """
    df = query_df(sql)
    if df.empty: return

    ax = df.plot(kind="bar", x="nombre_pais", y="total_turistas", legend=False, color="#4C72B0")
    ax.set_title("Top 10 Países Emisores Históricos")
    ax.set_xlabel("País de Origen")
    ax.set_ylabel("Millones de Turistas")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    
    out = OUT_DIR / "1_top_paises.png"
    plt.savefig(out)
    plt.close()
    print("OK gráfico:", out)

def grafica_ranking_comunidades():
    """2. Ranking de las 5 comunidades más visitadas en el último año (Quesito/Tarta)"""
    sql = """
    SELECT c.nombre_comunidad, SUM(h.numero_turistas) AS total_turistas
    FROM hecho_turismo h
    JOIN dim_comunidad c ON h.id_comunidad = c.id_comunidad
    JOIN dim_tiempo t ON h.id_tiempo = t.id_tiempo
    WHERE h.id_comunidad != 0 AND c.nombre_comunidad NOT LIKE '%Total%'
      -- Filtramos por el último año disponible de las comunidades
      AND t.anio = (
          SELECT MAX(t2.anio) FROM hecho_turismo h2 
          JOIN dim_tiempo t2 ON h2.id_tiempo = t2.id_tiempo 
          WHERE h2.id_comunidad != 0
      )
    GROUP BY c.nombre_comunidad
    ORDER BY total_turistas DESC
    LIMIT 5;
    """
    df = query_df(sql)
    if df.empty: return

    plt.figure(figsize=(8, 8))
    plt.pie(df["total_turistas"], labels=df["nombre_comunidad"], autopct='%1.1f%%', startangle=140, colors=plt.cm.Pastel1.colors)
    plt.title("Top 5 Comunidades Más Visitadas (Último Año)")
    plt.tight_layout()

    out = OUT_DIR / "2_ranking_comunidades.png"
    plt.savefig(out)
    plt.close()
    print("OK gráfico:", out)

def grafica_crecimiento_regional():
    """3. Comunidades que más han crecido en el último año (Gráfico de barras)"""
    sql = """
    SELECT c.nombre_comunidad, AVG(h.variacion_anual) AS crecimiento
    FROM hecho_turismo h
    JOIN dim_comunidad c ON h.id_comunidad = c.id_comunidad
    JOIN dim_tiempo t ON h.id_tiempo = t.id_tiempo
    WHERE h.id_comunidad != 0 AND c.nombre_comunidad NOT LIKE '%Total%'
      AND t.anio = (
          SELECT MAX(t2.anio) FROM hecho_turismo h2 
          JOIN dim_tiempo t2 ON h2.id_tiempo = t2.id_tiempo 
          WHERE h2.id_comunidad != 0
      )
    GROUP BY c.nombre_comunidad
    ORDER BY crecimiento DESC
    LIMIT 5;
    """
    df = query_df(sql)
    if df.empty: return

    ax = df.plot(kind="bar", x="nombre_comunidad", y="crecimiento", legend=False, color="#55A868")
    ax.set_title("Top 5 Comunidades con Mayor Crecimiento Anual (%)")
    ax.set_xlabel("Comunidad Autónoma")
    ax.set_ylabel("Crecimiento (%)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    out = OUT_DIR / "3_crecimiento_regional.png"
    plt.savefig(out)
    plt.close()
    print("OK gráfico:", out)

def grafica_motivos_viaje():
    """4. Distribución de motivos de viaje en el último año (Gráfico de Tarta)"""
    sql = """
    SELECT m.nombre_motivo, SUM(h.numero_turistas) AS total_turistas
    FROM hecho_turismo h
    JOIN dim_motivo m ON h.id_motivo = m.id_motivo
    JOIN dim_tiempo t ON h.id_tiempo = t.id_tiempo
    WHERE h.id_motivo != 0 AND m.nombre_motivo NOT LIKE '%Total%'
      AND t.anio = (
          SELECT MAX(t2.anio) FROM hecho_turismo h2 
          JOIN dim_tiempo t2 ON h2.id_tiempo = t2.id_tiempo 
          WHERE h2.id_motivo != 0
      )
    GROUP BY m.nombre_motivo
    ORDER BY total_turistas DESC;
    """
    df = query_df(sql)
    if df.empty: return

    plt.figure(figsize=(8, 8))
    # Separamos un poco los "trozos" para que quede más visual
    explode = [0.05] * len(df)
    plt.pie(df["total_turistas"], labels=df["nombre_motivo"], autopct='%1.1f%%', explode=explode, startangle=90, colors=plt.cm.Set3.colors)
    plt.title("Motivos Principales del Turismo (Último Año)")
    plt.tight_layout()

    out = OUT_DIR / "4_motivos_viaje.png"
    plt.savefig(out)
    plt.close()
    print("OK gráfico:", out)

def grafica_duracion_estancia():
    """5. Duración preferida de los turistas a nivel global (Gráfico de barras horizontales)"""
    sql = """
    SELECT d.descripcion_duracion, SUM(h.numero_turistas) AS total_turistas
    FROM hecho_turismo h
    JOIN dim_duracion d ON h.id_duracion = d.id_duracion
    WHERE h.id_duracion != 0 AND d.descripcion_duracion NOT LIKE '%Total%'
    GROUP BY d.descripcion_duracion
    ORDER BY total_turistas ASC; -- Ascendente para que la barra mayor quede arriba
    """
    df = query_df(sql)
    if df.empty: return

    ax = df.plot(kind="barh", x="descripcion_duracion", y="total_turistas", legend=False, color="#C44E52")
    ax.set_title("Duración Preferida de la Estancia (Histórico)")
    ax.set_xlabel("Número de Turistas")
    ax.set_ylabel("Duración")
    plt.tight_layout()

    out = OUT_DIR / "5_duracion_estancia.png"
    plt.savefig(out)
    plt.close()
    print("OK gráfico:", out)

def grafica_estacionalidad_meses():
    """6. Estacionalidad: Suma de turistas por meses para ver los picos (Gráfico de barras)"""
    sql = """
    SELECT t.mes, SUM(h.numero_turistas) AS total_turistas
    FROM hecho_turismo h
    JOIN dim_tiempo t ON h.id_tiempo = t.id_tiempo
    JOIN dim_pais p ON h.id_pais = p.id_pais
    -- Para no duplicar datos (como país y motivo tienen info mensual cruzada con total de turistas),
    -- tomamos la tabla de países como referencia de la base general para sacar el mes.
    WHERE h.id_pais != 0 AND p.nombre_pais NOT LIKE '%Total%'
    GROUP BY t.mes
    ORDER BY t.mes;
    """
    df = query_df(sql)
    if df.empty: return

    meses_nombres = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    df["nombre_mes"] = df["mes"].apply(lambda x: meses_nombres[x-1])

    ax = df.plot(kind="bar", x="nombre_mes", y="total_turistas", legend=False, color="#DD8452")
    ax.set_title("La Estacionalidad del Turismo (Meses Pico Históricos)")
    ax.set_xlabel("Mes del Año")
    ax.set_ylabel("Turistas Acumulados")
    plt.xticks(rotation=0)
    plt.tight_layout()

    out = OUT_DIR / "6_estacionalidad.png"
    plt.savefig(out)
    plt.close()
    print("OK gráfico:", out)

def main():
    print("Generando reportes analíticos...")
    grafica_top_paises_historicos()
    grafica_ranking_comunidades()
    grafica_crecimiento_regional()
    grafica_motivos_viaje()
    grafica_duracion_estancia()
    grafica_estacionalidad_meses()
    print("¡Todas las gráficas generadas con éxito en la carpeta 'analytics/out'!")

if __name__ == "__main__":
    main()
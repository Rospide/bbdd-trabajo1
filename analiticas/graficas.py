from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

from etl.db import get_conn

BASE_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = BASE_DIR / "analytics" / "out"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def query_df(sql: str, params=None) -> pd.DataFrame:
    conn = get_conn()
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()


def grafica_top_paises_ultimo_mes(top_n=10):
    sql = """
    SELECT
        dt.anio,
        dt.mes,
        p.nombre_pais,
        h.numero_turistas
    FROM hecho_turismo_pais h
    JOIN dim_tiempo dt ON dt.id_tiempo = h.id_tiempo
    JOIN dim_pais p ON p.id_pais = h.id_pais
    WHERE (dt.anio, dt.mes) = (
        SELECT anio, mes
        FROM dim_tiempo
        ORDER BY anio DESC, mes DESC
        LIMIT 1
    )
    ORDER BY h.numero_turistas DESC
    LIMIT %s;
    """
    df = query_df(sql, params=[top_n])

    if df.empty:
        print("No hay datos para top países.")
        return

    title = f"Top {top_n} países - {int(df['anio'][0])}-{int(df['mes'][0]):02d}"
    ax = df.plot(kind="bar", x="nombre_pais", y="numero_turistas", legend=False)
    ax.set_title(title)
    ax.set_xlabel("País")
    ax.set_ylabel("Número de turistas")
    plt.tight_layout()

    out = OUT_DIR / "top_paises_ultimo_mes.png"
    plt.savefig(out)
    plt.close()
    print("OK gráfico:", out)


def grafica_total_turistas_por_mes():
    sql = """
    SELECT
        dt.anio,
        dt.mes,
        SUM(h.numero_turistas) AS total_turistas
    FROM hecho_turismo_pais h
    JOIN dim_tiempo dt ON dt.id_tiempo = h.id_tiempo
    GROUP BY dt.anio, dt.mes
    ORDER BY dt.anio, dt.mes;
    """
    df = query_df(sql)

    if df.empty:
        print("No hay datos para serie temporal.")
        return

    df["periodo"] = df["anio"].astype(int).astype(str) + "-" + df["mes"].astype(int).map(lambda x: f"{x:02d}")
    ax = df.plot(kind="line", x="periodo", y="total_turistas", legend=False)
    ax.set_title("Total turistas por mes (sumatorio países)")
    ax.set_xlabel("Mes")
    ax.set_ylabel("Turistas")
    plt.xticks(rotation=45)
    plt.tight_layout()

    out = OUT_DIR / "total_turistas_por_mes.png"
    plt.savefig(out)
    plt.close()
    print("OK gráfico:", out)


def main():
    grafica_top_paises_ultimo_mes(top_n=10)
    grafica_total_turistas_por_mes()


if __name__ == "__main__":
    main()
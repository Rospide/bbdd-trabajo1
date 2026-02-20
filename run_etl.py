# run_etl.py
from etl import etl_pais, etl_comunidad, etl_motivo, etl_duracion

print("--- Iniciando ETL Pais ---")
etl_pais.main()
print("--- Iniciando ETL Comunidad ---")
etl_comunidad.main()
print("--- Iniciando ETL Motivo ---")
etl_motivo.main()
print("--- Iniciando ETL Duracion ---")
etl_duracion.main()
print("--- PROCESO COMPLETO ---")
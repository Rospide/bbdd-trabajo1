# bbdd-trabajo1


# 1. Clonar el repositorio
```bash
git clone https://github.com/Rospide/bbdd-trabajo1
cd bbdd-trabajo1
```

## 2. Crear el entorno virtual
```bash
py -m venv .venv
.\.venv\Scripts\Activate.ps1
```

## 3. Instalar dependencias
```bash
py -m pip install -r requirements.txt
```

## 4. Crear archivo .env
Crear un archivo llamado .env en la raíz del proyecto con el siguiente contenido:
```bash
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=TU_PASSWORD
MYSQL_DB=dw_turismo
```
Este archivo no se sube a GitHub (está en .gitignore).

## 5. Ejecutar ETL
Países:
```bash
py -m etl.etl_pais
```

Comunidades:
```bash
py -m etl.etl_comunidad
```




# bbdd-trabajo1 — Data Warehouse Turismo (INE FRONTUR)

Proyecto de Data Warehouse a partir de datos FRONTUR (INE), usando:

* MySQL (modelo en estrella con 2 tablas de hechos)
* Python + pandas (ETL desde Excel a MySQL)

---

## 0) Requisitos

* Windows
* MySQL Server + MySQL Workbench
* Python (recomendado desde python.org)
* Git

---

## 1) Clonar el repositorio

```bash
git clone https://github.com/Rospide/bbdd-trabajo1
cd bbdd-trabajo1
```

---

## 2) Crear y activar entorno virtual (venv)

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Si Windows bloquea scripts, ejecutar una vez:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

y volver a activar:

```powershell
.\.venv\Scripts\Activate.ps1
```

---

## 3) Instalar dependencias

```powershell
py -m pip install --upgrade pip
py -m pip install -r requirements.txt
```

Si no existe requirements.txt o faltan paquetes, instalar manualmente:

```powershell
py -m pip install pandas openpyxl mysql-connector-python python-dotenv
```

---

## 4) Preparar los datos (Excel) en local

Crear carpeta `data/` en la raíz del proyecto y copiar dentro los Excel:

* `10822.xlsx` (turistas por país y mes)
* `23988.xlsx` (turistas por comunidad y año)

Estructura esperada:

```
bbdd-trabajo1/
  data/
    10822.xlsx
    23988.xlsx
```

⚠ Estos Excel NO se suben a GitHub (por .gitignore).

---

## 5) Crear archivo `.env` (NO se sube a GitHub)

Crear un archivo llamado `.env` en la raíz del proyecto con:

```env
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=TU_PASSWORD
MYSQL_DB=dw_turismo
```

* Si tu usuario no tiene contraseña: `MYSQL_PASSWORD=`
* Este archivo NO se sube a GitHub (está ignorado).

---

## 6) Crear la base de datos y tablas (Schema)

### Opción A: desde MySQL Workbench

1. Abrir Workbench y conectarse a MySQL
2. Abrir una pestaña SQL (New Query Tab)
3. Ejecutar el script del repo:

Archivo: `sql/01_schema.sql` (o el que esté en /sql)

### Opción B: pegar directamente (si lo necesitas)

En Workbench, ejecuta:

```sql
CREATE DATABASE IF NOT EXISTS dw_turismo;
USE dw_turismo;
-- Ejecutar aquí el contenido del archivo sql/01_schema.sql
```

---

## 7) Probar conexión a MySQL

Ejecutar:

```powershell
py -m etl.test_db
```

Debe mostrar:

* Base de datos: `dw_turismo`
* Tablas: `dim_comunidad`, `dim_pais`, `dim_tiempo`, `hecho_turismo_pais`, `hecho_turismo_comunidad`

---

## 8) Ejecutar ETL (cargar datos en MySQL)

### 8.1 Cargar Países (10822.xlsx)

```powershell
py -m etl.etl_pais
```

### 8.2 Cargar Comunidades (23988.xlsx)

```powershell
py -m etl.etl_comunidad
```

---

## 9) Comprobación en MySQL (Workbench)

Pegar en Workbench:

```sql
USE dw_turismo;

-- Comprobar países (Excel 10822)
SELECT COUNT(*) AS paises FROM dim_pais;
SELECT COUNT(*) AS tiempos_mensuales FROM dim_tiempo WHERE mes IS NOT NULL;
SELECT COUNT(*) AS hechos_pais FROM hecho_turismo_pais;

-- Comprobar comunidades (Excel 23988)
SELECT COUNT(*) AS comunidades FROM dim_comunidad;
SELECT COUNT(*) AS tiempos_anuales FROM dim_tiempo WHERE mes IS NULL;
SELECT COUNT(*) AS hechos_comunidad FROM hecho_turismo_comunidad;

-- Ver ejemplos
SELECT * FROM hecho_turismo_pais LIMIT 10;
SELECT * FROM hecho_turismo_comunidad LIMIT 10;
```

---

## Notas

* El warning de openpyxl sobre estilos del workbook es normal con algunos Excel del INE y no afecta al ETL.
* `.env`, `.venv/` y `data/*.xlsx` no se suben a GitHub por seguridad y tamaño.


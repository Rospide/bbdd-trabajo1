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

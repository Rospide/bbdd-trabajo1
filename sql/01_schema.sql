-- Crear esquema
CREATE DATABASE IF NOT EXISTS dw_turismo;
USE dw_turismo;

-- ======================
-- DIMENSIONES
-- ======================

CREATE TABLE dim_tiempo (
  id_tiempo INT AUTO_INCREMENT PRIMARY KEY,
  anio SMALLINT NOT NULL,
  mes TINYINT NULL,
  trimestre TINYINT NULL,
  UNIQUE (anio, mes)
);

CREATE TABLE dim_pais (
  id_pais INT AUTO_INCREMENT PRIMARY KEY,
  nombre_pais VARCHAR(100) NOT NULL,
  UNIQUE (nombre_pais)
);

CREATE TABLE dim_comunidad (
  id_comunidad INT AUTO_INCREMENT PRIMARY KEY,
  nombre_comunidad VARCHAR(100) NOT NULL,
  UNIQUE (nombre_comunidad)
);

-- ======================
-- TABLAS DE HECHOS
-- ======================

CREATE TABLE hecho_turismo_pais (
  id_tiempo INT NOT NULL,
  id_pais INT NOT NULL,
  numero_turistas INT,
  variacion_anual DECIMAL(7,2),
  acumulado INT,
  variacion_acumulada DECIMAL(7,2),
  PRIMARY KEY (id_tiempo, id_pais),
  FOREIGN KEY (id_tiempo) REFERENCES dim_tiempo(id_tiempo),
  FOREIGN KEY (id_pais) REFERENCES dim_pais(id_pais)
);

CREATE TABLE hecho_turismo_comunidad (
  id_tiempo INT NOT NULL,
  id_comunidad INT NOT NULL,
  numero_turistas INT,
  variacion_anual DECIMAL(7,2),
  PRIMARY KEY (id_tiempo, id_comunidad),
  FOREIGN KEY (id_tiempo) REFERENCES dim_tiempo(id_tiempo),
  FOREIGN KEY (id_comunidad) REFERENCES dim_comunidad(id_comunidad)
);

DROP TABLE IF EXISTS MEDICIONES;
CREATE TABLE IF NOT EXISTS MEDICIONES (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    cod INT,
    dia INT,
    localidad VARCHAR(255),
    provincia VARCHAR(255),
    tempMaxima DOUBLE,
    horaTempMax VARCHAR(255),
    tempMinima DOUBLE,
    horaTempMin VARCHAR(255),
    precipitacion DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

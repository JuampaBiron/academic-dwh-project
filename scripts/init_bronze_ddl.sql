-- Tabla de Proyectos
CREATE TABLE sch_bronze.pfl_projects(
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    extraction_timestamp DATETIME2(3) NOT NULL,
    api_endpoint VARCHAR(500),
    http_status_code INT,
    raw_json NVARCHAR(MAX) CHECK (ISJSON(raw_json) = 1),
    record_hash AS HASHBYTES('SHA2_256', raw_json) PERSISTED,
    batch_id VARCHAR(50) NOT NULL,
    record_count INT,
    file_size_bytes INT,
    created_at DATETIME2(3) DEFAULT SYSDATETIME()
);

-- Tabla de Publicaciones
CREATE TABLE sch_bronze.pfl_publications(
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    extraction_timestamp DATETIME2(3) NOT NULL,
    api_endpoint VARCHAR(500),
    http_status_code INT,
    raw_json NVARCHAR(MAX) CHECK (ISJSON(raw_json) = 1),
    record_hash AS HASHBYTES('SHA2_256', raw_json) PERSISTED,
    batch_id VARCHAR(50) NOT NULL,
    record_count INT,
    file_size_bytes INT,
    created_at DATETIME2(3) DEFAULT SYSDATETIME()
);

-- Tabla de Académicos
CREATE TABLE sch_bronze.pfl_academics(
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    extraction_timestamp DATETIME2(3) NOT NULL,
    api_endpoint VARCHAR(500),
    http_status_code INT,
    raw_json NVARCHAR(MAX) CHECK (ISJSON(raw_json) = 1),
    record_hash AS HASHBYTES('SHA2_256', raw_json) PERSISTED,
    batch_id VARCHAR(50) NOT NULL,
    record_count INT,
    file_size_bytes INT,
    created_at DATETIME2(3) DEFAULT SYSDATETIME()
);

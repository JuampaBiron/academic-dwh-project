-- =====================================================
-- SISTEMA DE CONTROL DE EJECUCIONES
-- =====================================================

-- Crear esquema para control
CREATE SCHEMA IF NOT EXISTS control;

-- =====================================================
-- LIMPIAR TABLAS EXISTENTES (EN ORDEN CORRECTO)
-- =====================================================
DROP TABLE IF EXISTS control.execution_logs CASCADE;
DROP TABLE IF EXISTS control.job_dependencies CASCADE;
DROP TABLE IF EXISTS control.job_executions CASCADE;
DROP TABLE IF EXISTS control.job_definitions CASCADE;

-- =====================================================
-- 1. TABLA DICCIONARIO/CATÁLOGO DE TRABAJOS
-- =====================================================
CREATE TABLE control.job_definitions (
    job_id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL UNIQUE,
    job_description TEXT,
    job_category VARCHAR(50), -- 'scraping', 'transformation', 'loading', etc.
    
    -- Configuración de frecuencia
    frequency_type VARCHAR(20) NOT NULL, -- 'manual', 'daily', 'weekly', 'monthly', 'cron'
    frequency_value VARCHAR(50), -- '0 2 * * *' para cron, 'monday' para weekly, etc.
    
    -- Configuración de ejecución
    function_name VARCHAR(200) NOT NULL, -- Nombre de la función a ejecutar
    schema_name VARCHAR(50) DEFAULT 'public',
    timeout_minutes INTEGER DEFAULT 60,
    max_retries INTEGER DEFAULT 3,
    
    -- Parámetros de la función (JSON)
    parameters JSONB DEFAULT '{}',
    
    -- Control de estado
    is_enabled BOOLEAN DEFAULT true,
    is_active BOOLEAN DEFAULT true, -- Para soft delete
    
    -- Dependencias y orden
    execution_order INTEGER DEFAULT 1,
    depends_on INTEGER[], -- Array de job_ids que deben completarse antes
    
    -- Metadatos
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT USER
);

-- =====================================================
-- 2. TABLA DE EJECUCIONES (HISTORIAL + PENDIENTES)
-- =====================================================
CREATE TABLE control.job_executions (
    execution_id SERIAL PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES control.job_definitions(job_id),
    
    -- Información de programación
    scheduled_time TIMESTAMP NOT NULL,
    execution_date DATE NOT NULL, -- Fecha lógica de la ejecución
    
    -- Estado de la ejecución
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING', 
    -- Estados: PENDING, RUNNING, SUCCESS, FAILED, CANCELLED, TIMEOUT
    
    -- Timestamps de ejecución
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    
    -- Información de resultados
    result_message TEXT,
    result_data JSONB, -- Para almacenar resultados estructurados
    error_message TEXT,
    error_details JSONB,
    
    -- Información de reintentos
    attempt_number INTEGER DEFAULT 1,
    max_attempts INTEGER DEFAULT 1,
    
    -- Metadatos de ejecución
    executed_by VARCHAR(100),
    execution_host VARCHAR(100),
    process_id INTEGER,
    
    -- Auditoría
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 3. TABLA DE DEPENDENCIAS (OPCIONAL - MÁS GRANULAR)
-- =====================================================
CREATE TABLE control.job_dependencies (
    dependency_id SERIAL PRIMARY KEY,
    parent_job_id INTEGER NOT NULL REFERENCES control.job_definitions(job_id),
    child_job_id INTEGER NOT NULL REFERENCES control.job_definitions(job_id),
    dependency_type VARCHAR(20) DEFAULT 'SUCCESS', -- SUCCESS, COMPLETION, CUSTOM
    is_active BOOLEAN DEFAULT true,
    
    UNIQUE(parent_job_id, child_job_id)
);

-- =====================================================
-- 4. TABLA DE LOGS DETALLADOS (OPCIONAL)
-- =====================================================
CREATE TABLE control.execution_logs (
    log_id SERIAL PRIMARY KEY,
    execution_id INTEGER NOT NULL REFERENCES control.job_executions(execution_id),
    log_level VARCHAR(10) NOT NULL, -- INFO, WARNING, ERROR, DEBUG
    log_message TEXT NOT NULL,
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Información adicional
    log_context JSONB,
    step_name VARCHAR(100)
);

-- =====================================================
-- ÍNDICES PARA PERFORMANCE
-- =====================================================

-- Limpiar índices existentes
DROP INDEX IF EXISTS control.idx_job_definitions_category;
DROP INDEX IF EXISTS control.idx_job_definitions_enabled;
DROP INDEX IF EXISTS control.idx_job_definitions_order;
DROP INDEX IF EXISTS control.idx_job_executions_job_id;
DROP INDEX IF EXISTS control.idx_job_executions_status;
DROP INDEX IF EXISTS control.idx_job_executions_scheduled;
DROP INDEX IF EXISTS control.idx_job_executions_execution_date;
DROP INDEX IF EXISTS control.idx_job_executions_status_scheduled;
DROP INDEX IF EXISTS control.idx_job_executions_job_status_date;
DROP INDEX IF EXISTS control.idx_execution_logs_execution;
DROP INDEX IF EXISTS control.idx_execution_logs_timestamp;
DROP INDEX IF EXISTS control.idx_execution_logs_level;

-- Job Definitions
CREATE INDEX idx_job_definitions_category ON control.job_definitions(job_category);
CREATE INDEX idx_job_definitions_enabled ON control.job_definitions(is_enabled, is_active);
CREATE INDEX idx_job_definitions_order ON control.job_definitions(execution_order);

-- Job Executions
CREATE INDEX idx_job_executions_job_id ON control.job_executions(job_id);
CREATE INDEX idx_job_executions_status ON control.job_executions(status);
CREATE INDEX idx_job_executions_scheduled ON control.job_executions(scheduled_time);
CREATE INDEX idx_job_executions_execution_date ON control.job_executions(execution_date);
CREATE INDEX idx_job_executions_status_scheduled ON control.job_executions(status, scheduled_time);

-- Índice compuesto para consultas comunes
CREATE INDEX idx_job_executions_job_status_date ON control.job_executions(job_id, status, execution_date);

-- Execution Logs
CREATE INDEX idx_execution_logs_execution ON control.execution_logs(execution_id);
CREATE INDEX idx_execution_logs_timestamp ON control.execution_logs(log_timestamp);
CREATE INDEX idx_execution_logs_level ON control.execution_logs(log_level);

-- =====================================================
-- TRIGGERS PARA ACTUALIZAR TIMESTAMPS
-- =====================================================

-- Limpiar función existente
DROP FUNCTION IF EXISTS control.update_timestamp() CASCADE;

CREATE OR REPLACE FUNCTION control.update_timestamp()
RETURNS TRIGGER AS 
$$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

-- Triggers
CREATE TRIGGER trg_job_definitions_updated_at
    BEFORE UPDATE ON control.job_definitions
    FOR EACH ROW EXECUTE FUNCTION control.update_timestamp();

CREATE TRIGGER trg_job_executions_updated_at
    BEFORE UPDATE ON control.job_executions
    FOR EACH ROW EXECUTE FUNCTION control.update_timestamp();

-- =====================================================
-- DATOS INICIALES DE EJEMPLO
-- =====================================================

-- Insertar definiciones de trabajos para tu pipeline (usando UPSERT)
INSERT INTO control.job_definitions (
    job_name, job_description, job_category, 
    frequency_type, frequency_value, 
    function_name, schema_name, 
    execution_order, depends_on
) VALUES 
    ('scrape_unidades', 'Extracción de unidades académicas', 'scraping', 
     'daily', '0 1 * * *', 
     'fn_scrape_unidades', 'etl', 
     1, NULL),
     
    ('scrape_academics', 'Extracción de académicos por unidad', 'scraping', 
     'daily', '0 2 * * *', 
     'fn_scrape_academics', 'etl', 
     2, ARRAY[1]),
     
    ('scrape_publications', 'Extracción de publicaciones', 'scraping', 
     'daily', '0 3 * * *', 
     'fn_scrape_publications', 'etl', 
     3, ARRAY[2]),
     
    ('scrape_projects', 'Extracción de proyectos', 'scraping', 
     'daily', '0 4 * * *', 
     'fn_scrape_projects', 'etl', 
     4, ARRAY[2]),
     
    ('transform_to_silver', 'Transformación Bronze → Silver', 'transformation', 
     'daily', '0 5 * * *', 
     'fn_transform_all_to_silver', 'etl', 
     5, ARRAY[1,2,3,4]),
     
    ('load_bronze_data', 'Carga de datos en Bronze', 'loading', 
     'daily', '0 1 30 * *', 
     'fn_load_bronze_data', 'etl', 
     10, ARRAY[1,2,3,4])
ON CONFLICT (job_name) DO UPDATE SET
    job_description = EXCLUDED.job_description,
    job_category = EXCLUDED.job_category,
    frequency_type = EXCLUDED.frequency_type,
    frequency_value = EXCLUDED.frequency_value,
    function_name = EXCLUDED.function_name,
    schema_name = EXCLUDED.schema_name,
    execution_order = EXCLUDED.execution_order,
    depends_on = EXCLUDED.depends_on,
    updated_at = CURRENT_TIMESTAMP;

-- =====================================================
-- COMENTARIOS EN TABLAS
-- =====================================================
COMMENT ON TABLE control.job_definitions IS 'Catálogo de trabajos/funciones del pipeline ETL';
COMMENT ON TABLE control.job_executions IS 'Historial y cola de ejecuciones de trabajos';
COMMENT ON TABLE control.job_dependencies IS 'Dependencias entre trabajos';
COMMENT ON TABLE control.execution_logs IS 'Logs detallados de ejecuciones';

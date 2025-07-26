-- =====================================================
-- FUNCIONES AUXILIARES DEL SISTEMA DE CONTROL ETL
-- Archivo: sql/002_control_functions.sql
-- =====================================================

-- =====================================================
-- 1. FUNCIÓN PARA CREAR NUEVA EJECUCIÓN
-- =====================================================
CREATE OR REPLACE FUNCTION control.create_execution(
    p_job_name VARCHAR(100),
    p_scheduled_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    p_execution_date DATE DEFAULT CURRENT_DATE
)
RETURNS INTEGER AS $$
DECLARE
    v_job_id INTEGER;
    v_execution_id INTEGER;
    v_max_attempts INTEGER;
BEGIN
    -- Obtener información del trabajo
    SELECT job_id, max_retries + 1 
    INTO v_job_id, v_max_attempts
    FROM control.job_definitions 
    WHERE job_name = p_job_name AND is_enabled = true AND is_active = true;
    
    IF v_job_id IS NULL THEN
        RAISE EXCEPTION 'Trabajo no encontrado o deshabilitado: %', p_job_name;
    END IF;
    
    -- Crear la ejecución
    INSERT INTO control.job_executions (
        job_id, scheduled_time, execution_date, 
        status, max_attempts
    ) VALUES (
        v_job_id, p_scheduled_time, p_execution_date,
        'PENDING', v_max_attempts
    ) RETURNING execution_id INTO v_execution_id;
    
    -- Log de creación
    PERFORM control.log_execution(
        v_execution_id, 
        'INFO', 
        FORMAT('Ejecución creada para %s, programada para %s', p_job_name, p_scheduled_time)
    );
    
    RETURN v_execution_id;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 2. FUNCIÓN PARA INICIAR EJECUCIÓN
-- =====================================================
CREATE OR REPLACE FUNCTION control.start_execution(
    p_execution_id INTEGER,
    p_executed_by VARCHAR(100) DEFAULT USER,
    p_execution_host VARCHAR(100) DEFAULT inet_client_addr()::TEXT
)
RETURNS BOOLEAN AS $$
DECLARE
    v_current_status VARCHAR(20);
BEGIN
    -- Verificar estado actual
    SELECT status INTO v_current_status
    FROM control.job_executions 
    WHERE execution_id = p_execution_id;
    
    IF v_current_status IS NULL THEN
        RAISE EXCEPTION 'Ejecución no encontrada: %', p_execution_id;
    END IF;
    
    IF v_current_status != 'PENDING' THEN
        RAISE EXCEPTION 'Ejecución no está en estado PENDING: % (estado actual: %)', 
                        p_execution_id, v_current_status;
    END IF;
    
    -- Actualizar a RUNNING
    UPDATE control.job_executions 
    SET 
        status = 'RUNNING',
        start_time = CURRENT_TIMESTAMP,
        executed_by = p_executed_by,
        execution_host = p_execution_host,
        process_id = pg_backend_pid()
    WHERE execution_id = p_execution_id;
    
    -- Log de inicio
    PERFORM control.log_execution(
        p_execution_id, 
        'INFO', 
        FORMAT('Ejecución iniciada por %s en %s', p_executed_by, p_execution_host)
    );
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 3. FUNCIÓN PARA COMPLETAR EJECUCIÓN
-- =====================================================
CREATE OR REPLACE FUNCTION control.complete_execution(
    p_execution_id INTEGER,
    p_status VARCHAR(20), -- 'SUCCESS', 'FAILED', 'CANCELLED', 'TIMEOUT'
    p_result_message TEXT DEFAULT NULL,
    p_result_data JSONB DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
)
RETURNS BOOLEAN AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_duration INTEGER;
BEGIN
    -- Validar status
    IF p_status NOT IN ('SUCCESS', 'FAILED', 'CANCELLED', 'TIMEOUT') THEN
        RAISE EXCEPTION 'Estado inválido: %. Estados válidos: SUCCESS, FAILED, CANCELLED, TIMEOUT', p_status;
    END IF;
    
    -- Obtener tiempo de inicio
    SELECT start_time INTO v_start_time
    FROM control.job_executions 
    WHERE execution_id = p_execution_id;
    
    IF v_start_time IS NULL THEN
        RAISE EXCEPTION 'Ejecución no encontrada o no iniciada: %', p_execution_id;
    END IF;
    
    -- Calcular duración
    v_duration := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::INTEGER;
    
    -- Actualizar ejecución
    UPDATE control.job_executions 
    SET 
        status = p_status,
        end_time = CURRENT_TIMESTAMP,
        duration_seconds = v_duration,
        result_message = p_result_message,
        result_data = p_result_data,
        error_message = p_error_message
    WHERE execution_id = p_execution_id;
    
    -- Log de finalización
    PERFORM control.log_execution(
        p_execution_id, 
        CASE WHEN p_status = 'SUCCESS' THEN 'INFO' ELSE 'ERROR' END,
        FORMAT('Ejecución completada con estado %s en %s segundos', p_status, v_duration)
    );
    
    IF p_error_message IS NOT NULL THEN
        PERFORM control.log_execution(p_execution_id, 'ERROR', p_error_message);
    END IF;
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 4. FUNCIÓN PARA AGREGAR LOGS
-- =====================================================
CREATE OR REPLACE FUNCTION control.log_execution(
    p_execution_id INTEGER,
    p_log_level VARCHAR(10), -- 'INFO', 'WARNING', 'ERROR', 'DEBUG'
    p_log_message TEXT,
    p_step_name VARCHAR(100) DEFAULT NULL,
    p_log_context JSONB DEFAULT NULL
)
RETURNS BOOLEAN AS $$
BEGIN
    INSERT INTO control.execution_logs (
        execution_id, log_level, log_message, 
        step_name, log_context
    ) VALUES (
        p_execution_id, p_log_level, p_log_message,
        p_step_name, p_log_context
    );
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 5. FUNCIÓN PARA OBTENER EJECUCIONES PENDIENTES
-- =====================================================
CREATE OR REPLACE FUNCTION control.get_pending_executions(
    p_limit INTEGER DEFAULT 10,
    p_category VARCHAR(50) DEFAULT NULL
)
RETURNS TABLE (
    execution_id INTEGER,
    job_name VARCHAR(100),
    job_category VARCHAR(50),
    scheduled_time TIMESTAMP,
    execution_date DATE,
    function_name VARCHAR(200),
    schema_name VARCHAR(50),
    parameters JSONB,
    depends_on INTEGER[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        e.execution_id,
        jd.job_name,
        jd.job_category,
        e.scheduled_time,
        e.execution_date,
        jd.function_name,
        jd.schema_name,
        jd.parameters,
        jd.depends_on
    FROM control.job_executions e
    JOIN control.job_definitions jd ON e.job_id = jd.job_id
    WHERE e.status = 'PENDING'
      AND e.scheduled_time <= CURRENT_TIMESTAMP
      AND (p_category IS NULL OR jd.job_category = p_category)
    ORDER BY jd.execution_order, e.scheduled_time
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 6. FUNCIÓN PARA VERIFICAR DEPENDENCIAS
-- =====================================================
CREATE OR REPLACE FUNCTION control.check_dependencies(
    p_execution_id INTEGER
)
RETURNS BOOLEAN AS $$
DECLARE
    v_job_id INTEGER;
    v_execution_date DATE;
    v_depends_on INTEGER[];
    v_dependency_job_id INTEGER;
    v_dependency_status VARCHAR(20);
BEGIN
    -- Obtener información de la ejecución
    SELECT e.job_id, e.execution_date, jd.depends_on
    INTO v_job_id, v_execution_date, v_depends_on
    FROM control.job_executions e
    JOIN control.job_definitions jd ON e.job_id = jd.job_id
    WHERE e.execution_id = p_execution_id;
    
    -- Si no hay dependencias, retornar true
    IF v_depends_on IS NULL OR array_length(v_depends_on, 1) IS NULL THEN
        RETURN TRUE;
    END IF;
    
    -- Verificar cada dependencia
    FOREACH v_dependency_job_id IN ARRAY v_depends_on
    LOOP
        -- Buscar la última ejecución exitosa de la dependencia para la misma fecha
        SELECT status INTO v_dependency_status
        FROM control.job_executions
        WHERE job_id = v_dependency_job_id 
          AND execution_date = v_execution_date
          AND status = 'SUCCESS'
        ORDER BY end_time DESC
        LIMIT 1;
        
        -- Si no se encuentra ejecución exitosa, dependencia no cumplida
        IF v_dependency_status IS NULL THEN
            RETURN FALSE;
        END IF;
    END LOOP;
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 7. FUNCIÓN DE MONITOREO - ESTADO DEL SISTEMA
-- =====================================================
CREATE OR REPLACE FUNCTION control.get_system_status(
    p_days_back INTEGER DEFAULT 7
)
RETURNS TABLE (
    metric_name VARCHAR(50),
    metric_value TEXT,
    metric_count INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH execution_stats AS (
        SELECT 
            status,
            COUNT(*) as count,
            AVG(duration_seconds) as avg_duration
        FROM control.job_executions 
        WHERE created_at >= CURRENT_DATE - INTERVAL '%s days'
        GROUP BY status
    )
    SELECT 
        'executions_' || status as metric_name,
        count::TEXT as metric_value,
        count::INTEGER as metric_count
    FROM execution_stats
    
    UNION ALL
    
    SELECT 
        'pending_executions' as metric_name,
        COUNT(*)::TEXT as metric_value,
        COUNT(*)::INTEGER as metric_count
    FROM control.job_executions 
    WHERE status = 'PENDING'
    
    UNION ALL
    
    SELECT 
        'active_jobs' as metric_name,
        COUNT(*)::TEXT as metric_value,
        COUNT(*)::INTEGER as metric_count
    FROM control.job_definitions 
    WHERE is_enabled = true AND is_active = true;
END;
$$ LANGUAGE plpgsql STABLE;

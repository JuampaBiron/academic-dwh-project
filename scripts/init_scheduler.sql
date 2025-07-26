-- =====================================================
-- SCHEDULER AUTOMÁTICO DEL SISTEMA DE CONTROL ETL
-- Archivo: sql/003_scheduler_functions.sql
-- =====================================================

-- =====================================================
-- 1. FUNCIÓN AUXILIAR PARA CALCULAR PRÓXIMAS EJECUCIONES
-- =====================================================
CREATE OR REPLACE FUNCTION control.calculate_next_execution(
    p_frequency_type VARCHAR(20),
    p_frequency_value VARCHAR(50),
    p_base_date DATE DEFAULT CURRENT_DATE
)
RETURNS TIMESTAMP AS $$
DECLARE
    v_next_execution TIMESTAMP;
    v_hour INTEGER;
    v_minute INTEGER;
    v_day_of_week INTEGER;
    v_day_of_month INTEGER;
BEGIN
    CASE p_frequency_type
        WHEN 'manual' THEN
            -- No programar automáticamente
            RETURN NULL;
            
        WHEN 'daily' THEN
            -- Format: "HH:MM" o expresión cron simplificada "0 HH * * *"
            IF p_frequency_value ~ '^[0-9]{1,2}:[0-9]{2}$' THEN
                -- Formato HH:MM
                v_hour := split_part(p_frequency_value, ':', 1)::INTEGER;
                v_minute := split_part(p_frequency_value, ':', 2)::INTEGER;
            ELSIF p_frequency_value ~ '^[0-9]+ [0-9]+ \* \* \*$' THEN
                -- Formato cron: "MIN HOUR * * *"
                v_minute := split_part(p_frequency_value, ' ', 1)::INTEGER;
                v_hour := split_part(p_frequency_value, ' ', 2)::INTEGER;
            ELSE
                -- Default: 02:00
                v_hour := 2;
                v_minute := 0;
            END IF;
            
            v_next_execution := p_base_date + INTERVAL '1 day' + 
                               make_interval(hours => v_hour, mins => v_minute);
                               
        WHEN 'weekly' THEN
            -- Format: "monday 02:00" o "1 02:00" (1=Monday)
            IF p_frequency_value ~ '^[a-zA-Z]+' THEN
                -- Día de la semana en texto
                v_day_of_week := CASE LOWER(split_part(p_frequency_value, ' ', 1))
                    WHEN 'monday' THEN 1
                    WHEN 'tuesday' THEN 2
                    WHEN 'wednesday' THEN 3
                    WHEN 'thursday' THEN 4
                    WHEN 'friday' THEN 5
                    WHEN 'saturday' THEN 6
                    WHEN 'sunday' THEN 0
                    ELSE 1
                END;
            ELSE
                -- Día de la semana numérico
                v_day_of_week := split_part(p_frequency_value, ' ', 1)::INTEGER;
            END IF;
            
            -- Extraer hora (default 02:00)
            IF array_length(string_to_array(p_frequency_value, ' '), 1) >= 2 THEN
                v_hour := split_part(split_part(p_frequency_value, ' ', 2), ':', 1)::INTEGER;
                v_minute := COALESCE(split_part(split_part(p_frequency_value, ' ', 2), ':', 2)::INTEGER, 0);
            ELSE
                v_hour := 2;
                v_minute := 0;
            END IF;
            
            -- Calcular próximo día de la semana
            v_next_execution := date_trunc('week', p_base_date + INTERVAL '1 week') + 
                               INTERVAL '1 day' * v_day_of_week +
                               make_interval(hours => v_hour, mins => v_minute);
                               
        WHEN 'monthly' THEN
            -- Format: "1 02:00" (día 1 del mes a las 02:00)
            v_day_of_month := split_part(p_frequency_value, ' ', 1)::INTEGER;
            
            IF array_length(string_to_array(p_frequency_value, ' '), 1) >= 2 THEN
                v_hour := split_part(split_part(p_frequency_value, ' ', 2), ':', 1)::INTEGER;
                v_minute := COALESCE(split_part(split_part(p_frequency_value, ' ', 2), ':', 2)::INTEGER, 0);
            ELSE
                v_hour := 2;
                v_minute := 0;
            END IF;
            
            -- Próximo mes, día específico
            v_next_execution := date_trunc('month', p_base_date + INTERVAL '1 month') + 
                               INTERVAL '1 day' * (v_day_of_month - 1) +
                               make_interval(hours => v_hour, mins => v_minute);
                               
        WHEN 'cron' THEN
            -- Implementación simplificada de cron
            -- Format: "MIN HOUR DAY MONTH DAYOFWEEK"
            -- Por ahora, solo soporte básico - se puede expandir
            PERFORM control.log_execution(
                NULL, 'WARNING', 
                'Expresiones cron complejas no totalmente soportadas: ' || p_frequency_value
            );
            
            -- Default para cron no reconocido
            v_next_execution := p_base_date + INTERVAL '1 day' + INTERVAL '2 hours';
            
        ELSE
            RAISE EXCEPTION 'Tipo de frecuencia no soportado: %', p_frequency_type;
    END CASE;
    
    RETURN v_next_execution;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 2. FUNCIÓN PARA VERIFICAR SI YA EXISTE EJECUCIÓN
-- =====================================================
CREATE OR REPLACE FUNCTION control.execution_exists(
    p_job_id INTEGER,
    p_execution_date DATE
)
RETURNS BOOLEAN AS $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM control.job_executions
    WHERE job_id = p_job_id 
      AND execution_date = p_execution_date
      AND status IN ('PENDING', 'RUNNING', 'SUCCESS');
    
    RETURN v_count > 0;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 3. FUNCIÓN PRINCIPAL DEL SCHEDULER
-- =====================================================
CREATE OR REPLACE FUNCTION control.run_scheduler(
    p_days_ahead INTEGER DEFAULT 7,
    p_force_recreate BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
    job_name VARCHAR(100),
    execution_date DATE,
    scheduled_time TIMESTAMP,
    action VARCHAR(20)
) AS $$
DECLARE
    v_job RECORD;
    v_target_date DATE;
    v_next_execution TIMESTAMP;
    v_execution_id INTEGER;
    v_action VARCHAR(20);
    v_total_created INTEGER := 0;
    v_total_skipped INTEGER := 0;
BEGIN
    -- Log inicio del scheduler
    RAISE NOTICE 'Iniciando scheduler para los próximos % días', p_days_ahead;
    
    -- Iterar sobre cada trabajo habilitado
    FOR v_job IN 
        SELECT job_id, job_name, job_category, frequency_type, frequency_value, execution_order
        FROM control.job_definitions
        WHERE is_enabled = true AND is_active = true
          AND frequency_type != 'manual'
        ORDER BY execution_order, job_id
    LOOP
        -- Iterar sobre cada día en el rango
        FOR i IN 0..p_days_ahead-1 LOOP
            v_target_date := CURRENT_DATE + i;
            
            -- Verificar si ya existe ejecución para esta fecha
            IF NOT p_force_recreate AND control.execution_exists(v_job.job_id, v_target_date) THEN
                v_action := 'SKIPPED';
                v_total_skipped := v_total_skipped + 1;
            ELSE
                -- Calcular hora de ejecución para esta fecha
                v_next_execution := control.calculate_next_execution(
                    v_job.frequency_type,
                    v_job.frequency_value,
                    v_target_date - INTERVAL '1 day'  -- Base date anterior para calcular correctamente
                );
                
                -- Si hay ejecución calculada y es para la fecha objetivo
                IF v_next_execution IS NOT NULL AND DATE(v_next_execution) = v_target_date THEN
                    
                    -- Si force_recreate, eliminar ejecuciones existentes
                    IF p_force_recreate THEN
                        DELETE FROM control.job_executions 
                        WHERE job_id = v_job.job_id 
                          AND execution_date = v_target_date 
                          AND status = 'PENDING';
                    END IF;
                    
                    -- Crear nueva ejecución
                    INSERT INTO control.job_executions (
                        job_id, scheduled_time, execution_date, status, max_attempts
                    ) VALUES (
                        v_job.job_id, 
                        v_next_execution, 
                        v_target_date,
                        'PENDING',
                        (SELECT max_retries + 1 FROM control.job_definitions WHERE job_id = v_job.job_id)
                    ) RETURNING execution_id INTO v_execution_id;
                    
                    v_action := 'CREATED';
                    v_total_created := v_total_created + 1;
                    
                    -- Log de creación
                    PERFORM control.log_execution(
                        v_execution_id,
                        'INFO',
                        FORMAT('Ejecución programada por scheduler para %s', v_target_date)
                    );
                ELSE
                    v_action := 'NO_MATCH';
                END IF;
            END IF;
            
            -- Retornar información de esta iteración
            RETURN QUERY SELECT 
                v_job.job_name::VARCHAR(100),
                v_target_date::DATE,
                v_next_execution::TIMESTAMP,
                v_action::VARCHAR(20);
        END LOOP;
    END LOOP;
    
    -- Log resumen
    RAISE NOTICE 'Scheduler completado: % ejecuciones creadas, % omitidas', 
                 v_total_created, v_total_skipped;
    
    -- Insertar log general del scheduler
    INSERT INTO control.execution_logs (
        execution_id, log_level, log_message, step_name
    ) VALUES (
        NULL, 'INFO', 
        FORMAT('Scheduler ejecutado: %s ejecuciones creadas, %s omitidas', 
               v_total_created, v_total_skipped),
        'scheduler'
    );
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 4. FUNCIÓN PARA LIMPIAR EJECUCIONES ANTIGUAS
-- =====================================================
CREATE OR REPLACE FUNCTION control.cleanup_old_executions(
    p_days_to_keep INTEGER DEFAULT 30
)
RETURNS INTEGER AS $$
DECLARE
    v_deleted_count INTEGER;
    v_cutoff_date DATE;
BEGIN
    v_cutoff_date := CURRENT_DATE - p_days_to_keep;
    
    -- Eliminar logs antiguos primero (por foreign key)
    DELETE FROM control.execution_logs 
    WHERE execution_id IN (
        SELECT execution_id 
        FROM control.job_executions 
        WHERE execution_date < v_cutoff_date
    );
    
    -- Eliminar ejecuciones antiguas
    DELETE FROM control.job_executions 
    WHERE execution_date < v_cutoff_date;
    
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    
    -- Log de limpieza
    INSERT INTO control.execution_logs (
        execution_id, log_level, log_message, step_name
    ) VALUES (
        NULL, 'INFO', 
        FORMAT('Limpieza completada: %s ejecuciones eliminadas anteriores a %s', 
               v_deleted_count, v_cutoff_date),
        'cleanup'
    );
    
    RETURN v_deleted_count;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 5. FUNCIÓN DE MANTENIMIENTO COMPLETO
-- =====================================================
CREATE OR REPLACE FUNCTION control.maintenance_scheduler(
    p_days_ahead INTEGER DEFAULT 7,
    p_cleanup_days INTEGER DEFAULT 30,
    p_force_recreate BOOLEAN DEFAULT FALSE
)
RETURNS TEXT AS $$
DECLARE
    v_result TEXT;
    v_cleanup_count INTEGER;
    v_schedule_count INTEGER;
BEGIN
    -- 1. Limpiar ejecuciones antiguas
    SELECT control.cleanup_old_executions(p_cleanup_days) INTO v_cleanup_count;
    
    -- 2. Ejecutar scheduler
    SELECT COUNT(*) INTO v_schedule_count
    FROM control.run_scheduler(p_days_ahead, p_force_recreate)
    WHERE action = 'CREATED';
    
    -- 3. Generar reporte
    v_result := FORMAT(
        'Mantenimiento completado - Ejecuciones programadas: %s, Registros limpiados: %s',
        v_schedule_count, v_cleanup_count
    );
    
    RETURN v_result;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 6. VISTA PARA MONITOREAR PROGRAMACIÓN
-- =====================================================
CREATE OR REPLACE VIEW control.v_scheduled_executions AS
SELECT 
    jd.job_name,
    jd.job_category,
    jd.frequency_type,
    jd.frequency_value,
    e.execution_date,
    e.scheduled_time,
    e.status,
    e.created_at as programmed_at,
    CASE 
        WHEN e.scheduled_time <= CURRENT_TIMESTAMP AND e.status = 'PENDING' THEN 'READY'
        WHEN e.scheduled_time > CURRENT_TIMESTAMP AND e.status = 'PENDING' THEN 'SCHEDULED'
        ELSE e.status
    END as execution_status
FROM control.job_executions e
JOIN control.job_definitions jd ON e.job_id = jd.job_id
WHERE e.execution_date >= CURRENT_DATE - INTERVAL '1 day'
  AND e.execution_date <= CURRENT_DATE + INTERVAL '7 days'
ORDER BY e.scheduled_time;

-- =====================================================
-- COMENTARIOS Y DOCUMENTACIÓN
-- =====================================================
COMMENT ON FUNCTION control.run_scheduler IS 'Función principal del scheduler que crea ejecuciones automáticas basadas en job_definitions';
COMMENT ON FUNCTION control.calculate_next_execution IS 'Calcula la próxima hora de ejecución basada en el tipo y valor de frecuencia';
COMMENT ON FUNCTION control.cleanup_old_executions IS 'Limpia ejecuciones y logs antiguos para mantener la base de datos optimizada';
COMMENT ON VIEW control.v_scheduled_executions IS 'Vista de monitoreo que muestra las ejecuciones programadas y su estado';

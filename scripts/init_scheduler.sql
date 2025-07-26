-- =====================================================
-- SCHEDULER SIMPLIFICADO QUE FUNCIONA
-- =====================================================

-- =====================================================
-- FUNCIÓN SIMPLE PARA AGREGAR JOBS PARA HOY
-- =====================================================
CREATE OR REPLACE FUNCTION control.schedule_jobs_today()
RETURNS TEXT AS 
'
DECLARE
    v_job RECORD;
    v_execution_id INTEGER;
    v_scheduled_time TIMESTAMP;
    v_count INTEGER := 0;
BEGIN
    -- Iterar sobre jobs habilitados
    FOR v_job IN 
        SELECT job_id, job_name, execution_order
        FROM control.job_definitions
        WHERE is_enabled = true AND is_active = true
        ORDER BY execution_order
    LOOP
        -- Calcular hora (2 AM + orden * 1 hora)
        v_scheduled_time := CURRENT_DATE + INTERVAL ''2 hours'' + 
                           INTERVAL ''1 hour'' * (v_job.execution_order - 1);
        
        -- Verificar si ya existe
        IF NOT EXISTS (
            SELECT 1 FROM control.job_executions 
            WHERE job_id = v_job.job_id 
              AND execution_date = CURRENT_DATE
        ) THEN
            -- Crear ejecución
            INSERT INTO control.job_executions (
                job_id, scheduled_time, execution_date, status, max_attempts
            ) VALUES (
                v_job.job_id, v_scheduled_time, CURRENT_DATE, ''PENDING'', 3
            ) RETURNING execution_id INTO v_execution_id;
            
            v_count := v_count + 1;
            
            -- Log
            INSERT INTO control.execution_logs (
                execution_id, log_level, log_message
            ) VALUES (
                v_execution_id, ''INFO'', 
                ''Job programado: '' || v_job.job_name || '' para '' || v_scheduled_time
            );
        END IF;
    END LOOP;
    
    RETURN ''Jobs programados: '' || v_count;
END;
' LANGUAGE plpgsql;

-- =====================================================
-- FUNCIÓN PARA VER JOBS PENDIENTES DE HOY
-- =====================================================
CREATE OR REPLACE FUNCTION control.get_todays_jobs()
RETURNS TABLE (
    job_name VARCHAR(100),
    scheduled_time TIMESTAMP,
    status VARCHAR(20)
) AS 
'
BEGIN
    RETURN QUERY
    SELECT 
        jd.job_name,
        je.scheduled_time,
        je.status
    FROM control.job_executions je
    JOIN control.job_definitions jd ON je.job_id = jd.job_id
    WHERE je.execution_date = CURRENT_DATE
    ORDER BY je.scheduled_time;
END;
' LANGUAGE plpgsql;

-- =====================================================
-- FUNCIÓN PARA LIMPIAR JOBS PENDIENTES DE HOY
-- =====================================================
CREATE OR REPLACE FUNCTION control.clear_todays_pending()
RETURNS INTEGER AS 
'
DECLARE
    v_deleted INTEGER;
BEGIN
    DELETE FROM control.job_executions 
    WHERE execution_date = CURRENT_DATE 
      AND status = ''PENDING'';
    
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    
    INSERT INTO control.execution_logs (
        execution_id, log_level, log_message
    ) VALUES (
        NULL, ''INFO'', 
        ''Jobs pendientes eliminados: '' || v_deleted
    );
    
    RETURN v_deleted;
END;
' LANGUAGE plpgsql;

-- =====================================================
-- FUNCIÓN PARA PROGRAMAR JOBS PARA MAÑANA
-- =====================================================
CREATE OR REPLACE FUNCTION control.schedule_jobs_tomorrow()
RETURNS TEXT AS 
'
DECLARE
    v_job RECORD;
    v_execution_id INTEGER;
    v_scheduled_time TIMESTAMP;
    v_tomorrow DATE;
    v_count INTEGER := 0;
BEGIN
    v_tomorrow := CURRENT_DATE + 1;
    
    -- Iterar sobre jobs habilitados
    FOR v_job IN 
        SELECT job_id, job_name, execution_order
        FROM control.job_definitions
        WHERE is_enabled = true AND is_active = true
        ORDER BY execution_order
    LOOP
        -- Calcular hora para mañana
        v_scheduled_time := v_tomorrow + INTERVAL ''2 hours'' + 
                           INTERVAL ''1 hour'' * (v_job.execution_order - 1);
        
        -- Verificar si ya existe
        IF NOT EXISTS (
            SELECT 1 FROM control.job_executions 
            WHERE job_id = v_job.job_id 
              AND execution_date = v_tomorrow
        ) THEN
            -- Crear ejecución
            INSERT INTO control.job_executions (
                job_id, scheduled_time, execution_date, status, max_attempts
            ) VALUES (
                v_job.job_id, v_scheduled_time, v_tomorrow, ''PENDING'', 3
            ) RETURNING execution_id INTO v_execution_id;
            
            v_count := v_count + 1;
        END IF;
    END LOOP;
    
    RETURN ''Jobs programados para mañana: '' || v_count;
END;
' LANGUAGE plpgsql;

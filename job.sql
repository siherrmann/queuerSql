CREATE OR REPLACE FUNCTION init_job() RETURNS VOID AS $$
BEGIN
    -- Create pgcrypto extension
    CREATE EXTENSION IF NOT EXISTS pgcrypto;
    
    -- Create job table
    CREATE TABLE IF NOT EXISTS job (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        rid UUID UNIQUE DEFAULT gen_random_uuid(),
        worker_id BIGINT DEFAULT 0,
        worker_rid UUID DEFAULT NULL,
        options JSONB DEFAULT '{}',
        task_name VARCHAR(100) DEFAULT '',
        parameters JSONB DEFAULT '[]',
        status VARCHAR(50) DEFAULT 'QUEUED',
        scheduled_at TIMESTAMP DEFAULT NULL,
        started_at TIMESTAMP DEFAULT NULL,
        schedule_count INT DEFAULT 0,
        attempts INT DEFAULT 0,
        results JSONB DEFAULT '[]',
        results_encrypted BYTEA DEFAULT '',
        error TEXT DEFAULT '',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create job_archive table with TimescaleDB hypertable
    CREATE TABLE IF NOT EXISTS job_archive (
        LIKE job INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
        PRIMARY KEY (id, updated_at)
    );
    
    -- Create hypertable (TimescaleDB feature)
    PERFORM create_hypertable('job_archive', by_range('updated_at'), if_not_exists => TRUE);
    
    -- Create triggers for notifications
    CREATE OR REPLACE TRIGGER job_notify_event
        BEFORE INSERT OR UPDATE ON job
        FOR EACH ROW EXECUTE PROCEDURE notify_event();
    
    CREATE OR REPLACE TRIGGER job_archive_notify_event
        BEFORE INSERT ON job_archive
        FOR EACH ROW EXECUTE PROCEDURE notify_event();
    
    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_next_interval
        ON job
        USING HASH ((options->'schedule'->>'next_interval'));
    
    CREATE INDEX IF NOT EXISTS idx_job_worker_id ON job (worker_id);
    CREATE INDEX IF NOT EXISTS idx_job_worker_rid ON job (worker_rid);
    CREATE INDEX IF NOT EXISTS idx_job_status ON job (status);
    CREATE INDEX IF NOT EXISTS idx_job_created_at ON job (created_at);
    CREATE INDEX IF NOT EXISTS idx_job_updated_at ON job (updated_at);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_job_initial(input_worker_id BIGINT)
RETURNS TABLE (
    id BIGINT,
    rid UUID,
    worker_id BIGINT,
    worker_rid UUID,
    options JSONB,
    task_name VARCHAR(100),
    parameters JSONB,
    status VARCHAR(50),
    scheduled_at TIMESTAMP,
    started_at TIMESTAMP,
    schedule_count INT,
    attempts INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    WITH current_concurrency AS (
        SELECT COUNT(*) AS count
        FROM job
        WHERE job.worker_id = input_worker_id
        AND job.status = 'RUNNING'
    ),
    current_worker AS (
        SELECT
            worker.id,
            worker.rid,
            worker.available_tasks,
            worker.available_next_interval,
            worker.max_concurrency,
            COALESCE(cc.count, 0) AS current_concurrency
        FROM worker, current_concurrency AS cc
        WHERE worker.id = input_worker_id
        AND (worker.max_concurrency > COALESCE(cc.count, 0))
        FOR UPDATE
    ),
    job_ids AS (
        SELECT j.id
        FROM current_worker AS cw, current_concurrency AS cc,
        LATERAL (
            SELECT job.id
            FROM job
            WHERE
                job.task_name = ANY(cw.available_tasks::VARCHAR[])
                AND (
                    job.options->'schedule'->>'next_interval' IS NULL
                    OR job.options->'schedule'->>'next_interval' = ''
                    OR job.options->'schedule'->>'next_interval' = ANY(cw.available_next_interval::VARCHAR[])
                )
                AND (
                    job.status = 'QUEUED'
                    OR (job.status = 'SCHEDULED' AND job.scheduled_at <= (CURRENT_TIMESTAMP + INTERVAL '10 minutes'))
                )
            ORDER BY job.created_at ASC
            LIMIT (cw.max_concurrency - COALESCE(cc.count, 0))
            FOR UPDATE SKIP LOCKED
        ) AS j
    )
    UPDATE job SET
        worker_id = cw.id,
        worker_rid = cw.rid,
        status = 'RUNNING',
        started_at = CURRENT_TIMESTAMP,
        schedule_count = job.schedule_count + 1,
        attempts = job.attempts + 1,
        updated_at = CURRENT_TIMESTAMP
    FROM current_worker AS cw, job_ids
    WHERE job.id = ANY(SELECT job_ids.id FROM job_ids)
    AND EXISTS (SELECT 1 FROM current_worker)
    RETURNING
        job.id,
        job.rid,
        job.worker_id,
        job.worker_rid,
        job.options,
        job.task_name,
        job.parameters,
        job.status,
        job.scheduled_at,
        job.started_at,
        job.schedule_count,
        job.attempts,
        job.created_at,
        job.updated_at;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_job_final(
    input_id BIGINT,
    input_status VARCHAR(50),
    input_results JSONB,
    input_error TEXT
)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_worker_id BIGINT,
    output_worker_rid UUID,
    output_options JSONB,
    output_task_name VARCHAR(100),
    output_parameters JSONB,
    output_status VARCHAR(50),
    output_scheduled_at TIMESTAMP,
    output_started_at TIMESTAMP,
    output_schedule_count INT,
    output_attempts INT,
    output_results JSONB,
    output_error TEXT,
    output_created_at TIMESTAMP,
    output_updated_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    WITH jobs_old AS (
        DELETE FROM job
        WHERE id = input_id
        RETURNING
            id,
            rid,
            worker_id,
            worker_rid,
            options,
            task_name,
            parameters,
            scheduled_at,
            started_at,
            schedule_count,
            attempts,
            created_at,
            updated_at
    )
    INSERT INTO job_archive (
        id,
        rid,
        worker_id,
        worker_rid,
        options,
        task_name,
        parameters,
        status,
        scheduled_at,
        started_at,
        schedule_count,
        attempts,
        results,
        error,
        created_at,
        updated_at
    )
    SELECT
        jobs_old.id,
        jobs_old.rid,
        jobs_old.worker_id,
        jobs_old.worker_rid,
        jobs_old.options,
        jobs_old.task_name,
        jobs_old.parameters,
        input_status,
        jobs_old.scheduled_at,
        jobs_old.started_at,
        jobs_old.schedule_count,
        jobs_old.attempts,
        input_results,
        input_error,
        jobs_old.created_at,
        CURRENT_TIMESTAMP
    FROM jobs_old
    RETURNING
        id,
        rid,
        worker_id,
        worker_rid,
        options,
        task_name,
        parameters,
        status,
        scheduled_at,
        started_at,
        schedule_count,
        attempts,
        results,
        error,
        created_at,
        updated_at;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_job_final_encrypted(
    input_id BIGINT,
    input_status VARCHAR(50),
    input_results TEXT,
    input_error TEXT,
    input_encryption_key TEXT
)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_worker_id BIGINT,
    output_worker_rid UUID,
    output_options JSONB,
    output_task_name VARCHAR(100),
    output_parameters JSONB,
    output_status VARCHAR(50),
    output_scheduled_at TIMESTAMP,
    output_started_at TIMESTAMP,
    output_schedule_count INT,
    output_attempts INT,
    output_results JSONB,
    output_error TEXT,
    output_created_at TIMESTAMP,
    output_updated_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    WITH jobs_old AS (
        DELETE FROM job
        WHERE id = input_id
        RETURNING
            id,
            rid,
            worker_id,
            worker_rid,
            options,
            task_name,
            parameters,
            scheduled_at,
            started_at,
            schedule_count,
            attempts,
            created_at,
            updated_at
    )
    INSERT INTO job_archive (
        id,
        rid,
        worker_id,
        worker_rid,
        options,
        task_name,
        parameters,
        status,
        scheduled_at,
        started_at,
        schedule_count,
        attempts,
        results_encrypted,
        error,
        created_at,
        updated_at
    )
    SELECT
        jobs_old.id,
        jobs_old.rid,
        jobs_old.worker_id,
        jobs_old.worker_rid,
        jobs_old.options,
        jobs_old.task_name,
        jobs_old.parameters,
        input_status,
        jobs_old.scheduled_at,
        jobs_old.started_at,
        jobs_old.schedule_count,
        jobs_old.attempts,
        pgp_sym_encrypt(input_results, input_encryption_key),
        input_error,
        jobs_old.created_at,
        CURRENT_TIMESTAMP
    FROM jobs_old
    RETURNING
        id,
        rid,
        worker_id,
        worker_rid,
        options,
        task_name,
        parameters,
        status,
        scheduled_at,
        started_at,
        schedule_count,
        attempts,
        input_results::JSONB,
        error,
        created_at,
        updated_at;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_stale_jobs(
    input_new_status VARCHAR(50),
    input_excluded_status1 VARCHAR(50),
    input_excluded_status2 VARCHAR(50),
    input_excluded_status3 VARCHAR(50),
    input_worker_status VARCHAR(50)
)
RETURNS INT
AS $$
DECLARE
    affected_rows INT;
BEGIN
    UPDATE job 
    SET status = input_new_status 
    WHERE status NOT IN (input_excluded_status1, input_excluded_status2, input_excluded_status3)
      AND worker_rid IN (
          SELECT rid 
          FROM worker 
          WHERE status = input_worker_status
      );
    
    GET DIAGNOSTICS affected_rows = ROW_COUNT;
    RETURN affected_rows;
END;
$$ LANGUAGE plpgsql;
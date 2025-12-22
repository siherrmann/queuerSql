CREATE OR REPLACE FUNCTION init_job() RETURNS VOID AS $$
BEGIN
    -- Create TimescaleDB extension
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

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
        parameters_encrypted BYTEA DEFAULT '',
        parameters_keyed JSONB DEFAULT '{}',
        parameters_keyed_encrypted BYTEA DEFAULT '',
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

CREATE OR REPLACE FUNCTION insert_job(
    input_options JSONB,
    input_task_name VARCHAR(100),
    input_parameters JSONB,
    input_parameters_keyed JSONB,
    input_status VARCHAR(50),
    input_scheduled_at TIMESTAMP,
    input_schedule_count INT
)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_worker_id BIGINT,
    output_worker_rid UUID,
    output_options JSONB,
    output_task_name VARCHAR(100),
    output_parameters JSONB,
    output_parameters_keyed JSONB,
    output_status VARCHAR(50),
    output_scheduled_at TIMESTAMP,
    output_schedule_count INT,
    output_attempts INT,
    output_created_at TIMESTAMP,
    output_updated_at TIMESTAMP
)
AS $$
BEGIN
    RETURN QUERY
    INSERT INTO job (options, task_name, parameters, parameters_keyed, status, scheduled_at, schedule_count)
    VALUES (input_options, input_task_name, input_parameters, input_parameters_keyed, input_status, input_scheduled_at, input_schedule_count)
    RETURNING
        job.id,
        job.rid,
        job.worker_id,
        job.worker_rid,
        job.options,
        job.task_name,
        job.parameters,
        job.parameters_keyed,
        job.status,
        job.scheduled_at,
        job.schedule_count,
        job.attempts,
        job.created_at,
        job.updated_at;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_job_initial(input_worker_id BIGINT)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_worker_id BIGINT,
    output_worker_rid UUID,
    output_options JSONB,
    output_task_name VARCHAR(100),
    output_parameters JSONB,
    output_parameters_keyed JSONB,
    output_status VARCHAR(50),
    output_scheduled_at TIMESTAMP,
    output_started_at TIMESTAMP,
    output_schedule_count INT,
    output_attempts INT,
    output_created_at TIMESTAMP,
    output_updated_at TIMESTAMP
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
        job.parameters_keyed,
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
    output_parameters_keyed JSONB,
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
            parameters_encrypted,
            parameters_keyed,
            parameters_keyed_encrypted,
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
        parameters_encrypted,
        parameters_keyed,
        parameters_keyed_encrypted,
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
        jobs_old.parameters_encrypted,
        jobs_old.parameters_keyed,
        jobs_old.parameters_keyed_encrypted,
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
        parameters_keyed,
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
    output_parameters_keyed JSONB,
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
            parameters_encrypted,
            parameters_keyed,
            parameters_keyed_encrypted,
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
        parameters_encrypted,
        parameters_keyed,
        parameters_keyed_encrypted,
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
        jobs_old.parameters_encrypted,
        jobs_old.parameters_keyed,
        jobs_old.parameters_keyed_encrypted,
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
        parameters_keyed,
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

CREATE OR REPLACE FUNCTION select_job(input_encryption_key TEXT, input_rid UUID)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_worker_id BIGINT,
    output_worker_rid UUID,
    output_options JSONB,
    output_task_name VARCHAR(100),
    output_parameters JSONB,
    output_parameters_keyed JSONB,
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
    SELECT
        job.id,
        job.rid,
        job.worker_id,
        job.worker_rid,
        job.options,
        job.task_name,
        CASE
            WHEN octet_length(job.parameters_encrypted) > 0 THEN pgp_sym_decrypt(job.parameters_encrypted, input_encryption_key::text)::jsonb
            ELSE job.parameters
        END AS parameters,
        CASE
            WHEN octet_length(job.parameters_keyed_encrypted) > 0 THEN pgp_sym_decrypt(job.parameters_keyed_encrypted, input_encryption_key::text)::jsonb
            ELSE job.parameters_keyed
        END AS parameters_keyed,
        job.status,
        job.scheduled_at,
        job.started_at,
        job.schedule_count,
        job.attempts,
        CASE
            WHEN octet_length(job.results_encrypted) > 0 THEN pgp_sym_decrypt(job.results_encrypted, input_encryption_key::text)::jsonb
            ELSE job.results
        END AS results,
        job.error,
        job.created_at,
        job.updated_at
    FROM job
    WHERE job.rid = input_rid;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION select_all_jobs(input_encryption_key TEXT, input_last_id INT, input_entries INT)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_worker_id BIGINT,
    output_worker_rid UUID,
    output_options JSONB,
    output_task_name VARCHAR(100),
    output_parameters JSONB,
    output_parameters_keyed JSONB,
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
    SELECT
        job.id,
        job.rid,
        job.worker_id,
        job.worker_rid,
        job.options,
        job.task_name,
        CASE
            WHEN octet_length(job.parameters_encrypted) > 0 THEN pgp_sym_decrypt(job.parameters_encrypted, input_encryption_key::text)::jsonb
            ELSE job.parameters
        END AS parameters,
        CASE
            WHEN octet_length(job.parameters_keyed_encrypted) > 0 THEN pgp_sym_decrypt(job.parameters_keyed_encrypted, input_encryption_key::text)::jsonb
            ELSE job.parameters_keyed
        END AS parameters_keyed,
        job.status,
        job.scheduled_at,
        job.started_at,
        job.schedule_count,
        job.attempts,
        CASE
            WHEN octet_length(job.results_encrypted) > 0 THEN pgp_sym_decrypt(job.results_encrypted, input_encryption_key::text)::jsonb
            ELSE job.results
        END AS results,
        job.error,
        job.created_at,
        job.updated_at
    FROM job
    WHERE (0 = input_last_id
        OR job.created_at < (
            SELECT d.created_at
            FROM job AS d
            WHERE d.id = input_last_id))
    ORDER BY job.created_at DESC
    LIMIT input_entries;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION select_all_jobs_by_worker_rid(
    input_encryption_key TEXT,
    input_worker_rid UUID,
    input_last_id INT,
    input_entries INT
)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_worker_id BIGINT,
    output_worker_rid UUID,
    output_options JSONB,
    output_task_name VARCHAR(100),
    output_parameters JSONB,
    output_parameters_keyed JSONB,
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
    SELECT
        job.id,
        job.rid,
        job.worker_id,
        job.worker_rid,
        job.options,
        job.task_name,
        CASE
            WHEN octet_length(job.parameters_encrypted) > 0 THEN pgp_sym_decrypt(job.parameters_encrypted, input_encryption_key::text)::jsonb
            ELSE job.parameters
        END AS parameters,
        CASE
            WHEN octet_length(job.parameters_keyed_encrypted) > 0 THEN pgp_sym_decrypt(job.parameters_keyed_encrypted, input_encryption_key::text)::jsonb
            ELSE job.parameters_keyed
        END AS parameters_keyed,
        job.status,
        job.scheduled_at,
        job.started_at,
        job.schedule_count,
        job.attempts,
        CASE
            WHEN octet_length(job.results_encrypted) > 0 THEN pgp_sym_decrypt(job.results_encrypted, input_encryption_key::text)::jsonb
            ELSE job.results
        END AS results,
        job.error,
        job.created_at,
        job.updated_at
    FROM job
    WHERE job.worker_rid = input_worker_rid
        AND (0 = input_last_id
            OR job.created_at < (
                SELECT d.created_at
                FROM job AS d
                WHERE d.id = input_last_id))
    ORDER BY job.created_at DESC
    LIMIT input_entries;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION select_all_jobs_by_search(
    input_encryption_key TEXT,
    input_search TEXT,
    input_last_id INT,
    input_entries INT
)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_worker_id BIGINT,
    output_worker_rid UUID,
    output_options JSONB,
    output_task_name VARCHAR(100),
    output_parameters JSONB,
    output_parameters_keyed JSONB,
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
    SELECT
        job.id,
        job.rid,
        job.worker_id,
        job.worker_rid,
        job.options,
        job.task_name,
        CASE
            WHEN octet_length(job.parameters_encrypted) > 0 THEN pgp_sym_decrypt(job.parameters_encrypted, input_encryption_key::text)::jsonb
            ELSE job.parameters
        END AS parameters,
        CASE
            WHEN octet_length(job.parameters_keyed_encrypted) > 0 THEN pgp_sym_decrypt(job.parameters_keyed_encrypted, input_encryption_key::text)::jsonb
            ELSE job.parameters_keyed
        END AS parameters_keyed,
        job.status,
        job.scheduled_at,
        job.started_at,
        job.schedule_count,
        job.attempts,
        CASE
            WHEN octet_length(job.results_encrypted) > 0 THEN pgp_sym_decrypt(job.results_encrypted, input_encryption_key::text)::jsonb
            ELSE job.results
        END AS results,
        job.error,
        job.created_at,
        job.updated_at
    FROM job
    WHERE (job.rid::text ILIKE '%' || input_search || '%'
            OR job.worker_id::text ILIKE '%' || input_search || '%'
            OR job.task_name ILIKE '%' || input_search || '%'
            OR job.status ILIKE '%' || input_search || '%')
        AND (0 = input_last_id
            OR job.created_at < (
                SELECT u.created_at
                FROM job AS u
                WHERE u.id = input_last_id))
    ORDER BY job.created_at DESC
    LIMIT input_entries;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_retention_archive(input_retention_days INT)
RETURNS VOID AS $$
BEGIN
    PERFORM add_retention_policy('job_archive', (input_retention_days * INTERVAL '1 day'));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION remove_retention_archive()
RETURNS VOID AS $$
BEGIN
    PERFORM remove_retention_policy('job_archive');
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_job(input_rid UUID)
RETURNS VOID AS $$
BEGIN
    DELETE FROM job_archive WHERE rid = input_rid;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION select_job_from_archive(input_encryption_key TEXT, input_rid UUID)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_worker_id BIGINT,
    output_worker_rid UUID,
    output_options JSONB,
    output_task_name VARCHAR(100),
    output_parameters JSONB,
    output_parameters_keyed JSONB,
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
    SELECT
        job_archive.id,
        job_archive.rid,
        job_archive.worker_id,
        job_archive.worker_rid,
        job_archive.options,
        job_archive.task_name,
        CASE
            WHEN octet_length(job_archive.parameters_encrypted) > 0 THEN pgp_sym_decrypt(job_archive.parameters_encrypted, input_encryption_key::text)::jsonb
            ELSE job_archive.parameters
        END AS parameters,
        CASE
            WHEN octet_length(job_archive.parameters_keyed_encrypted) > 0 THEN pgp_sym_decrypt(job_archive.parameters_keyed_encrypted, input_encryption_key::text)::jsonb
            ELSE job_archive.parameters_keyed
        END AS parameters_keyed,
        job_archive.status,
        job_archive.scheduled_at,
        job_archive.started_at,
        job_archive.schedule_count,
        job_archive.attempts,
        CASE
            WHEN octet_length(job_archive.results_encrypted) > 0 THEN pgp_sym_decrypt(job_archive.results_encrypted, input_encryption_key::text)::jsonb
            ELSE job_archive.results
        END AS results,
        job_archive.error,
        job_archive.created_at,
        job_archive.updated_at
    FROM job_archive
    WHERE job_archive.rid = input_rid;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION select_all_jobs_from_archive(input_encryption_key TEXT, input_last_id INT, input_entries INT)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_worker_id BIGINT,
    output_worker_rid UUID,
    output_options JSONB,
    output_task_name VARCHAR(100),
    output_parameters JSONB,
    output_parameters_keyed JSONB,
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
    SELECT
        job_archive.id,
        job_archive.rid,
        job_archive.worker_id,
        job_archive.worker_rid,
        job_archive.options,
        job_archive.task_name,
        CASE
            WHEN octet_length(job_archive.parameters_encrypted) > 0 THEN pgp_sym_decrypt(job_archive.parameters_encrypted, input_encryption_key::text)::jsonb
            ELSE job_archive.parameters
        END AS parameters,
        CASE
            WHEN octet_length(job_archive.parameters_keyed_encrypted) > 0 THEN pgp_sym_decrypt(job_archive.parameters_keyed_encrypted, input_encryption_key::text)::jsonb
            ELSE job_archive.parameters_keyed
        END AS parameters_keyed,
        job_archive.status,
        job_archive.scheduled_at,
        job_archive.started_at,
        job_archive.schedule_count,
        job_archive.attempts,
        CASE
            WHEN octet_length(job_archive.results_encrypted) > 0 THEN pgp_sym_decrypt(job_archive.results_encrypted, input_encryption_key::text)::jsonb
            ELSE job_archive.results
        END AS results,
        job_archive.error,
        job_archive.created_at,
        job_archive.updated_at
    FROM job_archive
    WHERE (0 = input_last_id
        OR job_archive.created_at < (
            SELECT d.created_at
            FROM job_archive AS d
            WHERE d.id = input_last_id))
    ORDER BY job_archive.created_at DESC
    LIMIT input_entries;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION select_all_jobs_from_archive_by_search(
    input_encryption_key TEXT,
    input_search TEXT,
    input_last_id INT,
    input_entries INT
)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_worker_id BIGINT,
    output_worker_rid UUID,
    output_options JSONB,
    output_task_name VARCHAR(100),
    output_parameters JSONB,
    output_parameters_keyed JSONB,
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
    SELECT
        job_archive.id,
        job_archive.rid,
        job_archive.worker_id,
        job_archive.worker_rid,
        job_archive.options,
        job_archive.task_name,
        CASE
            WHEN octet_length(job_archive.parameters_encrypted) > 0 THEN pgp_sym_decrypt(job_archive.parameters_encrypted, input_encryption_key::text)::jsonb
            ELSE job_archive.parameters
        END AS parameters,
        CASE
            WHEN octet_length(job_archive.parameters_keyed_encrypted) > 0 THEN pgp_sym_decrypt(job_archive.parameters_keyed_encrypted, input_encryption_key::text)::jsonb
            ELSE job_archive.parameters_keyed
        END AS parameters_keyed,
        job_archive.status,
        job_archive.scheduled_at,
        job_archive.started_at,
        job_archive.schedule_count,
        job_archive.attempts,
        CASE
            WHEN octet_length(job_archive.results_encrypted) > 0 THEN pgp_sym_decrypt(job_archive.results_encrypted, input_encryption_key::text)::jsonb
            ELSE job_archive.results
        END AS results,
        job_archive.error,
        job_archive.created_at,
        job_archive.updated_at
    FROM job_archive
    WHERE (job_archive.rid::text ILIKE '%' || input_search || '%'
            OR job_archive.worker_id::text ILIKE '%' || input_search || '%'
            OR job_archive.task_name ILIKE '%' || input_search || '%'
            OR job_archive.status ILIKE '%' || input_search || '%')
        AND (0 = input_last_id
            OR job_archive.created_at < (
                SELECT u.created_at
                FROM job_archive AS u
                WHERE u.id = input_last_id))
    ORDER BY job_archive.created_at DESC
    LIMIT input_entries;
END;
$$ LANGUAGE plpgsql;
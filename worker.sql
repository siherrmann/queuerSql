CREATE OR REPLACE FUNCTION init_worker()
RETURNS VOID
AS $$
BEGIN
    -- Create the worker table if it doesn't exist
    CREATE TABLE IF NOT EXISTS worker (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        rid UUID UNIQUE DEFAULT gen_random_uuid(),
        name VARCHAR(100) DEFAULT '',
        options JSONB DEFAULT '{}',
        available_tasks VARCHAR[] DEFAULT ARRAY[]::VARCHAR[],
        available_next_interval VARCHAR[] DEFAULT ARRAY[]::VARCHAR[],
        current_concurrency INT DEFAULT 0,
        max_concurrency INT DEFAULT 1,
        status VARCHAR(50) DEFAULT 'READY',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Create indexes for efficient querying
    CREATE INDEX IF NOT EXISTS idx_worker_rid ON worker (rid);
    CREATE INDEX IF NOT EXISTS idx_worker_name ON worker (name);
    CREATE INDEX IF NOT EXISTS idx_worker_status ON worker (status);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION insert_worker(
    input_name VARCHAR(100),
    input_options JSONB,
    input_max_concurrency INT
)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_name VARCHAR(100),
    output_options JSONB,
    output_max_concurrency INT,
    output_status VARCHAR(50),
    output_created_at TIMESTAMP,
    output_updated_at TIMESTAMP
)
AS $$
BEGIN
    RETURN QUERY
    INSERT INTO worker (name, options, max_concurrency)
    VALUES (input_name, input_options, input_max_concurrency)
    RETURNING
        id,
        rid,
        name,
        options,
        max_concurrency,
        status,
        created_at,
        updated_at;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_worker(
    input_name VARCHAR(100),
    input_options JSONB,
    input_available_tasks VARCHAR[],
    input_available_next_interval VARCHAR[],
    input_max_concurrency INT,
    input_status VARCHAR(50),
    input_rid UUID
)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_name VARCHAR(100),
    output_options JSONB,
    output_available_tasks VARCHAR[],
    output_available_next_interval VARCHAR[],
    output_max_concurrency INT,
    output_status VARCHAR(50),
    output_created_at TIMESTAMP,
    output_updated_at TIMESTAMP
)
AS $$
BEGIN
    RETURN QUERY
    UPDATE worker
    SET
        name = input_name,
        options = input_options,
        available_tasks = input_available_tasks,
        available_next_interval = input_available_next_interval,
        max_concurrency = input_max_concurrency,
        status = input_status,
        updated_at = CURRENT_TIMESTAMP
    WHERE
        rid = input_rid
    RETURNING
        id,
        rid,
        name,
        options,
        available_tasks,
        available_next_interval,
        max_concurrency,
        status,
        created_at,
        updated_at;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_stale_workers(
    input_new_status VARCHAR(50),
    input_status1 VARCHAR(50),
    input_status2 VARCHAR(50),
    input_cutoff_time TIMESTAMP
)
RETURNS INT
AS $$
DECLARE
    affected_rows INT;
BEGIN
    UPDATE worker
    SET status = input_new_status
    WHERE (status = input_status1 OR status = input_status2)
      AND updated_at < input_cutoff_time;
    
    GET DIAGNOSTICS affected_rows = ROW_COUNT;
    RETURN affected_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_worker(
    input_rid UUID
)
RETURNS VOID
AS $$
BEGIN
    DELETE FROM worker
    WHERE rid = input_rid;
END;
$$ LANGUAGE plpgsql;
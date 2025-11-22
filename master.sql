CREATE OR REPLACE FUNCTION init_master()
RETURNS VOID
AS $$
BEGIN
    -- Create the master table if it doesn't exist
    CREATE TABLE IF NOT EXISTS master (
        id INTEGER PRIMARY KEY DEFAULT 1,
        worker_id BIGINT DEFAULT 0,
        worker_rid UUID,
        settings JSONB DEFAULT '{}',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Create indexes for efficient querying
    CREATE INDEX IF NOT EXISTS idx_master_worker_id ON master (worker_id);
    CREATE INDEX IF NOT EXISTS idx_master_worker_rid ON master (worker_rid);
    
    -- Insert initial master record if it doesn't exist
    INSERT INTO master DEFAULT VALUES
    ON CONFLICT (id) DO NOTHING;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_master(
    input_worker_id BIGINT,
    input_worker_rid UUID,
    input_settings JSONB,
    input_lock_timeout_minutes INT
)
RETURNS TABLE (
    output_id INTEGER,
    output_worker_id BIGINT,
    output_worker_rid UUID,
    output_settings JSONB,
    output_created_at TIMESTAMP,
    output_updated_at TIMESTAMP
)
AS $$
BEGIN
    RETURN QUERY
    WITH current_master AS (
        SELECT
            m.id,
            m.worker_id,
            m.worker_rid,
            m.settings,
            m.created_at,
            m.updated_at
        FROM master m
        WHERE m.id = 1
        AND (
            m.updated_at < (CURRENT_TIMESTAMP - (input_lock_timeout_minutes * INTERVAL '1 minute'))
            OR m.worker_id = input_worker_id
            OR m.worker_id = 0
        )
        FOR UPDATE SKIP LOCKED
    )
    UPDATE master
    SET
        worker_id = input_worker_id,
        worker_rid = input_worker_rid,
        settings = input_settings,
        updated_at = CURRENT_TIMESTAMP
    FROM current_master
    WHERE master.id = current_master.id
    RETURNING
        current_master.id,
        current_master.worker_id,
        current_master.worker_rid,
        current_master.settings,
        current_master.created_at,
        current_master.updated_at;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION select_master()
RETURNS TABLE (
    output_id INTEGER,
    output_worker_id BIGINT,
    output_worker_rid UUID,
    output_settings JSONB,
    output_created_at TIMESTAMP,
    output_updated_at TIMESTAMP
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        m.id,
        m.worker_id,
        m.worker_rid,
        m.settings,
        m.created_at,
        m.updated_at
    FROM master m
    WHERE m.id = 1;
END;
$$ LANGUAGE plpgsql;

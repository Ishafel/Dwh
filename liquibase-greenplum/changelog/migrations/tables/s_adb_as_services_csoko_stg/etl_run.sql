--liquibase formatted sql

--changeset codex:etl_run splitStatements:false
CREATE TABLE IF NOT EXISTS s_adb_as_services_csoko_stg.etl_run (
    run_id uuid NOT NULL,
    function_name text NOT NULL,
    src_schema text,
    src_table text,
    tgt_schema text,
    tgt_table text,
    do_truncate boolean,
    do_analyze boolean,
    status text NOT NULL,
    rows_inserted bigint,
    error_text text,
    started_at timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    duration_ms bigint,
    extra text,
    src_row_count bigint,
    tgt_row_count bigint
)
DISTRIBUTED BY (run_id);

--liquibase formatted sql

--changeset codex:etl_run runOnChange:true splitStatements:false
DROP TABLE IF EXISTS s_adb_as_services_csoko_stg.etl_run CASCADE;

CREATE TABLE IF NOT EXISTS s_adb_as_services_csoko_stg.etl_run (
    run_id uuid NULL, -- Уникальный идентификатор запуска
    function_name text NULL, -- Имя функции, вызванной для загрузки
    env text NULL, -- Окружение (например, DEV, TEST, PROD)
    src_schema text NULL, -- Исходная схема
    src_table text NULL, -- Исходная таблица
    tgt_schema text NULL, -- Целевая схема
    tgt_table text NULL, -- Целевая таблица
    do_truncate bool DEFAULT false NULL, -- Флаг: нужно ли очищать целевую таблицу перед загрузкой
    do_analyze bool DEFAULT false NULL, -- Флаг: нужно ли выполнять ANALYZE после загрузки
    status text DEFAULT 'RUNNING'::text NULL, -- Статус выполнения (RUNNING, SUCCESS, FAILED)
    rows_inserted int8 NULL, -- Количество вставленных строк
    error_text text NULL, -- Текст ошибки, если произошла
    started_at timestamptz DEFAULT clock_timestamp() NULL, -- Время начала выполнения
    finished_at timestamptz NULL, -- Время завершения выполнения
    duration_ms int8 NULL, -- Длительность выполнения в миллисекундах
    db_name text DEFAULT current_database() NULL, -- Имя базы данных
    username text DEFAULT current_user NULL, -- Имя пользователя, запустившего процесс
    session_pid int4 DEFAULT pg_backend_pid() NULL, -- Идентификатор процесса сессии
    txid int8 DEFAULT txid_current() NULL, -- Идентификатор транзакции
    extra text NULL, -- Дополнительная информация
    src_row_count int8 NULL,
    tgt_row_count int8 NULL
)
WITH (
    appendonly=false
)
DISTRIBUTED BY (run_id);
COMMENT ON TABLE s_adb_as_services_csoko_stg.etl_run IS 'Таблица для хранения информации о запусках процедур загрузки данных в stg';

-- Column comments

COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.run_id IS 'Уникальный идентификатор запуска';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.function_name IS 'Имя функции, вызванной для загрузки';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.env IS 'Окружение (например, DEV, TEST, PROD)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.src_schema IS 'Исходная схема';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.src_table IS 'Исходная таблица';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.tgt_schema IS 'Целевая схема';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.tgt_table IS 'Целевая таблица';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.do_truncate IS 'Флаг: нужно ли очищать целевую таблицу перед загрузкой';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.do_analyze IS 'Флаг: нужно ли выполнять ANALYZE после загрузки';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.status IS 'Статус выполнения (RUNNING, SUCCESS, FAILED)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.rows_inserted IS 'Количество вставленных строк';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.error_text IS 'Текст ошибки, если произошла';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.started_at IS 'Время начала выполнения';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.finished_at IS 'Время завершения выполнения';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.duration_ms IS 'Длительность выполнения в миллисекундах';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.db_name IS 'Имя базы данных';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.username IS 'Имя пользователя, запустившего процесс';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.session_pid IS 'Идентификатор процесса сессии';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.txid IS 'Идентификатор транзакции';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.extra IS 'Дополнительная информация';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.src_row_count IS 'Количество строк в таблице источнике.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.etl_run.tgt_row_count IS 'Количество строк в целевой таблице.';
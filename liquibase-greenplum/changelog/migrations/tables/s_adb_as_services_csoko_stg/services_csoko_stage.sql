--liquibase formatted sql

--changeset codex:services_csoko_stage runOnChange:true splitStatements:false
CREATE TABLE IF NOT EXISTS s_adb_as_services_csoko_stg.services_csoko_stage (
    id bigint,
    stage_name text,
    pxf_name text,
    is_current boolean
)
DISTRIBUTED BY (id);

INSERT INTO s_adb_as_services_csoko_stg.services_csoko_stage (
    id,
    stage_name,
    pxf_name,
    is_current
)
SELECT
    1,
    'dev_csoko',
    'hive',
    TRUE
WHERE NOT EXISTS (
    SELECT 1
    FROM s_adb_as_services_csoko_stg.services_csoko_stage
    WHERE is_current = TRUE
);

COMMENT ON TABLE s_adb_as_services_csoko_stg.services_csoko_stage
IS 'Справочник стендов и PXF-серверов для сервисных функций CSOKO.';

COMMENT ON COLUMN s_adb_as_services_csoko_stg.services_csoko_stage.id
IS 'Технический идентификатор записи.';

COMMENT ON COLUMN s_adb_as_services_csoko_stg.services_csoko_stage.stage_name
IS 'Имя стенда, используемое сервисными функциями.';

COMMENT ON COLUMN s_adb_as_services_csoko_stg.services_csoko_stage.pxf_name
IS 'Имя PXF-сервера Greenplum для текущего стенда.';

COMMENT ON COLUMN s_adb_as_services_csoko_stg.services_csoko_stage.is_current
IS 'Признак текущего стенда.';

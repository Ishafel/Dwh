CREATE SCHEMA IF NOT EXISTS s_adb_as_services_csoko_stg;

CREATE TABLE IF NOT EXISTS s_adb_as_services_csoko_stg.services_csoko_stage (
    id bigint,
    stage_name text,
    pxf_name text,
    is_current boolean
)
DISTRIBUTED BY (id);

CREATE TABLE IF NOT EXISTS s_adb_as_services_csoko_stg.services_csoko_smd_subscription (
    id bigint,
    stage_name text,
    source_table text,
    subscription_name text
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

INSERT INTO s_adb_as_services_csoko_stg.services_csoko_smd_subscription (
    id,
    stage_name,
    source_table,
    subscription_name
)
SELECT
    1,
    'dev_csoko',
    'example_hive_customers',
    'demo'
WHERE NOT EXISTS (
    SELECT 1
    FROM s_adb_as_services_csoko_stg.services_csoko_smd_subscription
    WHERE stage_name = 'dev_csoko'
      AND source_table = 'example_hive_customers'
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

COMMENT ON TABLE s_adb_as_services_csoko_stg.services_csoko_smd_subscription
IS 'Справочник подписок СМД по стендам и исходным таблицам для формирования PXF LOCATION.';

COMMENT ON COLUMN s_adb_as_services_csoko_stg.services_csoko_smd_subscription.id
IS 'Технический идентификатор записи.';

COMMENT ON COLUMN s_adb_as_services_csoko_stg.services_csoko_smd_subscription.stage_name
IS 'Имя стенда подписки.';

COMMENT ON COLUMN s_adb_as_services_csoko_stg.services_csoko_smd_subscription.source_table
IS 'Имя исходной таблицы, полученное во входном JSON функции.';

COMMENT ON COLUMN s_adb_as_services_csoko_stg.services_csoko_smd_subscription.subscription_name
IS 'Имя подписки, используемое в PXF-адресе.';

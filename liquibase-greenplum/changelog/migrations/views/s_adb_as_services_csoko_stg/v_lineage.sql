--liquibase formatted sql

--changeset codex:v_lineage runOnChange:true runAlways:true splitStatements:false
--         Создание и обновление набора представлений для анализа графа зависимостей (lineage) объектов в витринах данных.
--         Включает представления для рёбер графа, корневых объектов, связей "цель-источник", статусов ETL-прогонов
--         и агрегированной оценки состояния конвейеров данных. Представления используются для мониторинга целостности,
--         отладки зависимостей и определения влияния сбоев ETL на конечные витрины.
--         Поддерживается детализация по схемам, таблицам, типам объектов и временным меткам загрузки.
DROP VIEW IF EXISTS s_adb_as_services_csoko_stg.v_lineage_edges_superset CASCADE;
CREATE OR REPLACE VIEW s_adb_as_services_csoko_stg.v_lineage_edges_superset AS
SELECT DISTINCT
    root_schema,                 -- Схема корневого объекта
    root_object_name,              -- Имя корневого объекта
    parent_schema,               -- Схема родительского объекта
    parent_name,                 -- Имя родительского объекта
    object_schema AS child_schema, -- Схема дочернего объекта
    object_name AS child_name   -- Имя дочернего объекта
FROM s_adb_as_services_csoko_stg.view_lineage
WHERE parent_oid IS NOT NULL;

COMMENT ON VIEW s_adb_as_services_csoko_stg.v_lineage_edges_superset IS 'Представление рёбер линий происхождения объектов (зависимости "родитель-ребёнок")';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_edges_superset.root_schema IS 'Схема корневого объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_edges_superset.root_object_name IS 'Имя корневого объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_edges_superset.parent_schema IS 'Схема родительского объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_edges_superset.parent_name IS 'Имя родительского объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_edges_superset.child_schema IS 'Схема дочернего объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_edges_superset.child_name IS 'Имя дочернего объекта';


DROP VIEW IF EXISTS s_adb_as_services_csoko_stg.v_lineage_roots_superset CASCADE;
CREATE OR REPLACE VIEW s_adb_as_services_csoko_stg.v_lineage_roots_superset AS
SELECT
    lineage_loaded_at,
    root_oid,
    root_schema,
    root_object_name,
    root_object_type,
    MAX(lvl) AS max_depth,
    COUNT(DISTINCT object_oid) AS object_cnt,
    COUNT(DISTINCT CASE WHEN object_type IN ('table', 'partitioned table', 'foreign table') THEN object_oid END) AS table_cnt,
    COUNT(DISTINCT CASE WHEN object_type IN ('view', 'materialized view') THEN object_oid END) AS view_cnt,
    COUNT(DISTINCT CASE WHEN v_src_status = 'not found' THEN object_oid END) AS no_vsrc_cnt,
    COUNT(DISTINCT CASE WHEN is_leaf THEN object_oid END) AS leaf_cnt
FROM s_adb_as_services_csoko_stg.view_lineage
GROUP BY
    lineage_loaded_at,
    root_oid,
    root_schema,
    root_object_name,
    root_object_type;

COMMENT ON VIEW s_adb_as_services_csoko_stg.v_lineage_roots_superset IS 'Агрегированное представление корневых объектов в графе зависимостей (lineage). Содержит статистику по глубине, количеству таблиц, представлений и "листьев" для каждого корневого объекта.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_roots_superset.lineage_loaded_at IS 'Время загрузки данных о зависимостях';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_roots_superset.root_oid IS 'Уникальный идентификатор корневого объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_roots_superset.root_schema IS 'Схема корневого объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_roots_superset.root_object_name IS 'Имя корневого объекта (представления или таблицы)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_roots_superset.root_object_type IS 'Тип корневого объекта (например, view, table)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_roots_superset.max_depth IS 'Максимальная глубина графа зависимостей от корня';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_roots_superset.object_cnt IS 'Общее количество уникальных объектов в графе зависимостей';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_roots_superset.table_cnt IS 'Количество таблиц (включая партиционированные и внешние) в графе';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_roots_superset.view_cnt IS 'Количество представлений и материализованных представлений в графе';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_roots_superset.no_vsrc_cnt IS 'Количество объектов, для которых не найден исходный код (v_src_status = ''not found'')';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_roots_superset.leaf_cnt IS 'Количество "листьев" (объектов без потомков) в графе зависимостей';


DROP VIEW IF EXISTS s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset CASCADE;
CREATE OR REPLACE VIEW s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset
AS
SELECT t.lineage_loaded_at,
       t.root_oid,
       t.root_schema,
       t.root_object_name,
       t.root_object_type,
       (t.root_schema || '.'::text) || t.root_object_name AS root_full_name,
       t.object_oid AS tgt_oid,
       t.object_schema AS tgt_schema,
       t.object_name AS tgt_table,
       t.object_type AS tgt_type,
       (t.object_schema || '.'::text) || t.object_name AS tgt_full_name,
       t.v_src_status,
       t.dependency_path AS tgt_dependency_path,
       s.object_oid AS src_oid,
       s.object_schema AS src_schema,
       s.object_name AS src_table,
       s.object_type AS src_type,
       CASE
           WHEN s.object_schema IS NOT NULL AND s.object_name IS NOT NULL THEN (s.object_schema || '.'::text) || s.object_name
           ELSE NULL::text
       END AS src_full_name,
    s.dependency_path AS src_dependency_path,
    CASE
           WHEN s.object_oid IS NOT NULL THEN 'exact_target_and_src'::text
           WHEN t.v_src_status = 'not_applicable_external_table'::text THEN 'external_table'::text
           ELSE 'target_only_no_vsrc'::text
       END AS lineage_match_type
FROM s_adb_as_services_csoko_stg.view_lineage t
     LEFT JOIN s_adb_as_services_csoko_stg.view_lineage s ON s.lineage_loaded_at = t.lineage_loaded_at AND s.root_oid = t.root_oid AND s.parent_oid = t.object_oid AND s.edge_type IN ('matched_v_src'::text, 'matched_f_src'::text)
WHERE t.object_type = ANY (ARRAY['table'::text, 'partitioned table'::text, 'foreign table'::text])
GROUP BY t.lineage_loaded_at, t.root_oid, t.root_schema, t.root_object_name, t.root_object_type, (t.root_schema || '.'::text) || t.root_object_name, t.object_oid, t.object_schema, t.object_name, t.object_type, (t.object_schema || '.'::text) || t.object_name, t.v_src_status, t.dependency_path, s.object_oid, s.object_schema, s.object_name, s.object_type,
         CASE
             WHEN s.object_schema IS NOT NULL AND s.object_name IS NOT NULL THEN (s.object_schema || '.'::text) || s.object_name
             ELSE NULL::text
         END, s.dependency_path,
    CASE
             WHEN s.object_oid IS NOT NULL THEN 'exact_target_and_src'::text
             WHEN t.v_src_status = 'not_applicable_external_table'::text THEN 'external_table'::text
             ELSE 'target_only_no_vsrc'::text
         END;

COMMENT ON VIEW s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset IS 'Представление, объединяющее целевые таблицы и их источники (v_src/f_src) через lineage-зависимости. Определяет тип соответствия: полное совпадение, внешняя таблица или отсутствие источника.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.lineage_loaded_at IS 'Время загрузки текущего среза lineage-зависимостей.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.root_oid IS 'OID корневого объекта (исходной материализованной таблицы).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.root_schema IS 'Схема корневого объекта.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.root_object_name IS 'Имя корневого объекта.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.root_object_type IS 'Тип корневого объекта.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.root_full_name IS 'Полное имя корневого объекта (схема.имя).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.tgt_oid IS 'OID целевой таблицы (наследника).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.tgt_schema IS 'Схема целевой таблицы.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.tgt_table IS 'Имя целевой таблицы.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.tgt_type IS 'Тип целевой таблицы.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.tgt_full_name IS 'Полное имя целевой таблицы (схема.имя).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.v_src_status IS 'Статус соответствующего source-adapter (например, found или not_applicable_external_table).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.tgt_dependency_path IS 'Путь зависимостей от корня до целевой таблицы.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.src_oid IS 'OID связанного source-adapter (v_src/f_src), если найден.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.src_schema IS 'Схема source-adapter.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.src_table IS 'Имя source-adapter.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.src_type IS 'Тип source-adapter: view, materialized view или function.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.src_full_name IS 'Полное имя source-adapter (схема.имя), если существует.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.src_dependency_path IS 'Путь зависимостей от корня до source-adapter.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset.lineage_match_type IS 'Тип соответствия: exact_target_and_src — найден source-adapter, external_table — внешняя таблица, target_only_no_vsrc — source-adapter не найден.';


DROP VIEW IF EXISTS s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset CASCADE;
CREATE OR REPLACE VIEW s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset
AS WITH etl_base AS (
    SELECT e.run_id,
           e.function_name,
           e.env,
           e.src_schema,
           e.src_table,
           e.tgt_schema,
           e.tgt_table,
           e.do_truncate,
           e.do_analyze,
           e.status,
           e.rows_inserted,
           e.error_text,
           e.started_at,
           e.finished_at,
           e.finished_at_eff,
           e.duration_ms_eff,
           e.duration_sec,
           e.error_key,
           e.rows_per_sec,
           e.db_name,
           e.username,
           e.session_pid,
           e.txid,
           e.extra,
           e.is_today_run,
           row_number() OVER (PARTITION BY e.tgt_schema, e.tgt_table, e.src_schema, e.src_table ORDER BY COALESCE(e.finished_at_eff, e.finished_at, e.started_at) DESC, e.started_at DESC) AS rn
    FROM s_adb_as_services_csoko_stg.v_etl_run_superset e),
latest_exact AS (
    SELECT q.run_id,
           q.function_name,
           q.env,
           q.src_schema,
           q.src_table,
           q.tgt_schema,
           q.tgt_table,
           q.do_truncate,
           q.do_analyze,
           q.status,
           q.rows_inserted,
           q.error_text,
           q.started_at,
           q.finished_at,
           q.finished_at_eff,
           q.duration_ms_eff,
           q.duration_sec,
           q.error_key,
           q.rows_per_sec,
           q.db_name,
           q.username,
           q.session_pid,
           q.txid,
           q.extra,
           q.is_today_run,
           q.rn
    FROM etl_base q
    WHERE q.rn = 1)
SELECT DISTINCT m.lineage_loaded_at,
       m.root_schema,
       m.root_object_name,
       m.tgt_schema,
       m.tgt_table,
       m.src_schema,
       m.src_table,
       m.v_src_status,
       m.lineage_match_type,
       ex.run_id,
       ex.status AS etl_status,
       ex.started_at,
       ex.finished_at,
       ex.duration_sec,
       ex.is_today_run,
       CASE
           WHEN ex.run_id IS NOT NULL THEN 'exact_target_and_src'::text
           ELSE 'no_etl_match'::text
       END AS etl_match_type,
    CASE
           WHEN COALESCE(ex.status) = 'ERROR'::text THEN 'impacted'::text
           WHEN COALESCE(ex.status) = 'SUCCESS'::text THEN 'ok'::text
           WHEN m.v_src_status = 'not_applicable_external_table'::text THEN 'not_applicable'::text
           ELSE 'unknown'::text
       END AS mart_impact_status,
    CASE
           WHEN COALESCE(ex.status) = 'ERROR'::text THEN -1
           WHEN COALESCE(ex.status) = 'SUCCESS'::text THEN 1
           WHEN m.v_src_status = 'not_applicable_external_table'::text THEN 0
           ELSE 0
       END AS mart_impact_status_number
FROM s_adb_as_services_csoko_stg.v_lineage_target_source_to_root_superset m
     LEFT JOIN latest_exact ex ON ex.tgt_schema = m.tgt_schema AND ex.tgt_table = m.tgt_table AND ex.src_schema = m.src_schema AND ex.src_table = m.src_table;

COMMENT ON VIEW s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset IS 'Представление объединяет данные линейной зависимости объектов (lineage) с последними статусами ETL-прогонов. Используется для анализа влияния статуса загрузки источников на целевые таблицы. Учитывает точное совпадение по целевой и исходной таблице, а также fallback-совпадение только по целевой таблице.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.lineage_loaded_at IS 'Время актуальности данных линейной зависимости (метка обновления графа зависимостей).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.root_schema IS 'Схема корневого объекта.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.root_object_name IS 'Имя корневого объекта.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.tgt_schema IS 'Схема целевого объекта.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.tgt_table IS 'Имя целевой таблицы/представления.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.src_schema IS 'Схема исходной таблицы.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.src_table IS 'Имя исходной таблицы.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.v_src_status IS 'Статус источника из метаданных (например, not_applicable_external_table).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.lineage_match_type IS 'Тип совпадения в графе зависимостей (например, direct, inferred).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.run_id IS 'Идентификатор запуска ETL-прогонки, связанного с загрузкой.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.etl_status IS 'Оригинальный статус выполнения ETL-прогона.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.started_at IS 'Время начала выполнения ETL-прогона.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.finished_at IS 'Эффективное время завершения прогона (с учётом корректировок).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.duration_sec IS 'Длительность выполнения в секундах.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.is_today_run IS 'Флаг: был ли прогон сегодня.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.etl_match_type IS 'Тип сопоставления ETL с lineage: exact_target_and_src, target_only_fallback, no_etl_match и др.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.mart_impact_status IS 'Статус влияния на витрину: ok, impacted, in_progress, not_applicable, unknown.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset.mart_impact_status_number IS 'Статус влияния на витрину в цифровом виде.';


DROP VIEW IF EXISTS s_adb_as_services_csoko_stg.v_lineage_etl_status_superset CASCADE;
CREATE OR REPLACE VIEW s_adb_as_services_csoko_stg.v_lineage_etl_status_superset
AS SELECT v_lineage_etl_dependency_status_superset.root_schema,
          v_lineage_etl_dependency_status_superset.root_object_name,
          count(*) AS dependency_cnt,
          sum(
              CASE
                  WHEN v_lineage_etl_dependency_status_superset.mart_impact_status = 'impacted'::text THEN 1
                  ELSE 0
              END) AS impacted_dep_cnt,
    sum(
              CASE
                  WHEN v_lineage_etl_dependency_status_superset.mart_impact_status = 'in_progress'::text THEN 1
                  ELSE 0
              END) AS running_dep_cnt,
    sum(
              CASE
                  WHEN v_lineage_etl_dependency_status_superset.mart_impact_status = 'ok'::text THEN 1
                  ELSE 0
              END) AS ok_dep_cnt,
    sum(
              CASE
                  WHEN v_lineage_etl_dependency_status_superset.mart_impact_status = 'unknown'::text THEN 1
                  ELSE 0
              END) AS unknown_dep_cnt,
    sum(
              CASE
                  WHEN v_lineage_etl_dependency_status_superset.etl_match_type = 'target_only_no_vsrc'::text THEN 1
                  ELSE 0
              END) AS no_vsrc_dep_cnt,
    sum(
              CASE
                  WHEN v_lineage_etl_dependency_status_superset.etl_match_type = 'target_only_fallback'::text THEN 1
                  ELSE 0
              END) AS fallback_dep_cnt,
    sum(
              CASE
                  WHEN v_lineage_etl_dependency_status_superset.etl_match_type = 'no_etl_match'::text THEN 1
                  ELSE 0
              END) AS no_etl_match_cnt,
    max(v_lineage_etl_dependency_status_superset.finished_at) AS last_event_at,
    max(
              CASE
                  WHEN v_lineage_etl_dependency_status_superset.mart_impact_status = 'impacted'::text THEN v_lineage_etl_dependency_status_superset.finished_at
                  ELSE NULL::timestamp with time zone
              END) AS last_failed_at,
    CASE
              WHEN sum(
                              CASE
                                      WHEN v_lineage_etl_dependency_status_superset.mart_impact_status = 'impacted'::text THEN 1
                                  ELSE 0
                              END) > 0 THEN 'red'::text
              WHEN sum(
                              CASE
                                      WHEN v_lineage_etl_dependency_status_superset.mart_impact_status = 'in_progress'::text THEN 1
                                  ELSE 0
                              END) > 0 THEN 'yellow'::text
              WHEN sum(
                              CASE
                                      WHEN v_lineage_etl_dependency_status_superset.mart_impact_status = 'unknown'::text THEN 1
                                  ELSE 0
                              END) > 0 THEN 'gray'::text
              ELSE 'green'::text
          END AS mart_status,
    CASE
              WHEN sum(
                              CASE
                                      WHEN v_lineage_etl_dependency_status_superset.mart_impact_status = 'impacted'::text THEN 1
                                  ELSE 0
                              END) > 0 THEN 1
              WHEN sum(
                              CASE
                                      WHEN v_lineage_etl_dependency_status_superset.mart_impact_status = 'in_progress'::text THEN 1
                                  ELSE 0
                              END) > 0 THEN 2
              WHEN sum(
                              CASE
                                      WHEN v_lineage_etl_dependency_status_superset.mart_impact_status = 'unknown'::text THEN 1
                                  ELSE 0
                              END) > 0 THEN 3
              ELSE 4
          END AS mart_status_rank
FROM s_adb_as_services_csoko_stg.v_lineage_etl_dependency_status_superset
GROUP BY v_lineage_etl_dependency_status_superset.lineage_loaded_at,  v_lineage_etl_dependency_status_superset.root_schema, v_lineage_etl_dependency_status_superset.root_object_name;

COMMENT ON VIEW s_adb_as_services_csoko_stg.v_lineage_etl_status_superset IS 'Агрегированное состояние ETL-зависимостей для корневых объектов: суммарные счётчики статусов дочерних объектов, время последних событий и итоговый статус (red/yellow/green) на основе анализа v_lineage_etl_dependency_status_superset.';

COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_status_superset.root_schema IS 'Схема корневого объекта, от которого строится анализ зависимостей';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_status_superset.root_object_name IS 'Имя корневого объекта, для которого рассчитывается статус ETL';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_status_superset.dependency_cnt IS 'Общее количество зависимостей (дочерних объектов), связанных с корневым объектом';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_status_superset.impacted_dep_cnt IS 'Количество зависимостей со статусом "impacted" — ошибка или сбой в обработке';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_status_superset.running_dep_cnt IS 'Количество зависимостей со статусом "in_progress" — в процессе выполнения';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_status_superset.ok_dep_cnt IS 'Количество зависимостей со статусом "ok" — успешно завершённых';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_status_superset.unknown_dep_cnt IS 'Количество зависимостей со статусом "unknown" — статус не определён';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_status_superset.no_vsrc_dep_cnt IS 'Количество зависимостей типа "target_only_no_vsrc" — отсутствует v_src, только целевой объект';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_status_superset.fallback_dep_cnt IS 'Количество зависимостей типа "target_only_fallback" — используется fallback-логика загрузки';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_status_superset.no_etl_match_cnt IS 'Количество зависимостей без соответствия в ETL (no_etl_match)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_status_superset.last_event_at IS 'Время последнего события (завершения) среди всех зависимостей';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_status_superset.last_failed_at IS 'Время последней ошибки (последнего failed-события) среди зависимостей';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_status_superset.mart_status IS 'Агрегированный статус состояния ETL: red (ошибки), yellow (), gray (неизвестно), green (всё ок)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_lineage_etl_status_superset.mart_status_rank IS 'Целочисленный ранг статуса для сортировки: 1=red (ошибки), 2=yellow (), 3=gray (неизвестно), 4=green (всё ок)';

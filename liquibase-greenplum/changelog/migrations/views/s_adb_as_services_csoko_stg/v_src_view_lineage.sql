--liquibase formatted sql

--changeset codex:v_src_view_lineage runOnChange:true runAlways:true splitStatements:false
DROP VIEW IF EXISTS s_adb_as_services_csoko_stg.v_src_view_lineage CASCADE;

CREATE OR REPLACE VIEW s_adb_as_services_csoko_stg.v_src_view_lineage AS
-- === ШАГ 1: Определение целевых схем ===
-- Выбираем все схемы
-- Эти схемы будут использоваться как отправная точка для анализа зависимостей

WITH RECURSIVE scope_schemas AS (
    SELECT schema_name::TEXT
    FROM (VALUES
        ('s_adb_as_services_csoko_dds'),
        ('s_adb_as_services_csoko_df'),
        ('s_adb_as_services_csoko_dm'),
        ('s_adb_as_services_csoko_navigator'),
        ('s_adb_as_services_csoko_ods'),
        ('s_adb_as_services_csoko_stg'),
        ('s_adb_as_services_csoko_udlapprove'),
        ('s_adb_as_services_csoko_udlprod'),
        ('s_adb_as_services_csoko_view')
    ) AS t(schema_name)
),
-- === ШАГ 2: Поиск корневых объектов ===
-- Находим объекты в активных схемах
-- Они становятся корнями графа зависимостей
start_views AS (
    SELECT
        c.oid AS root_oid,
        n.nspname AS root_schema,
        c.relname AS root_object_name,
        c.relkind AS root_kind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN scope_schemas s ON s.schema_name = n.nspname
    WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
      -- Source-adapters are dependencies of target tables, not independent roots.
      AND c.relname NOT LIKE 'v_src_%'
      -- Keep the lineage graph focused on business objects, not on the monitoring contour itself.
      AND NOT (
          n.nspname = 's_adb_as_services_csoko_stg'
          AND (
              c.relname IN ('etl_run', 'view_lineage', 'v_src_view_lineage')
              OR c.relname LIKE 'v_lineage_%'
              OR c.relname LIKE 'v_etl_run_%'
          )
      )
),
-- === ШАГ 3: Получение реальных зависимостей из системного каталога ===
-- Используем метаданные PostgreSQL (`pg_depend`, `pg_rewrite`) для построения графа зависимостей
-- Зависимости возникают, когда одно представление ссылается на другое или на таблицу
catalog_edges AS (
    SELECT DISTINCT
        parent.oid AS parent_oid,
        parent_ns.nspname AS parent_schema,
        parent.relname AS parent_name,
        parent.relkind AS parent_kind,
        child.oid AS child_oid,
        child_ns.nspname AS child_schema,
        child.relname AS child_name,
        child.relkind AS child_kind,
        'catalog_dependency'::TEXT AS edge_type
    FROM pg_class parent
    JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
    JOIN pg_rewrite rw ON rw.ev_class = parent.oid
    JOIN pg_depend d ON d.classid = 'pg_rewrite'::REGCLASS
        AND d.objid = rw.oid
        AND d.refclassid = 'pg_class'::REGCLASS
    JOIN pg_class child ON child.oid = d.refobjid
    JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
    WHERE parent.relkind IN ('v', 'm')
      AND child.relkind IN ('r', 'p', 'v', 'm', 'f')
      AND child.oid <> parent.oid
),
-- === ШАГ 4: Синтетические зависимости: таблицы → v_src_<table_name> / f_src_<table_name> ===
-- Допущение: если существует таблица `sales`, то источником может быть `v_src_sales` или `f_src_sales`
-- Такие зависимости не отражаются в `pg_depend`, поэтому добавляем их искусственно
-- Это помогает выявить источники данных, которые могут быть скрыты за ETL-слоем
source_edges AS (
    SELECT
        t.oid AS parent_oid,
        t_ns.nspname AS parent_schema,
        t.relname AS parent_name,
        t.relkind AS parent_kind,
        v.oid AS child_oid,
        v_ns.nspname AS child_schema,
        v.relname AS child_name,
        v.relkind AS child_kind,
        'matched_v_src'::TEXT AS edge_type
    FROM pg_class t
    JOIN pg_namespace t_ns ON t_ns.oid = t.relnamespace
    JOIN pg_class v ON v.relname = 'v_src_' || t.relname
        AND v.relkind IN ('v', 'm')
    JOIN pg_namespace v_ns ON v_ns.oid = v.relnamespace
        AND v_ns.oid = t.relnamespace
    WHERE t.relkind IN ('r', 'p', 'f')
    UNION ALL
    SELECT
        t.oid AS parent_oid,
        t_ns.nspname AS parent_schema,
        t.relname AS parent_name,
        t.relkind AS parent_kind,
        p.oid AS child_oid,
        p_ns.nspname AS child_schema,
        p.proname AS child_name,
        'F'::"char" AS child_kind,
        'matched_f_src'::TEXT AS edge_type
    FROM pg_class t
    JOIN pg_namespace t_ns ON t_ns.oid = t.relnamespace
    JOIN pg_proc p ON p.proname = 'f_src_' || t.relname
    JOIN pg_namespace p_ns ON p_ns.oid = p.pronamespace
        AND p_ns.oid = t.relnamespace
    WHERE t.relkind IN ('r', 'p', 'f')
),
-- === ШАГ 5: Объединение всех типов зависимостей ===
-- Собираем как реальные (`catalog_edges`), так и искусственные (`vsrc_edges`) зависимости
all_edges AS (
    SELECT * FROM catalog_edges
    UNION ALL
    SELECT * FROM source_edges
),
-- === ШАГ 6: Рекурсивный обход графа зависимостей ===
-- Начинаем с корневых view/mview и спускаемся вниз по цепочке зависимостей
walk AS (
    -- Корень
    SELECT
        sv.root_oid,
        sv.root_schema,
        sv.root_object_name,
        CASE sv.root_kind
            WHEN 'v' THEN 'view'
            WHEN 'm' THEN 'materialized view'
            WHEN 'r' THEN 'table'
            WHEN 'p' THEN 'partitioned table'
            WHEN 'f' THEN 'foreign table'
            ELSE sv.root_kind::TEXT
        END AS root_object_type,
        sv.root_oid AS object_oid,
        sv.root_schema AS object_schema,
        sv.root_object_name AS object_name,
        sv.root_kind AS object_kind,
        NULL::OID AS parent_oid,
        NULL::TEXT AS parent_schema,
        NULL::TEXT AS parent_name,
        NULL::TEXT AS parent_type,
        NULL::TEXT AS edge_type,
        0 AS lvl,
        ARRAY[sv.root_oid]::OID[] AS path_oids,
        sv.root_schema || '.' || sv.root_object_name AS dependency_path
    FROM start_views sv
    UNION ALL
    -- Рекурсивный случай: переход к дочерним объектам
    SELECT
        w.root_oid,
        w.root_schema,
        w.root_object_name,
        w.root_object_type,
        e.child_oid AS object_oid,
        e.child_schema AS object_schema,
        e.child_name AS object_name,
        e.child_kind AS object_kind,
        e.parent_oid,
        e.parent_schema,
        e.parent_name,
        CASE e.parent_kind
            WHEN 'v' THEN 'view'
            WHEN 'm' THEN 'materialized view'
            WHEN 'r' THEN 'table'
            WHEN 'p' THEN 'partitioned table'
            WHEN 'f' THEN 'foreign table'
            WHEN 'F' THEN 'function'
            ELSE e.parent_kind::TEXT
        END AS parent_type,
        e.edge_type,
        w.lvl + 1 AS lvl,
        w.path_oids || e.child_oid,
        w.dependency_path ||
            CASE
                WHEN e.edge_type = 'matched_v_src' THEN ' =>v_src=> '
                WHEN e.edge_type = 'matched_f_src' THEN ' =>f_src=> '
                ELSE ' -> '
            END ||
            e.child_schema || '.' || e.child_name AS dependency_path
    FROM walk w
    JOIN all_edges e ON e.parent_oid = w.object_oid
    WHERE NOT e.child_oid = ANY (w.path_oids)
),
-- === ШАГ 7: Обогащение результатов дополнительной информацией ===
-- Добавляем флаги: является ли объект листом, внешней таблицей, есть ли у него v_src/f_src
walk_enriched AS (
    SELECT
        w.*,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM all_edges ae
                WHERE ae.parent_oid = w.object_oid
            ) THEN FALSE
            ELSE TRUE
        END AS is_leaf,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM pg_exttable ext
                WHERE ext.reloid = w.object_oid
            ) THEN TRUE
            ELSE FALSE
        END AS is_external_table,
        CASE
            WHEN w.object_kind IN ('r', 'p', 'f') THEN
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM pg_exttable ext
                        WHERE ext.reloid = w.object_oid
                    ) THEN 'not_applicable_external_table'
                    WHEN w.object_kind = 'f' THEN 'not_applicable_foreign_table'
                    WHEN EXISTS (
                        SELECT 1
                        FROM source_edges ve
                        WHERE ve.parent_oid = w.object_oid
                    ) THEN 'found'
                    ELSE 'not found'
                END
            ELSE NULL
        END AS v_src_status
    FROM walk w
)
-- === ФИНАЛЬНЫЙ ВЫВОД ===
-- Возвращаем полный граф зависимостей с метаданными
SELECT
    CURRENT_TIMESTAMP AS lineage_loaded_at,
    we.root_oid,
    we.root_schema,
    we.root_object_name,
    we.root_object_type,
    we.lvl,
    we.parent_oid,
    we.parent_schema,
    we.parent_name,
    we.parent_type,
    we.object_oid,
    we.object_schema,
    we.object_name,
    CASE we.object_kind
        WHEN 'v' THEN 'view'
        WHEN 'm' THEN 'materialized view'
        WHEN 'r' THEN 'table'
        WHEN 'p' THEN 'partitioned table'
        WHEN 'f' THEN 'foreign table'
        WHEN 'F' THEN 'function'
        ELSE we.object_kind::TEXT
    END AS object_type,
    we.edge_type,
    we.is_leaf,
    we.v_src_status,
    we.dependency_path
FROM walk_enriched we;


COMMENT ON VIEW s_adb_as_services_csoko_stg.v_src_view_lineage IS
'Представление для анализа графа зависимостей между объектами (представлениями, таблицами и т.д.) в рамках указанных схем.
 Позволяет отслеживать lineage данных: от конечных витрин до исходных таблиц, включая промежуточные слои.
 Поддерживает рекурсивный обход зависимостей, включая как реальные (из pg_depend), так и гипотетические (v_src_* / f_src_*) связи.';

COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.lineage_loaded_at IS 'Время формирования строки (момент запуска запроса)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.root_oid IS 'OID корневого объекта (начальной точки анализа)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.root_schema IS 'Схема корневого объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.root_object_name IS 'Имя корневого объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.root_object_type IS 'Тип корневого объекта: table, view, materialized view и др.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.lvl IS 'Уровень вложенности в графе зависимостей (0 — корень)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.parent_oid IS 'OID родительского объекта в цепочке зависимостей';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.parent_schema IS 'Схема родительского объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.parent_name IS 'Имя родительского объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.parent_type IS 'Тип родительского объекта: table, view, materialized view и др.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.object_oid IS 'OID текущего объекта в цепочке';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.object_schema IS 'Схема текущего объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.object_name IS 'Имя текущего объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.object_type IS 'Тип текущего объекта: table, view, materialized view и др.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.edge_type IS 'Тип зависимости: catalog_dependency (реальная), matched_v_src или matched_f_src (гипотетическая)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.is_leaf IS 'Флаг: является ли объект листом (не имеет потомков)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.v_src_status IS 'Статус наличия source-adapter для таблиц: found, not found, not_applicable_external_table и др. Source-adapter может быть v_src_* или f_src_*.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_src_view_lineage.dependency_path IS 'Текстовый путь зависимости от корня до текущего объекта, отражающий всю цепочку';

-- Храним один актуальный срез lineage: после пересоздания сборочной view сразу
-- обновляем физическую таблицу, которую читают Superset-витрины.
TRUNCATE TABLE s_adb_as_services_csoko_stg.view_lineage;

INSERT INTO s_adb_as_services_csoko_stg.view_lineage (
    lineage_loaded_at,
    root_oid,
    root_schema,
    root_object_name,
    root_object_type,
    lvl,
    parent_oid,
    parent_schema,
    parent_name,
    parent_type,
    object_oid,
    object_schema,
    object_name,
    object_type,
    edge_type,
    is_leaf,
    v_src_status,
    dependency_path
)
SELECT
    lineage_loaded_at,
    root_oid,
    root_schema,
    root_object_name,
    root_object_type,
    lvl,
    parent_oid,
    parent_schema,
    parent_name,
    parent_type,
    object_oid,
    object_schema,
    object_name,
    object_type,
    edge_type,
    is_leaf,
    v_src_status,
    dependency_path
FROM s_adb_as_services_csoko_stg.v_src_view_lineage;

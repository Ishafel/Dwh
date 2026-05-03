--liquibase formatted sql

--changeset codex:view_lineage runOnChange:true splitStatements:false
DROP TABLE IF EXISTS  s_adb_as_services_csoko_stg.view_lineage CASCADE;

CREATE TABLE IF NOT EXISTS s_adb_as_services_csoko_stg.view_lineage (
    lineage_loaded_at timestamptz NOT NULL, -- Время загрузки записи о зависимостях
    root_oid oid NOT NULL,                -- OID корневого объекта (исходной материализованной таблицы/представления)
    root_schema text NOT NULL,            -- Схема корневого объекта
    root_object_name text NOT NULL,         -- Имя корневого объекта
    root_object_type text NOT NULL,         -- Тип корневого объекта (например, 'materialized view', 'view', 'table')
    lvl integer NOT NULL,                 -- Уровень вложенности в графе зависимостей (глубина от корня)
    parent_oid oid,                       -- OID родительского объекта (NULL для корня)
    parent_schema text,                   -- Схема родительского объекта
    parent_name text,                     -- Имя родительского объекта
    parent_type text,                     -- Тип родительского объекта
    object_oid oid NOT NULL,              -- OID текущего объекта в цепочке зависимостей
    object_schema text NOT NULL,          -- Схема текущего объекта
    object_name text NOT NULL,            -- Имя текущего объекта
    object_type text NOT NULL,            -- Тип текущего объекта (view, table и т.д.)
    edge_type text,                       -- Тип зависимости (например, 'depends_on')
    is_leaf boolean NOT NULL,             -- Признак листового узла (нет дочерних зависимостей)
    v_src_status text,                    -- Статус источника данных (опционально, например, 'active', 'deprecated')
    dependency_path text NOT NULL        -- Путь зависимостей в виде строки (например, 'schema1.view1->schema2.view2')
)
DISTRIBUTED BY (root_oid);

COMMENT ON TABLE s_adb_as_services_csoko_stg.view_lineage IS 'Таблица хранит граф зависимостей представлений и таблиц в базе данных, включая информацию о вложенности, путях зависимостей и свойствах объектов. Используется для анализа lineage данных, отслеживания происхождения и влияния изменений в объектах.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.lineage_loaded_at IS 'Время загрузки записи о зависимостях';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.root_oid IS 'OID корневого объекта (исходной материализованной таблицы/представления)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.root_schema IS 'Схема корневого объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.root_object_name IS 'Имя корневого объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.root_object_type IS 'Тип корневого объекта (например, ''materialized view'', ''view'', ''table'')';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.lvl IS 'Уровень вложенности в графе зависимостей (глубина от корня)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.parent_oid IS 'OID родительского объекта (NULL для корня)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.parent_schema IS 'Схема родительского объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.parent_name IS 'Имя родительского объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.parent_type IS 'Тип родительского объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.object_oid IS 'OID текущего объекта в цепочке зависимостей';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.object_schema IS 'Схема текущего объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.object_name IS 'Имя текущего объекта';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.object_type IS 'Тип текущего объекта (view, table и т.д.)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.edge_type IS 'Тип зависимости (например, ''depends_on'')';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.is_leaf IS 'Признак листового узла (нет дочерних зависимостей)';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.v_src_status IS 'Статус источника данных (опционально, например, ''active'', ''deprecated'')';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.view_lineage.dependency_path IS 'Путь зависимостей в виде строки (например, ''schema1.view1->schema2.view2'')';

-- DROP FUNCTION s_adb_as_services_csoko_stg.f_create_external_table(jsonb);

CREATE OR REPLACE FUNCTION s_adb_as_services_csoko_stg.f_create_external_table(v_json jsonb)
    RETURNS bool
    LANGUAGE plpgsql
    VOLATILE
AS $$
/*
 * Техническая часть по формированию внешних таблиц
 * в зависимости от контекста стенда.
 */
DECLARE
    v_schema_name text;         --Имя схемы
    v_table_name text;          --Имя таблицы
    v_columns jsonb;            --Массив описаний колонок
    v_column_description text;  --Описание колонки
    v_column record;            --Рекорд запись из json
    v_column_defs text;         --Сгенерированная строка для определения колонок в таблице
    v_column_name text;         --Имя колонки
    v_column_type text;         --Тип колонки
    v_pxf_location text;        --Сгенерированная строка пхф
    v_pxf_connect text;         --Имя пхф коннектора
    v_subscription_name text;   --Имя подписки в СМД
    v_source_table text;        --Имя источника
    v_profile text;             --Тип источника данных
    v_format text;              --Уточняет формат данных внутри форматтера
    v_formatter text;           --Формат данных, который бдует использоваться для преобразования исходных данных в строки SQL
    v_encoding text;            --Задает кодировку символов
    v_stage_name text;          --Имя стенда
BEGIN
    /*
     * Проверка входной переменной
     */
    IF v_json IS NULL THEN
        RAISE EXCEPTION 'Входная переменная JSONB не может быть пустой.';
    END IF;

    /*
     * Константы
     */
    v_profile := 'HIVE';
    v_format := 'CUSTOM';
    v_formatter := 'pxfwritable_import';
    v_encoding := 'UTF8';

    /*
     * Заполняем название таблицы и источник
     */
    v_schema_name := v_json->>'schema_name';
    v_table_name := v_json->>'table_name';
    v_source_table := v_json->>'source_table';
    v_columns := v_json->'columns';

    /*
     * Проверка для общих значений таблицы
     */
    IF v_schema_name IS NULL OR TRIM(v_schema_name) = '' THEN
        RAISE EXCEPTION 'Имя схемы не может быть пустым.';
    END IF;

    IF v_table_name IS NULL OR TRIM(v_table_name) = '' THEN
        RAISE EXCEPTION 'Имя таблицы не может быть пустым.';
    END IF;

    IF v_source_table IS NULL OR TRIM(v_source_table) = '' THEN
        RAISE EXCEPTION 'Имя исходной таблицы не может быть пустой.';
    END IF;

    IF v_columns IS NULL THEN
        RAISE EXCEPTION 'Не найдено определение колонок для таблицы "%".', v_table_name;
    END IF;

    IF jsonb_typeof(v_columns) <> 'array' THEN
        RAISE EXCEPTION 'Поле "columns" для таблицы "%" должно быть массивом.', v_table_name;
    END IF;

    IF jsonb_array_length(v_columns) = 0 THEN
        RAISE EXCEPTION 'Не найдено определение колонок для таблицы "%s".', v_table_name;
    END IF;

    /*
     * Устанавливаем схему по умолчанию
     */
    EXECUTE FORMAT('set schema %L', v_schema_name);

    /*
     * Полученаем название подписки
     */
    SELECT subscription_name INTO v_subscription_name
    FROM s_adb_as_services_csoko_stg.services_csoko_smd_subscription
    WHERE (
        SELECT stage_name
        FROM s_adb_as_services_csoko_stg.services_csoko_stage
        WHERE is_current = TRUE
    ) = stage_name
      AND source_table = v_source_table
    ORDER BY id DESC
    LIMIT 1;

    SELECT stage_name INTO v_stage_name
    FROM s_adb_as_services_csoko_stg.services_csoko_stage
    WHERE is_current = TRUE;

    IF v_subscription_name IS NULL AND v_stage_name = 'dev_csoko' THEN
        v_subscription_name := 'DEV_CSOKO_SUBSCRIPTION';
    END IF;

    IF v_stage_name = 'psi_csoko' THEN
        v_subscription_name := 'PSI_CSOKO_SUBSCRIPTION';
    END IF;

    IF v_subscription_name IS NULL THEN
        RAISE EXCEPTION 'Не найдена подписка для источника данных "%".', v_source_table;
    END IF;

    SELECT pxf_name INTO v_pxf_connect
    FROM s_adb_as_services_csoko_stg.services_csoko_stage
    WHERE is_current = TRUE;

    IF v_pxf_connect IS NULL THEN
        RAISE EXCEPTION 'Не найден PXF-коннектор для текущего стенда.';
    END IF;

    /*
     * Формирование строки pxf
     */
    v_pxf_location := 'pxf://prx_' || v_subscription_name || '_' || v_source_table || '?profile=' || v_profile || '&server=' || v_pxf_connect;

    /*
     * Проверка корректности заполнения колонок
     */
    FOR v_column IN SELECT * FROM jsonb_array_elements(v_columns)
    LOOP
        v_column_name := v_column.value::jsonb->>'name';
        v_column_type := v_column.value::jsonb->>'type';
        v_column_description := v_column.value::jsonb->>'description';

        /*
         * Проверка для частных значений таблицы
         */
        IF v_column_name IS NULL OR TRIM(v_column_name) = '' THEN
            RAISE EXCEPTION 'Название колонки для таблицы "%s" не может быть пустым.', v_table_name;
        END IF;

        IF v_column_type IS NULL OR TRIM(v_column_type) = '' THEN
            RAISE EXCEPTION 'Тип колонки "%" не может быть пустым.', v_column_name;
        END IF;

        -- Разрешаем только простой набор SQL-типов и параметров, чтобы type
        -- не мог попасть в динамический DDL как произвольный SQL-фрагмент.
        IF v_column_type !~* '^[a-z][a-z0-9_ ]*(\([0-9]+(,[0-9]+)?\))?(\[\])?$' THEN
            RAISE EXCEPTION 'Недопустимый тип колонки "%" для колонки "%".', v_column_type, v_column_name;
        END IF;

        IF v_column_description IS NULL OR TRIM(v_column_description) = '' THEN
            RAISE EXCEPTION 'Комментарий к колонке "%" не может быть пустым.', v_column_name;
        END IF;
    END LOOP;

    /*
     * Формируем строку для определения колонок
     */
    SELECT string_agg(FORMAT('%I %s', c->>'name', c->>'type'), ', ')
    INTO v_column_defs
    FROM jsonb_array_elements(v_columns) AS c;

    /*
     * Выполняем команду создания внешней таблицы
     */
    EXECUTE FORMAT(
        'CREATE EXTERNAL' || ' TABLE %I.%I (%s) LOCATION (%L) ON ALL FORMAT %L (FORMATTER=%L) ENCODING %L;',
        v_schema_name,
        v_table_name,
        v_column_defs,
        v_pxf_location,
        v_format,
        v_formatter,
        v_encoding
    );

    /*
     * Проходимся по каждой колонке и создаем комментарии
     */
    FOR v_column IN SELECT * FROM jsonb_array_elements(v_columns)
    LOOP
        v_column_name := v_column.value::jsonb->>'name';
        v_column_type := v_column.value::jsonb->>'type';
        v_column_description := v_column.value::jsonb->>'description';

        EXECUTE FORMAT(
            'COMMENT ON COLUMN %I.%I.%I IS %L;',
            v_schema_name,
            v_table_name,
            v_column_name,
            v_column_description
        );
    END LOOP;

    RETURN TRUE;
END;
$$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_adb_as_services_csoko_stg.f_create_external_table(jsonb)
IS 'Сервисная функция создает таблицу и комментарии к ней на  основе структуры, заданной в JSON.';

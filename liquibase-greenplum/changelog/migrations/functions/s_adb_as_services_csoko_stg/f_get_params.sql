--liquibase formatted sql

--changeset codex:f_get_params runOnChange:true splitStatements:false
CREATE OR REPLACE FUNCTION s_adb_as_services_csoko_stg.f_get_params(p_schema_src text, p_table_src text)
    RETURNS TABLE (params text, is_func bool)
    LANGUAGE plpgsql
    VOLATILE
AS $$
DECLARE
    v_input_schema text;
    v_input_name text;
    v_source_name text;
    v_signature text;
    v_proretset bool;
    v_return_schema text;
    v_return_name text;
    v_proargmodes text[];
    v_proargnames text[];
    v_cnt int;
    val text;
    i int;
BEGIN
    /*
     * Функция возвращает список колонок через запятую с безопасным quoting.
     * Источником может быть таблица, представление, external table или
     * set-returning function. Флаг is_func показывает, что источник найден как функция.
     */
    is_func := false;
    v_input_schema := btrim(p_schema_src);
    v_source_name := btrim(p_table_src);

    IF v_input_schema IS NULL OR v_input_schema = '' THEN
        RAISE EXCEPTION 'Source schema is not specified.';
    END IF;

    IF v_source_name IS NULL OR v_source_name = '' THEN
        RAISE EXCEPTION 'Source object is not specified.';
    END IF;

    /*
     * Сохраняем исходное имя объекта для диагностики, но для поиска убираем
     * сигнатуру. Перегруженные функции пока отклоняем явно, пока не появится
     * точный разбор типов аргументов через pg_proc.
     */
    v_signature := substring(v_source_name FROM '\((.*)\)');
    v_source_name := regexp_replace(v_source_name, '\(.*\)', '');
    v_input_name := v_source_name;

    IF v_signature IS NOT NULL AND btrim(v_signature) <> '' THEN
        RAISE EXCEPTION 'Function signatures are not supported by f_get_params yet: %.%', v_input_schema, p_table_src;
    END IF;

    /*
     * Читаем pg_catalog напрямую вместо парсинга format_type(...). Так схема
     * и имя возвращаемого composite-типа остаются стабильными для SETOF.
     */
    SELECT
        p.proretset,
        rtn.nspname,
        rt.typname,
        p.proargmodes,
        p.proargnames,
        count(*) OVER (PARTITION BY pn.nspname, p.proname) AS cnt
      INTO v_proretset,
           v_return_schema,
           v_return_name,
           v_proargmodes,
           v_proargnames,
           v_cnt
      FROM pg_catalog.pg_proc p
      JOIN pg_catalog.pg_namespace pn
        ON p.pronamespace = pn.oid
      JOIN pg_catalog.pg_type rt
        ON rt.oid = p.prorettype
      JOIN pg_catalog.pg_namespace rtn
        ON rtn.oid = rt.typnamespace
     WHERE pn.nspname = v_input_schema
       AND p.proname = v_source_name;

    IF v_cnt IS NOT NULL THEN
        is_func := true;
    END IF;

    IF v_cnt > 1 THEN
        RAISE EXCEPTION 'More than one function named %.% exists. Pass a unique function name or remove overloads.', v_input_schema, v_source_name;
    END IF;

    IF v_proretset THEN
        IF v_return_name = 'record' THEN
            /*
             * RETURNS TABLE(...) и OUT/INOUT-параметры хранятся как record
             * с именованными выходными аргументами. Эти имена и есть колонки источника.
             */
            IF v_proargmodes IS NULL OR v_proargnames IS NULL THEN
                RAISE EXCEPTION 'Function %.% returns record without named OUT columns.', v_input_schema, v_source_name;
            END IF;

            i := 1;
            FOREACH val IN ARRAY v_proargmodes
            LOOP
                IF val IN ('o', 'b', 't') THEN
                    IF v_proargnames[i] IS NULL OR btrim(v_proargnames[i]) = '' THEN
                        RAISE EXCEPTION 'Function %.% has an unnamed output column.', v_input_schema, v_source_name;
                    END IF;
                    params := concat_ws(', ', params, quote_ident(v_proargnames[i]));
                END IF;
                i := i + 1;
            END LOOP;

            IF params IS NULL THEN
                RAISE EXCEPTION 'Function %.% returns record without output columns.', v_input_schema, v_source_name;
            END IF;

            RETURN NEXT;
            RETURN;
        ELSE
            /*
             * SETOF some_composite_type: находим возвращаемую relation через
             * метаданные pg_type, затем читаем ее колонки как у обычной таблицы/view.
             */
            p_schema_src := v_return_schema;
            v_source_name := v_return_name;
        END IF;
    ELSIF is_func THEN
        RAISE EXCEPTION 'Function %.% does not return a set.', v_input_schema, v_source_name;
    END IF;

    /*
     * Путь для relation: таблица, view, external table или composite relation,
     * найденная по возвращаемому типу SETOF-функции.
     */
    SELECT string_agg(quote_ident(s.column_name), ', ' ORDER BY s.ordinal_position)
      INTO params
      FROM information_schema.columns s
     WHERE s.table_schema = p_schema_src
       AND s.table_name = v_source_name;

    IF params IS NULL THEN
        IF is_func THEN
            RAISE EXCEPTION 'Function %.% returns a set of %.%, but that relation was not found or has no columns.', v_input_schema, v_input_name, p_schema_src, v_source_name;
        ELSE
            RAISE EXCEPTION 'Object %.% not found or has no columns.', p_schema_src, v_source_name;
        END IF;
    END IF;

    RETURN NEXT;
END;
$$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_adb_as_services_csoko_stg.f_get_params(text, text) IS 'Функция получения исходящих атрибутов из таблицы, представления или функции';

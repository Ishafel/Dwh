--liquibase formatted sql

--changeset codex:f_refresh_table_v2_1 runOnChange:true splitStatements:false stripComments:false
CREATE OR REPLACE FUNCTION s_adb_as_services_csoko_stg.f_refresh_table_v2_1(p_schema_target text, p_table_target text, p_schema_src text, p_table_src text, p_truncate bool DEFAULT true, p_do_analyze bool DEFAULT true, p_compare_row_count bool DEFAULT false)
    RETURNS int8
    LANGUAGE plpgsql
    SECURITY DEFINER
    VOLATILE
AS $$


DECLARE
    -- Основные рабочие переменные загрузки.
    v_rows               bigint := 0;
    v_cols               text;
    v_sql                text;
    v_source_from        text;
    v_insert_filter      text := '';
    v_is_func            boolean;
    v_is_table_exists    boolean;
    v_schema_ext         text := 's_adb_as_services_csoko_stg';
    v_table_ext          text;
    v_rows_before_del    bigint;
    v_rows_to_del        numeric;
    v_ctl_loading        bigint[];
    v_has_src_ctl_loading boolean := false;
    v_has_tgt_ctl_loading boolean := false;
    v_deleted_rows        bigint := 0;
    v_lock_acquired       boolean := false;

    -- Таймеры нужны для диагностики узких мест через etl_run.extra.
    v_run_start          timestamptz := clock_timestamp();
    v_step_start         timestamptz;
    v_query_start        timestamptz;
    v_finished_at        timestamptz;
    v_duration_ms        bigint;

    v_is_error           boolean := false;
    v_error              text := '';
    v_extra              text := '';

    v_src_row_count      bigint := 0;
    v_tgt_row_count      bigint := 0;

    -- Диагностика исключений: сохраняем детали в error_text/extra вместо RAISE INFO.
    v_sqlstate           text := '';
    v_msg                text := '';
    v_detail             text := '';
    v_hint               text := '';
    v_context            text := '';
BEGIN
    v_step_start := clock_timestamp();
    v_extra := v_extra || format(
        'step=1_validation; target=%s.%s; source=%s.%s; requested_truncate=%s; requested_analyze=%s; compare_row_count=%s; ',
        p_schema_target,
        p_table_target,
        p_schema_src,
        p_table_src,
        p_truncate,
        p_do_analyze,
        p_compare_row_count
    );

    /* 1) Валидация */
    IF p_schema_target IS NULL OR p_table_target IS NULL THEN
        v_is_error := true;
        v_rows := -1;
        v_error := format('Target table is not specified (schema=%s, table=%s)', p_schema_target, p_table_target);
    ELSIF p_schema_src IS NULL OR p_table_src IS NULL THEN
        v_is_error := true;
        v_rows := -1;
        v_error := format('Source table is not specified (schema=%s, table=%s)', p_schema_src, p_table_src);
    END IF;
    v_extra := v_extra || format('validation_status=%s; ', CASE WHEN v_is_error THEN 'ERROR' ELSE 'OK' END);
    v_extra := v_extra || format(
        'validation_duration_ms=%s; ',
        (extract(epoch from (clock_timestamp() - v_step_start)) * 1000)::bigint
    );

    /* 1.1) Защита от параллельного запуска на одну target-таблицу */
    IF NOT v_is_error THEN
        v_step_start := clock_timestamp();
        v_extra := v_extra || 'step=1_1_target_advisory_lock; ';

        -- Используем transaction-level advisory lock: он автоматически
        -- освобождается при завершении транзакции, даже если функция упадет.
        -- try-вариант не подвешивает повторный запуск, а сразу пишет понятную ошибку.
        v_lock_acquired := pg_try_advisory_xact_lock(
            hashtext(p_schema_target),
            hashtext(p_table_target)
        );

        IF NOT v_lock_acquired THEN
            v_is_error := true;
            v_rows := -1;
            v_error := format(
                'Refresh for target %I.%I is already running',
                p_schema_target,
                p_table_target
            );
        END IF;

        v_extra := v_extra || format(
            'target_lock_key=%s.%s; target_lock_acquired=%s; target_lock_duration_ms=%s; ',
            p_schema_target,
            p_table_target,
            v_lock_acquired,
            (extract(epoch from (clock_timestamp() - v_step_start)) * 1000)::bigint
        );
    END IF;

    /* 2) Колонки: ВСЕ колонки источника (в порядке source) */
    IF NOT v_is_error THEN
        v_step_start := clock_timestamp();
        v_extra := v_extra || 'step=2_resolve_source_columns; ';

        -- f_get_params одинаково обрабатывает обычные relations и set-returning functions.
        SELECT params, is_func FROM s_adb_as_services_csoko_stg.f_get_params(p_schema_src, p_table_src)
        INTO v_cols, v_is_func;
        v_extra := v_extra || format(
            'get_params_duration_ms=%s; ',
            (extract(epoch from (clock_timestamp() - v_step_start)) * 1000)::bigint
        );
        v_extra := v_extra || format('get_params_columns=%s; source_is_function=%s; ', v_cols, v_is_func);
        IF v_cols IS NULL OR length(btrim(v_cols)) = 0 THEN
            v_is_error := true;
            v_rows := -1;
            v_error := format('Source table %I.%I not found or has no columns', p_schema_src, p_table_src);
        END IF;
        v_extra := v_extra || format('resolve_source_columns_status=%s; ', CASE WHEN v_is_error THEN 'ERROR' ELSE 'OK' END);
        v_extra := v_extra || format(
            'resolve_source_columns_duration_ms=%s; ',
            (extract(epoch from (clock_timestamp() - v_step_start)) * 1000)::bigint
        );
    END IF;

    IF NOT v_is_error THEN
        -- Источник может быть таблицей/view или функцией без аргументов.
        -- Дальше используем v_source_from как готовый фрагмент FROM.
        IF v_is_func THEN
            v_source_from := format('%I.%I()', p_schema_src, p_table_src);
        ELSE
            v_source_from := format('%I.%I', p_schema_src, p_table_src);
        END IF;
        v_extra := v_extra || format('source_from=%s; ', v_source_from);
    END IF;

    /* 3) TRUNCATE */
    IF NOT v_is_error THEN
        v_step_start := clock_timestamp();
        BEGIN
            v_extra := v_extra || 'step=3_prepare_target; ';
            IF NOT p_truncate THEN
                v_extra := v_extra || 'load_strategy=incremental_requested; ';

                -- Инкрементальная стратегия работает только при наличии ctl_loading
                -- и в источнике, и в целевой таблице. Иначе безопаснее перейти
                -- в полный truncate-refresh.
                SELECT EXISTS (
                    SELECT 1
                      FROM information_schema.columns t
                     WHERE t.table_schema = p_schema_target
                       AND t.table_name = p_table_target
                       AND lower(t.column_name) = 'ctl_loading'
                )
                  INTO v_has_tgt_ctl_loading;
                v_extra := v_extra || format(
                    'target_ctl_loading_check_duration_ms=%s; ',
                    (extract(epoch from (clock_timestamp() - v_step_start)) * 1000)::bigint
                );

                IF v_is_func THEN
                    v_query_start := clock_timestamp();
                    v_has_src_ctl_loading := lower(regexp_replace(v_cols, '\s+', '', 'g')) ~ '(^|,)(ctl_loading|"ctl_loading")(,|$)';
                    v_extra := v_extra || format(
                        'source_ctl_loading_check_duration_ms=%s; ',
                        (extract(epoch from (clock_timestamp() - v_query_start)) * 1000)::bigint
                    );
                ELSE
                    v_query_start := clock_timestamp();
                    SELECT EXISTS (
                        SELECT 1
                          FROM information_schema.columns s
                         WHERE s.table_schema = p_schema_src
                           AND s.table_name = p_table_src
                           AND lower(s.column_name) = 'ctl_loading'
                    )
                      INTO v_has_src_ctl_loading;
                    v_extra := v_extra || format(
                        'source_ctl_loading_check_duration_ms=%s; ',
                        (extract(epoch from (clock_timestamp() - v_query_start)) * 1000)::bigint
                    );
                END IF;

                v_extra := v_extra || format(
                    'incremental_ctl_loading_check: source=%s; target=%s; ',
                    v_has_src_ctl_loading,
                    v_has_tgt_ctl_loading
                );

                IF NOT v_has_src_ctl_loading OR NOT v_has_tgt_ctl_loading THEN
                    p_truncate := true;
                    v_extra := v_extra || 'load_strategy=truncate; incremental_fallback_reason=missing_ctl_loading; ';
                ELSE
                    -- Сначала оцениваем объем удаления: при больших изменениях
                    -- дешевле сделать полный truncate-refresh.
                    -- Список ctl_loading не собираем в массив, чтобы не держать
                    -- потенциально большой набор ключей в памяти функции.
                    v_sql := format(
                        'SELECT count(*)
                           FROM %I.%I tgt
                          WHERE NOT EXISTS (
                              SELECT 1
                                FROM %s src
                               WHERE src.ctl_loading IS NOT DISTINCT FROM tgt.ctl_loading
                          )',
                        p_schema_target,
                        p_table_target,
                        v_source_from
                    );
                    v_extra := v_extra || format('incremental_rows_to_delete_sql=%s; ', v_sql);
                    v_query_start := clock_timestamp();
                    EXECUTE v_sql INTO v_rows_to_del;
                    v_extra := v_extra || format(
                        'incremental_rows_to_delete_duration_ms=%s; ',
                        (extract(epoch from (clock_timestamp() - v_query_start)) * 1000)::bigint
                    );
                    v_extra := v_extra || format('incremental_rows_to_delete=%s; ', v_rows_to_del);

                    IF v_rows_to_del > 0 THEN
                        -- Старая эвристика производительности: маленькие изменения
                        -- удаляем точечно, большие изменения превращаем в full refresh.
                        v_sql := format(
                            'SELECT count(*)
                               FROM %I.%I',
                            p_schema_target,
                            p_table_target
                        );
                        v_extra := v_extra || format('incremental_rows_before_delete_sql=%s; ', v_sql);
                        v_query_start := clock_timestamp();
                        EXECUTE v_sql INTO v_rows_before_del;
                        v_extra := v_extra || format(
                            'incremental_rows_before_delete_duration_ms=%s; ',
                            (extract(epoch from (clock_timestamp() - v_query_start)) * 1000)::bigint
                        );
                        v_extra := v_extra || format('incremental_rows_before_delete=%s; ', v_rows_before_del);

                        IF v_rows_before_del > 0
                           AND v_rows_before_del < 1000000
                           AND v_rows_to_del / v_rows_before_del < 0.5 THEN
                            -- Удаляем из target те ctl_loading, которых больше нет в source.
                            -- anti-join корректно работает и с обычной таблицей, и с функцией-источником.
                            v_sql := format(
                                'DELETE FROM %I.%I tgt
                                 WHERE NOT EXISTS (
                                     SELECT 1
                                     FROM %s src
                                     WHERE src.ctl_loading IS NOT DISTINCT FROM tgt.ctl_loading
                                 )',
                                p_schema_target,
                                p_table_target,
                                v_source_from
                            );
                            v_extra := v_extra || format('incremental_delete_sql=%s; ', v_sql);
                            v_query_start := clock_timestamp();
                            EXECUTE v_sql;
                            GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;
                            v_extra := v_extra || format(
                                'incremental_delete_duration_ms=%s; ',
                                (extract(epoch from (clock_timestamp() - v_query_start)) * 1000)::bigint
                            );
                            v_extra := v_extra || format('incremental_deleted=%s; ', v_deleted_rows);
                        ELSE
                            p_truncate := true;
                            v_extra := v_extra || 'load_strategy=truncate; incremental_fallback_reason=delete_threshold; ';
                        END IF;
                    END IF;

                    IF NOT p_truncate THEN
                        v_extra := v_extra || 'load_strategy=incremental; ';
                        -- На insert берем только те ctl_loading, которых еще нет в target.
                        -- Общие партии не трогаем: это сохраняет incremental-семантику.
                        v_insert_filter := format(
                            ' WHERE NOT EXISTS (
                                  SELECT 1
                                  FROM %I.%I tgt
                                  WHERE tgt.ctl_loading IS NOT DISTINCT FROM src.ctl_loading
                              )',
                            p_schema_target,
                            p_table_target
                        );
                    END IF;
                END IF;
            ELSE
                v_extra := v_extra || 'load_strategy=truncate_requested; ';
            END IF;

            IF p_truncate THEN
                -- В truncate-режиме фильтр вставки сбрасывается:
                -- после очистки нужно заново вставить весь source.
                v_sql := format('TRUNCATE TABLE %I.%I', p_schema_target, p_table_target);
                v_extra := v_extra || format('truncate_sql=%s; ', v_sql);
                v_query_start := clock_timestamp();
                EXECUTE v_sql;
                v_extra := v_extra || format(
                    'truncate_duration_ms=%s; ',
                    (extract(epoch from (clock_timestamp() - v_query_start)) * 1000)::bigint
                );
                v_extra := v_extra || 'truncate_status=done; ';
                v_insert_filter := '';
            END IF;
            v_extra := v_extra || 'prepare_target_status=OK; ';
            v_extra := v_extra || format(
                'prepare_target_duration_ms=%s; ',
                (extract(epoch from (clock_timestamp() - v_step_start)) * 1000)::bigint
            );
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_sqlstate = RETURNED_SQLSTATE,
                v_msg      = MESSAGE_TEXT,
                v_detail   = PG_EXCEPTION_DETAIL,
                v_hint     = PG_EXCEPTION_HINT,
                v_context  = PG_EXCEPTION_CONTEXT;

            v_is_error := true;
            v_rows := -1;
            v_error :=
                'SQLSTATE=' || coalesce(v_sqlstate,'') ||
                '; MESSAGE=' || coalesce(v_msg,'') ||
                CASE WHEN v_detail  IS NOT NULL THEN '; DETAIL='  || v_detail  ELSE '' END ||
                CASE WHEN v_hint    IS NOT NULL THEN '; HINT='    || v_hint    ELSE '' END ||
                CASE WHEN v_context IS NOT NULL THEN '; CONTEXT=' || v_context ELSE '' END;

            v_extra := v_extra || format('failed_sql=%s; ', v_sql);
            v_extra := v_extra || format(
                'prepare_target_duration_ms=%s; ',
                (extract(epoch from (clock_timestamp() - v_step_start)) * 1000)::bigint
            );
        END;
    END IF;

    /* 4) INSERT: в target вставляем все колонки из source */
    IF NOT v_is_error THEN
        v_step_start := clock_timestamp();
        BEGIN
            v_extra := v_extra || 'step=4_insert; ';

            -- Считаем ровно тот набор source-строк, который будет вставляться.
            -- Для incremental это уже source с фильтром новых ctl_loading.
            v_sql := format('SELECT COUNT(*) FROM %s src%s', v_source_from, v_insert_filter);
            v_extra := v_extra || format('source_row_count_sql=%s; ', v_sql);
            v_query_start := clock_timestamp();
            EXECUTE v_sql INTO v_src_row_count;
            v_extra := v_extra || format(
                'source_row_count_duration_ms=%s; ',
                (extract(epoch from (clock_timestamp() - v_query_start)) * 1000)::bigint
            );
            v_extra := v_extra || format('source_row_count=%s; ', v_src_row_count);

            -- Колонки берутся из source в его порядке; target должен иметь тот же набор.
            v_sql := format(
                'INSERT INTO %I.%I (%s) SELECT %s FROM %s src%s',
                p_schema_target, p_table_target, v_cols,
                v_cols,
                v_source_from,
                v_insert_filter
            );
            v_query_start := clock_timestamp();
            EXECUTE v_sql;
            v_extra := v_extra || format('insert_sql=%s; ', v_sql);
            GET DIAGNOSTICS v_rows = ROW_COUNT;
            v_extra := v_extra || format(
                'insert_duration_ms=%s; ',
                (extract(epoch from (clock_timestamp() - v_query_start)) * 1000)::bigint
            );
            v_extra := v_extra || format('inserted_rows=%s; insert_status=OK; ', v_rows);
            v_extra := v_extra || format(
                'insert_step_duration_ms=%s; ',
                (extract(epoch from (clock_timestamp() - v_step_start)) * 1000)::bigint
            );
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_sqlstate = RETURNED_SQLSTATE,
                v_msg      = MESSAGE_TEXT,
                v_detail   = PG_EXCEPTION_DETAIL,
                v_hint     = PG_EXCEPTION_HINT,
                v_context  = PG_EXCEPTION_CONTEXT;

            v_is_error := true;
            v_rows := -1;
            v_error :=
                'SQLSTATE=' || coalesce(v_sqlstate,'') ||
                '; MESSAGE=' || coalesce(v_msg,'') ||
                CASE WHEN v_detail  IS NOT NULL THEN '; DETAIL='  || v_detail  ELSE '' END ||
                CASE WHEN v_hint    IS NOT NULL THEN '; HINT='    || v_hint    ELSE '' END ||
                CASE WHEN v_context IS NOT NULL THEN '; CONTEXT=' || v_context ELSE '' END;

            v_extra := v_extra || format('failed_sql=%s; ', v_sql);
            v_extra := v_extra || format(
                'insert_step_duration_ms=%s; ',
                (extract(epoch from (clock_timestamp() - v_step_start)) * 1000)::bigint
            );
        END;
    END IF;

    /* 5) ANALYZE */
    IF NOT v_is_error AND p_do_analyze THEN
        v_step_start := clock_timestamp();
        BEGIN
            -- ANALYZE вынесен отдельным шагом, потому что на больших таблицах
            -- он может быть заметной частью общего времени загрузки.
            v_extra := v_extra || 'step=5_analyze; ';
            v_sql := format('ANALYZE %I.%I', p_schema_target, p_table_target);
            v_extra := v_extra || format('analyze_sql=%s; ', v_sql);
            EXECUTE v_sql;
            v_extra := v_extra || 'analyze_status=OK; ';
            v_extra := v_extra || format(
                'analyze_duration_ms=%s; ',
                (extract(epoch from (clock_timestamp() - v_step_start)) * 1000)::bigint
            );
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_sqlstate = RETURNED_SQLSTATE,
                v_msg      = MESSAGE_TEXT,
                v_detail   = PG_EXCEPTION_DETAIL,
                v_hint     = PG_EXCEPTION_HINT,
                v_context  = PG_EXCEPTION_CONTEXT;

            v_is_error := true;
            v_rows := -1;
            v_error :=
                'SQLSTATE=' || coalesce(v_sqlstate,'') ||
                '; MESSAGE=' || coalesce(v_msg,'') ||
                CASE WHEN v_detail  IS NOT NULL THEN '; DETAIL='  || v_detail  ELSE '' END ||
                CASE WHEN v_hint    IS NOT NULL THEN '; HINT='    || v_hint    ELSE '' END ||
                CASE WHEN v_context IS NOT NULL THEN '; CONTEXT=' || v_context ELSE '' END;

            v_extra := v_extra || format('failed_sql=%s; ', v_sql);
            v_extra := v_extra || format(
                'analyze_duration_ms=%s; ',
                (extract(epoch from (clock_timestamp() - v_step_start)) * 1000)::bigint
            );
        END;
    ELSIF NOT v_is_error THEN
        v_extra := v_extra || 'step=5_analyze; analyze_status=skipped; analyze_duration_ms=0; ';
    END IF;

    /* 6) Подсчет строк в источнике и целевой таблице */
    IF NOT v_is_error THEN
        v_step_start := clock_timestamp();
        BEGIN
            v_extra := v_extra || 'step=6_row_count_check; ';

            /* Данные о количестве строк source получены перед INSERT. */
            v_tgt_row_count := v_rows;

            v_extra := v_extra || format('source_row_count=%s; target_row_count=%s; ', v_src_row_count, v_tgt_row_count);

            /* Проверка совпадения количества строк только если p_compare_row_count = true */
            IF p_compare_row_count AND v_src_row_count != v_tgt_row_count THEN
                v_is_error := true;
                v_error := format('Row count mismatch: source=%s, target=%s', v_src_row_count, v_tgt_row_count);
                v_rows := -1;
            END IF;
            v_extra := v_extra || format('row_count_check_status=%s; ', CASE WHEN v_is_error THEN 'ERROR' ELSE 'OK' END);
            v_extra := v_extra || format(
                'row_count_check_duration_ms=%s; ',
                (extract(epoch from (clock_timestamp() - v_step_start)) * 1000)::bigint
            );
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_sqlstate = RETURNED_SQLSTATE,
                v_msg      = MESSAGE_TEXT,
                v_detail   = PG_EXCEPTION_DETAIL,
                v_hint     = PG_EXCEPTION_HINT,
                v_context  = PG_EXCEPTION_CONTEXT;

            -- Ошибка при подсчете строк не приводит к фатальной ошибке функции, только записывается в extra
            v_extra := v_extra || format('row_count_query_error=SQLSTATE=%s; MESSAGE=%s; failed_sql=%s; ',
                coalesce(v_sqlstate,''), coalesce(v_msg,''), v_sql);
            v_extra := v_extra || format(
                'row_count_check_duration_ms=%s; ',
                (extract(epoch from (clock_timestamp() - v_step_start)) * 1000)::bigint
            );
        END;
    END IF;

    /* 7) Финал: один INSERT в журнал */
    v_finished_at := clock_timestamp();
    v_duration_ms := (extract(epoch from (v_finished_at - v_run_start)) * 1000)::bigint;
    v_extra := v_extra || format(
        'step=7_finish; final_status=%s; duration_ms=%s; final_rows=%s; ',
        CASE WHEN v_is_error THEN 'ERROR' ELSE 'SUCCESS' END,
        v_duration_ms,
        v_rows
    );

    -- Внутри функции extra удобно собирать как key=value; key=value; ...
    -- Перед записью превращаем это в многострочный текст для чтения в UI/psql.
    v_extra := regexp_replace(v_extra, ';[[:space:]]+', E';\n', 'g');

    BEGIN
        -- Журнал пишем один раз в конце, чтобы запись отражала итоговый статус
        -- и содержала полную трассу шагов, SQL и таймингов.
        INSERT INTO s_adb_as_services_csoko_stg.etl_run(
            run_id,
            function_name,
            src_schema, src_table,
            tgt_schema, tgt_table,
            do_truncate, do_analyze,
            status,
            rows_inserted,
            error_text,
            started_at,
            finished_at,
            duration_ms,
            extra,
            src_row_count,
            tgt_row_count
        )
        VALUES (
            gen_random_uuid(),
            's_adb_as_services_csoko_stg.f_refresh_table_v2_1(text, text, text, text, bool, bool, bool)',
            p_schema_src, p_table_src,
            p_schema_target, p_table_target,
            p_truncate, p_do_analyze,
            CASE WHEN v_is_error THEN 'ERROR' ELSE 'SUCCESS' END,
            v_rows,
            CASE WHEN v_is_error THEN v_error ELSE NULL END,
            v_run_start,
            v_finished_at,
            v_duration_ms,
            v_extra,
            v_src_row_count,
            v_tgt_row_count
        );
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    RETURN v_rows;
END;


$$
EXECUTE ON ANY;

-- DROP FUNCTION s_adb_as_services_csoko_stg.f_refresh_table_v2_1(text, text, text, text, bool, bool, bool);

CREATE OR REPLACE FUNCTION s_adb_as_services_csoko_stg.f_refresh_table_v2_1(p_schema_target text, p_table_target text, p_schema_src text, p_table_src text, p_truncate bool DEFAULT true, p_do_analyze bool DEFAULT true, p_compare_row_count bool DEFAULT false)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$


DECLARE
    v_rows        		bigint := 0;
    v_cols        		text;
    v_sql         		text;
	v_is_func     		boolean;
	v_is_table_exists	boolean;
	v_schema_ext		text := 's_adb_as_services_csoko_stg';
	v_table_ext			text;
	v_rows_before_del	bigint;
	v_rows_to_del		numeric;
	v_ctl_loading		bigint[];

    v_run_start   		timestamptz := clock_timestamp();
    v_finished_at 		timestamptz;
    v_duration_ms 		bigint;

    v_is_error 			boolean := false;
    v_error    			text := '';
    v_extra    			text := '';

    v_src_row_count 	bigint := 0;
    v_tgt_row_count 	bigint := 0;

    -- diagnostics
    v_sqlstate 			text := '';
    v_msg      			text := '';
    v_detail   			text := '';
    v_hint     			text := '';
    v_context  			text := '';
BEGIN
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

    /* 2) Колонки: ВСЕ колонки источника (в порядке source) */
    IF NOT v_is_error THEN
        /*SELECT string_agg(quote_ident(s.column_name), ', ' ORDER BY s.ordinal_position)
          INTO v_cols
          FROM information_schema.columns s
         WHERE s.table_schema = p_schema_src
           AND s.table_name   = p_table_src;
		*/

		SELECT params, is_func FROM s_adb_as_services_csoko_stg.f_get_params(p_schema_src, p_table_src)
		INTO v_cols, v_is_func;
		v_extra := v_extra || format('s_adb_as_services_csoko_stg.f_get_params: v_cols=%s; v_is_func=%s;', v_cols, v_is_func);
        IF v_cols IS NULL OR length(btrim(v_cols)) = 0 THEN
            v_is_error := true;
            v_rows := -1;
            v_error := format('Source table %I.%I not found or has no columns', p_schema_src, p_table_src);
        END IF;

		RAISE info '% %', v_cols, v_is_func;
    END IF;

	RAISE info '3';

    /* 3) TRUNCATE */
    IF NOT v_is_error THEN
        BEGIN
			if not p_truncate then
				-- ПРОВЕРКА АКТУАЛЬНОСТИ ДАННЫХ ПО CTL_LOADING
				-- определяем наличие external table
				v_table_ext = p_table_target || '_ext';
				v_sql = format('
					SELECT EXISTS (
						SELECT 1
						FROM information_schema.tables WHERE table_schema = ''%I'' AND table_name = ''%I''
					)', v_schema_ext, v_table_ext
				);
				v_extra := v_extra || format('3.1 v_sql=%s;', v_sql);
				EXECUTE v_sql
				INTO v_is_table_exists;
				--RAISE info '3.2: %', v_sql;
				--RAISE info '3.1: %.% exists? %', v_schema_ext, v_table_ext, v_is_table_exists;

				IF v_is_table_exists THEN
					-- определяем неактуальные загрузки и кол-во записей
					v_sql := format('
						WITH t1 AS (
							SELECT DISTINCT ctl_loading
							FROM %I.%I
						)
						SELECT array_agg(distinct t2.ctl_loading), count(*)
						FROM %I.%I t2
						LEFT JOIN t1 ON t1.ctl_loading = t2.ctl_loading
						WHERE t1.ctl_loading IS NULL',
						v_schema_ext, v_table_ext,
						p_schema_target, p_table_target
					);
					v_extra := v_extra || format('3.2 v_sql=%s;', v_sql);
					--RAISE info '-------- %', v_sql;
					EXECUTE v_sql
					INTO v_ctl_loading, v_rows_to_del;
					v_extra := v_extra || format('3.2 ctl_loading to del=%s; Rows to del:=%s', v_ctl_loading,v_rows_to_del);
					--RAISE info '3.2: ctl_loading to del: %, Rows to del:  %', v_ctl_loading, v_rows_to_del;

					IF v_rows_to_del > 0 THEN
						-- подсчет количества записей в таблице для выбора стратегии удаления
						v_sql = format('
							SELECT count(*)
							FROM %I.%I
						', p_schema_target, p_table_target);
						v_extra := v_extra || format('3.3: v_sql = %s;', v_sql);
						EXECUTE v_sql INTO v_rows_before_del;
						v_extra := v_extra || format('3.3: Total rows in table v_rows_before_del=%s;', v_rows_before_del);
						--RAISE info '3.3: Total rows in table: %', v_rows_before_del;

						IF v_rows_before_del < 1000000 and v_rows_to_del / v_rows_before_del < 0.5 then
							v_sql := format('
								DELETE FROM %I.%I
								WHERE ctl_loading = ANY(%L)',
								p_schema_target, p_table_target, v_ctl_loading
							);
							v_extra := v_extra || format('3.4: v_sql = %s;', v_sql);
							--RAISE info '===== %', v_sql;
							EXECUTE v_sql;
							v_extra := v_extra || format('3.4: Deleted %s rows! ;', v_rows_to_del);
							--RAISE info '3.4: Deleted % rows!', v_rows_to_del;
						ELSE
							p_truncate = True;
						END IF;
					END IF;
				END IF;
			end if;

			if p_truncate then
	            v_sql := format('TRUNCATE TABLE %I.%I', p_schema_target, p_table_target);
	            v_extra := v_extra || format('3.4: v_sql=%s; ', v_sql);
				EXECUTE v_sql;
				v_extra := v_extra || format('3.4: Truncated! ;');
				--RAISE info '3.4: Truncated!';
			end if;
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
        END;
    END IF;

	RAISE info '4: %', v_is_error;

    /* 4) INSERT: в target вставляем все колонки из source */
    IF NOT v_is_error THEN
		raise info '4.1';
        v_sql := format(
            'INSERT INTO %I.%I (%s) SELECT %s FROM %s.%s',
            p_schema_target, p_table_target, v_cols,
            v_cols,
            p_schema_src, p_table_src
        );

        BEGIN
            EXECUTE v_sql;
			v_extra := v_extra || format('4.1: insert_sql=%s; ', v_sql);
			--raise info '4.2: %', v_sql;
            GET DIAGNOSTICS v_rows = ROW_COUNT;
			v_extra := v_extra || format('4.3: inserted=%s; ', v_rows);
			RAISE info '4.3: inserted %', v_rows;
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
        END;
    END IF;

    /* 5) ANALYZE */
    IF NOT v_is_error AND p_do_analyze THEN
        BEGIN
            v_sql := format('ANALYZE %I.%I', p_schema_target, p_table_target);
            v_extra := v_extra || format('analyze_sql=%s; ', v_sql);
            EXECUTE v_sql;
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
        END;
    END IF;

    /* 6) Подсчет строк в источнике и целевой таблице */
    IF NOT v_is_error THEN
        BEGIN
            v_sql := format('SELECT COUNT(*) FROM %I.%I', p_schema_src, p_table_src);
            EXECUTE v_sql INTO v_src_row_count;

            /*Данные о количество строк уже получены */
            v_tgt_row_count := v_rows;

            v_extra := v_extra || format('source_row_count=%s; target_row_count=%s; ', v_src_row_count, v_tgt_row_count);

            /* Проверка совпадения количества строк только если p_compare_row_count = true */
            IF p_compare_row_count AND v_src_row_count != v_tgt_row_count THEN
                v_is_error := true;
                v_error := format('Row count mismatch: source=%s, target=%s', v_src_row_count, v_tgt_row_count);
                v_rows := -1;
            END IF;
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
        END;
    END IF;

    /* 7) Финал: один INSERT в журнал */
    v_finished_at := clock_timestamp();
    v_duration_ms := (extract(epoch from (v_finished_at - v_run_start)) * 1000)::bigint;

    BEGIN
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
            's_adb_as_services_csoko_stg.f_refresh_table_v2(text, text, text, text, bool, bool, bool)',
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

    IF v_is_error THEN
        RAISE INFO 'Error in f_refresh_table_v2: %', v_error;
    END IF;

    RETURN v_rows;
END;


$$
EXECUTE ON ANY;

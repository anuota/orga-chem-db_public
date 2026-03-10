-- 0006_flatten_legacy_orig_value_payloads.sql
-- Convert legacy payload objects like {"orig": "X", "value": 123}
-- into plain scalar values (123) inside every analysis table JSONB entries.data.

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT
            c.table_name,
            c.column_name
        FROM information_schema.columns c
        WHERE c.table_schema = 'public'
          AND c.data_type = 'jsonb'
          AND c.column_name = c.table_name
          AND EXISTS (
              SELECT 1
              FROM information_schema.columns s
              WHERE s.table_schema = 'public'
                AND s.table_name = c.table_name
                AND s.column_name = 'samplenumber'
          )
        ORDER BY c.table_name
    LOOP
        EXECUTE format(
            $sql$
            UPDATE public.%I t
            SET %I = jsonb_set(
                COALESCE(t.%I, '{}'::jsonb),
                '{entries}',
                COALESCE((
                    SELECT jsonb_agg(
                        jsonb_set(
                            e.elem,
                            '{data}',
                            COALESCE((
                                SELECT jsonb_object_agg(
                                    kv.key,
                                    CASE
                                        WHEN jsonb_typeof(kv.value) = 'object' AND kv.value ? 'value'
                                            THEN kv.value->'value'
                                        ELSE kv.value
                                    END
                                )
                                FROM jsonb_each(
                                    CASE
                                        WHEN jsonb_typeof(e.elem->'data') = 'object'
                                            THEN e.elem->'data'
                                        ELSE '{}'::jsonb
                                    END
                                ) kv
                            ), '{}'::jsonb),
                            true
                        )
                        ORDER BY e.ord
                    )
                    FROM jsonb_array_elements(
                        COALESCE(t.%I->'entries', '[]'::jsonb)
                    ) WITH ORDINALITY AS e(elem, ord)
                ), '[]'::jsonb),
                true
            )
            WHERE t.%I ? 'entries'
              AND t.%I::text LIKE '%%"orig"%%';
            $sql$,
            r.table_name,
            r.column_name,
            r.column_name,
            r.column_name,
            r.column_name,
            r.column_name
        );
    END LOOP;
END
$$;

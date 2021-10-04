--// КЛАДР : дома

DO $$ begin raise notice '--- Processing kladr_house ---'; end; $$;

-- создаем временную таблицу с импортируемыми данными КЛАДР'а
CREATE TEMPORARY TABLE _kladr_house(
  LIKE kladr_house INCLUDING INDEXES
);

CREATE TABLE IF NOT EXISTS kladr_house$log(
  ro kladr_house,
  rn kladr_house
);

-- триггер на логирование
DROP TRIGGER IF EXISTS log_kladr_house ON kladr_house;
CREATE TRIGGER log_kladr_house
  AFTER INSERT OR UPDATE OR DELETE
  ON kladr_house
    FOR EACH ROW
      EXECUTE PROCEDURE diff$log();

-- заполняем преобразованными из DBF данными
INSERT INTO _kladr_house(code, codeExt, name, idx, ifns, ocato)
  SELECT
    regexp_replace(substr("CODE", 1, 15), '^(.{0,}?)((((0{3})?0{3})?0{3})?0{4})?$', E'\\1', 'ig'),
    substr("CODE", 16, 4),
    "NAME",
    nullif("INDEX", ''),
    nullif("GNINMB", ''),
    nullif("OCATD", '')
  FROM
    "DOMA.DBF";

-- удаляем отсутствующие
DELETE FROM
  kladr_house T
USING
  kladr_house X LEFT JOIN
  _kladr_house Y
    USING(code, codeExt)
WHERE
  (T.code, T.codeExt) = (X.code, X.codeExt) AND
  Y IS NULL;

-- обновляем оставшиеся
UPDATE
  kladr_house kl
SET
  (
    name,
    idx,
    ifns,
    ocato
  ) =
  (
    kli.name,
    kli.idx,
    kli.ifns,
    kli.ocato
  )
FROM
  _kladr_house kli
WHERE
  (kl.code, kl.codeExt) = (kli.code, kli.codeExt) AND
  (
    kl.name,
    kl.idx,
    kl.ifns,
    kl.ocato
  ) IS DISTINCT FROM
  (
    kli.name,
    kli.idx,
    kli.ifns,
    kli.ocato
  );

-- очищаем совпадающие
DELETE FROM
  _kladr_house kli
USING
  kladr_house kl
WHERE
  (kli.code, kli.codeExt) = (kl.code, kl.codeExt);

-- вставляем оставшиеся
INSERT INTO kladr_house
  SELECT
    *
  FROM
    _kladr_house;

-- обновляем поисковый кэш
DELETE FROM
  kladr_hs
WHERE
  (code) IN (
      SELECT
        (ro).code
      FROM
        kladr_house$log
      WHERE
        ro IS DISTINCT FROM NULL
    UNION ALL
      SELECT
        (rn).code
      FROM
        kladr_house$log
      WHERE
        rn IS DISTINCT FROM NULL
  );

-- заполняем преобразованными данными
CREATE TEMPORARY TABLE _kladr_hs0 AS
  SELECT DISTINCT ON(code, house)
    code,
    idx,
    ifns,
    ocato,
    unnest(houses) house
  FROM
    (
      SELECT
        *,
        CASE
          WHEN _range IS NULL AND name ~ E'_' THEN ARRAY[regexp_replace(name, '_', '-')]
          WHEN _range IS NULL THEN ARRAY[name]
          WHEN _range IS NOT NULL THEN ARRAY(
            SELECT
              i::text
            FROM
              generate_series(_range[1]::integer + CASE WHEN _range[4] IS NOT NULL THEN (_range[1]::integer + _range[4]::integer) % 2 ELSE 0 END, _range[2]::integer, _range[3]::integer) i
          )
          ELSE NULL
        END houses
      FROM
        (
          SELECT
            code,
            idx,
            ifns,
            ocato,
            name,
            CASE
              WHEN name ~ E'^Н\\(\\d+-\\d+\\)$' THEN regexp_split_to_array(substr(name, 3, length(name) - 3), '-') || '2'::text || '1'::text
              WHEN name ~ E'^Ч\\(\\d+-\\d+\\)$' THEN regexp_split_to_array(substr(name, 3, length(name) - 3), '-') || '2'::text || '0'::text
              WHEN name = 'Н' THEN '{1,999,2}'::text[]
              WHEN name = 'Ч' THEN '{2,998,2}'::text[]
              WHEN name ~ E'^\\d+-\\d+$' THEN regexp_split_to_array(name, '-') || '1'::text
              ELSE NULL
            END _range
          FROM
            (
              SELECT
                code,
                idx,
                ifns,
                ocato,
                unnest(regexp_split_to_array(upper(name), ',')) "name"
              FROM
                kladr_house
              WHERE
                (code) IN (
                    SELECT
                      (ro).code
                    FROM
                      kladr_house$log
                    WHERE
                      ro IS DISTINCT FROM NULL
                  UNION ALL
                    SELECT
                      (rn).code
                    FROM
                      kladr_house$log
                    WHERE
                      rn IS DISTINCT FROM NULL
                )
            ) T
        ) T
    ) T
ORDER BY
  code, house, (_range IS NULL) DESC;

CREATE INDEX ON _kladr_hs0(code, house, idx DESC NULLS LAST);

CREATE TEMPORARY TABLE _kladr_hs1 AS
  SELECT DISTINCT ON (code, house)
    code,
    idx,
    ifns,
    ocato,
    house
  FROM
    _kladr_hs0
  ORDER BY
    code, house, idx DESC NULLS LAST;

CREATE INDEX ON _kladr_hs1(code, house);

CREATE TEMPORARY TABLE _kladr_hs2 AS
  SELECT
    code,
    coalesce(
      idx,
      coalesce(
        (
          SELECT
            idx
          FROM
            _kladr_hs1
          WHERE
            (code, house) = (T.code, regexp_replace(T.house, E'^(\\d+)(\\D)?.*$', E'\\1', 'ig'))
          LIMIT 1
        ),
        coalesce(
          (
            SELECT
              idx
            FROM
              kladr
            WHERE
              code IN (
                substr(T.code, 1, 15),
                substr(T.code, 1, 11),
                substr(T.code, 1,  8),
                substr(T.code, 1,  5),
                substr(T.code, 1,  2)
              ) AND
--              status = '00' AND
              idx IS NOT NULL
            ORDER BY
              length(code) DESC
            LIMIT 1
          ),
          ''
        )
      )
    ) idx,
    ifns,
    ocato,
    house
  FROM
    _kladr_hs1 T;

CREATE INDEX ON _kladr_hs2(code, idx, ifns, ocato, house);

INSERT INTO kladr_hs(code, idx, ifns, ocato, houses)
  SELECT
    code,
    idx,
    ifns,
    ocato,
    array_agg(house ORDER BY house) houses
  FROM
    _kladr_hs2
  GROUP BY
    1, 2, 3, 4;

DELETE FROM kladr_house$log;

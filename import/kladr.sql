--// КЛАДР : от регионов до улиц

DO $$ begin raise notice '--- Processing kladr ---'; end; $$;

-- создаем временную таблицу с импортируемыми данными КЛАДР'а
CREATE TEMPORARY TABLE _kladr(
  LIKE kladr INCLUDING INDEXES
);

CREATE TABLE IF NOT EXISTS kladr$log(
  ro kladr,
  rn kladr
);

-- триггер на логирование
DROP TRIGGER IF EXISTS log_kladr ON kladr;
CREATE TRIGGER log_kladr
  AFTER INSERT OR UPDATE OR DELETE
  ON kladr
    FOR EACH ROW
      EXECUTE PROCEDURE diff$log();

-- заполняем преобразованными из DBF данными
INSERT INTO _kladr(code, status, name, abbr, idx, ifns, ocato, lvl)
  SELECT DISTINCT ON(code, status)
    *
  FROM
    (
      SELECT
        regexp_replace(rpad(substr("CODE", 1, length("CODE") - 2), 15, '0'), '^(.{0,}?)((((0{3})?0{3})?0{3})?0{4})?$', E'\\1', 'ig') code,
        substr("CODE", length("CODE") - 1, 2) status,
        "NAME",
        "SOCR",
        nullif("INDEX", ''),
        nullif("GNINMB", ''),
        nullif("OCATD", ''),
        "STATUS"
      FROM
        (
          SELECT
            "CODE",
            "NAME",
            "SOCR",
            "INDEX",
            "GNINMB",
            "OCATD",
            "STATUS"::smallint
          FROM
            "KLADR.DBF"
        UNION ALL
          SELECT
            "CODE",
            "NAME",
            "SOCR",
            "INDEX",
            "GNINMB",
            "OCATD",
            NULL::smallint "STATUS"
          FROM
            "STREET.DBF"
        ) T
    ) T;

-- удаляем отсутствующие
DELETE FROM
  kladr T
USING
  kladr X LEFT JOIN
  _kladr Y
    USING(code, status)
WHERE
  (T.code, T.status) = (X.code, X.status) AND
  Y IS NULL;

-- обновляем оставшиеся
UPDATE
  kladr kl
SET
  (
    name,
    abbr,
    idx,
    ifns,
    ocato,
    lvl
  ) =
  (
    kli.name,
    kli.abbr,
    kli.idx,
    kli.ifns,
    kli.ocato,
    kli.lvl
  )
FROM
  _kladr kli
WHERE
  (kl.code, kl.status) = (kli.code, kli.status) AND
  (
    kl.name,
    kl.abbr,
    kl.idx,
    kl.ifns,
    kl.ocato,
    kl.lvl
  ) IS DISTINCT FROM
  (
    kli.name,
    kli.abbr,
    kli.idx,
    kli.ifns,
    kli.ocato,
    kli.lvl
  );

-- очищаем совпадающие
DELETE FROM
  _kladr kli
USING
  kladr kl
WHERE
  (kli.code, kli.status) = (kl.code, kl.status);

-- вставляем оставшиеся
INSERT INTO kladr
  SELECT
    *
  FROM
    _kladr;

-- обновляем поисковый кэш
DELETE FROM
  kladr_kw
WHERE
  (code, status) IN (
    SELECT
      (ro).code,
      (ro).status
    FROM
      kladr$log
    WHERE
      ro IS DISTINCT FROM NULL
  );

INSERT INTO
  kladr_kw(code, status, keyword)
SELECT DISTINCT
  code,
  status,
  kw
FROM
  (
    SELECT
      (rn).code,
      (rn).status,
      regexp_split_to_table(lower((rn).name), E'[^\\-a-zа-яё0-9]+', 'i') kw
    FROM
      kladr$log
    WHERE
      rn IS DISTINCT FROM NULL
  ) T
WHERE
  kw <> '';

DELETE FROM kladr$log;

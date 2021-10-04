-- создаем временную таблицу с импортируемыми данными КЛАДР'а

DO $$ begin raise notice '--- Processing kladr_repl ---'; end; $$;

CREATE TEMPORARY TABLE _kladr_repl(
  LIKE kladr_repl INCLUDING INDEXES
);

-- заполняем преобразованными из DBF данными
INSERT INTO _kladr_repl(oldCode, newCode)
  SELECT DISTINCT
    co,
    cn
  FROM
    (
      SELECT
        regexp_replace(rpad(co, 15, '0'), '^(.{0,}?)((((0{3})?0{3})?0{3})?0{4})?$', E'\\1', 'ig') co,
        so,
        regexp_replace(rpad(cn, 15, '0'), '^(.{0,}?)((((0{3})?0{3})?0{3})?0{4})?$', E'\\1', 'ig') cn,
        sn
      FROM
        (
          SELECT
            *,
            substr("OLDCODE", 1, length("OLDCODE") - 2) co,
            substr("OLDCODE", length("OLDCODE") - 1, 2) so,
            substr("NEWCODE", 1, length("NEWCODE") - 2) cn,
            substr("NEWCODE", length("NEWCODE") - 1, 2) sn
          FROM
            "ALTNAMES.DBF"
        ) T
    ) T;

-- удаляем отсутствующие
DELETE FROM
  kladr_repl T
USING
  kladr_repl X LEFT JOIN
  _kladr_repl Y
    USING(oldCode, newCode)
WHERE
  (T.oldCode, T.newCode) = (X.oldCode, X.newCode) AND
  Y IS NULL;

-- очищаем совпадающие
DELETE FROM
  _kladr_repl kli
USING
  kladr_repl kl
WHERE
  (kli.oldCode, kli.newCode) = (kl.oldCode, kl.newCode);

-- вставляем оставшиеся
INSERT INTO kladr_repl
  SELECT
    *
  FROM
    _kladr_repl;

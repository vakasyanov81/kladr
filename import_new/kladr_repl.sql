-- создаем временную таблицу с импортируемыми данными КЛАДР'а

DO $$ begin raise notice '--- Processing kladr_repl ---'; end; $$;

-- очистим таблицу
TRUNCATE TABLE kladr_repl;

-- заполняем преобразованными из DBF данными
INSERT INTO kladr_repl(oldCode, newCode)
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

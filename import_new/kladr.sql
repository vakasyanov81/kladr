--// КЛАДР : от регионов до улиц

DO $$ begin raise notice '--- Processing kladr ---'; end; $$;

-- очистим таблицу
TRUNCATE TABLE kladr;

-- заполняем преобразованными из DBF данными
INSERT INTO kladr(code, status, name, abbr, idx, ifns, ocato, lvl)
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

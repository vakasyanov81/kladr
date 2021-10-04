--// КЛАДР : сокращения

DO $$ begin raise notice '--- Processing kladr_abbr ---'; end; $$;

-- очищаем таблицу
TRUNCATE TABLE kladr_abbr;

-- заполняем преобразованными из DBF данными
INSERT INTO kladr_abbr(code, lvl, name)
  SELECT
    "SCNAME",
    "KOD_T_ST"::smallint,
    "SOCRNAME"
  FROM
    "SOCRBASE.DBF";

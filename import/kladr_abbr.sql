--// КЛАДР : сокращения

DO $$ begin raise notice '--- Processing kladr_abbr ---'; end; $$;

-- создаем временную таблицу с импортируемыми данными КЛАДР'а
CREATE TEMPORARY TABLE _kladr_abbr(
	  LIKE kladr_abbr INCLUDING INDEXES
);

-- заполняем преобразованными из DBF данными
INSERT INTO _kladr_abbr(code, lvl, name)
  SELECT
    "SCNAME",
    "KOD_T_ST"::smallint,
    "SOCRNAME"
  FROM
    "SOCRBASE.DBF";

-- удаляем отсутствующие
DELETE FROM
  kladr_abbr T
USING
  kladr_abbr X LEFT JOIN
  _kladr_abbr Y
    USING(code, lvl)
WHERE
  (T.code, T.lvl) = (X.code, X.lvl) AND
  Y IS NULL;

-- обновляем оставшиеся
UPDATE
  kladr_abbr kl
SET
  name = kli.name
FROM
  _kladr_abbr kli
WHERE
  (kl.code, kl.lvl) = (kli.code, kli.lvl) AND
  (
	    kl.name
	  ) IS DISTINCT FROM
	  (
		    kli.name
		  );

		-- очищаем совпадающие
DELETE FROM
  _kladr_abbr kli
USING
  kladr_abbr kl
WHERE
  (kli.code, kli.lvl) = (kl.code, kl.lvl);

-- вставляем оставшиеся
INSERT INTO kladr_abbr
  SELECT
    *
  FROM
    _kladr_abbr;

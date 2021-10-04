#!/bin/sh

. `dirname "$0"`/app.conf

echo "`date '+%F %T'` ==== Connecting to DB : $pghost:$pgport:$pgbase:$pguser"
# тестирование подключения к БД
psql -t -c 'SELECT 1' -h $pghost -p $pgport -U $pguser -w $pgbase 1>/dev/null 2>/dev/null
rv="$?"
if [ "$rv" != "0" ]; then
  echo "$pghost:$pgport:$pgbase:$pguser:$pgpass" >>~/.pgpass
  chmod 0600 ~/.pgpass
  psql -t -c 'SELECT 1' -h $pghost -p $pgport -U $pguser -w $pgbase 1>/dev/null 2>/dev/null
  rv="$?"
fi

if [ "$rv" != "0" ]; then
  echo "DB not connected : $pghost:$pgport:$pgbase:$pguser"
  exit 1
fi


# инициализация каталога _dbf
#_dbf=`mktemp -d`

mkdir ./dbf && chmod 777 ./dbf
_dbf=`readlink -f ./dbf`
rm -rf ${_dbf} 2>/dev/null
mkdir ${_dbf} 2>/dev/null
touch ${_dbf}/.sql
dir=`dirname "$0"`
dir=`readlink -f $dir`


## импорт базы КЛАДР'а в _dbf/.sql
# защита от автоотключения по таймауту
echo "SET statement_timeout = 0;" >>${_dbf}/.sql
# включаем WIN-кодировку
echo "SET client_encoding = 'WIN1251';" >>${_dbf}/.sql
# включаем application_name для мониторинга активного процесса
echo "SET application_name = 'kladr : import [`hostname`]';" >>${_dbf}/.sql
# включаем "последовательные" транзакции
echo "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;" >>${_dbf}/.sql
# блокируем эксклюзивно upd-таблицу для "замораживания" параллельных операций
echo "LOCK TABLE kladr_upd IN EXCLUSIVE MODE NOWAIT;" >>${_dbf}/.sql
# сбрасываем информацию в таблицу протокола проверок
echo "DO \$\$ begin raise notice '--- do insert kladr_chk ---'; end; \$\$;" >>${_dbf}/.sql
echo "INSERT INTO kladr_chk(hostname) VALUES('`hostname`');" >>${_dbf}/.sql

# инициализация временного каталога импорта
#tmp=`mktemp -d`
mkdir ./tmp && chmod 777 ./tmp
tmp=`readlink -f ./tmp`
cd $tmp


echo "`date '+%F %T'` ==== Downloading : $source"
# загрузка базы КЛАДР'а с ограничением по скорости или без
# wget -S $source --limit-rate=8k 2>.hdr
cp ../source_dbf/Base.7z ./
cp ../source_dbf/.hdr ./
# wget -S $source 2>.hdr


echo "`date '+%F %T'` ==== Comparing 'Last-Modified'"
rc=`cat .hdr | egrep 'HTTP/[0-9]\.[0-9] [0-9]{3}' | sed -e 's/^[ ]*HTTP\/[0-9]\.[0-9][ ]*\([0-9]*\).*$/\1/i' | egrep -v '301' | head -1`
lm=`cat .hdr | egrep 'Last-Modified' | sed -e 's/^[ ]*Last-Modified:[ ]*//i' | head -1`
echo "  -- HTTP code : $rc"
echo "  -- HTTP 'Last-Modified' : $lm"
pglm=`psql -h $pghost -p $pgport -U $pguser -w -t -c 'SELECT lm FROM kladr_upd ORDER BY id DESC LIMIT 1' $pgbase | sed -e 's/^[ ]*//i'`
echo "  -- PGDB 'Last-Modified' : $pglm"

if [ "$rc" = "200" ] && [ "$lm" != "" ] && [ "$lm" != "$pglm" ]; then
  # распаковка базы
  echo "`date '+%F %T'` ==== Unpacking 7z"
  p7zip -d Base.7z 1>/dev/null 2>/dev/null
  cp $tmp/* ${_dbf}
  cd $dir

  echo "`date '+%F %T'` ==== Processing DBF"
  # обработка всех .DBF
  for dbf in `find ${_dbf} -maxdepth 1 -iname '*.DBF'`; do
    dbfn=`basename $dbf | tr '[:lower:]' '[:upper:]'`
    # преобразование заголовков
    echo "  -- DBF : $dbfn"
    echo "    -- header"
    # получаем структуру полей DBF | DOS2WIN | берем только описания полей (skip 2 строки) | оставляем только их имена
    fld=`dbview -b -t -e -o -r $dbf | recode CP866..CP1251 | tail -n+2 | xargs -l | egrep -io "^[a-z0-9_]+"`
    echo "CREATE TEMPORARY TABLE \"$dbfn\"(" >>${_dbf}/.sql
    fl="0"
    for i in ${fld}; do
      [ "$fl" = "1" ] && echo ',' >>${_dbf}/.sql
      echo -n "  \"$i\"\n    varchar" >>${_dbf}/.sql
      fl="1"
    done
    echo ");" >>${_dbf}/.sql
    # преобразование данных
    echo "    -- data"
    echo "COPY \"$dbfn\"(" >>${_dbf}/.sql
    fl="0"
    for i in ${fld}; do
      [ "$fl" = "1" ] && echo ',' >>${_dbf}/.sql
      echo -n "  \"$i\"" >>${_dbf}/.sql
      fl="1"
    done
    echo ") FROM stdin;" >>${_dbf}/.sql
    # получаем данные DBF, разделенные '~' | склеиваем "висящие" строки ([\t\r\n] в теле поля данных) | DOS2WIN | убираем все '\t' | убираем концевые ';' | заменяем ';'->'\t'
    dbview -d~ -b -t $dbf | sed -e :a -e '/[\r\t]$/N; s/[\r\t]\n//g; ta' | recode CP866..CP1251 | sed -e 's/\t//g; s/~\r//g; s/~,/,/g; s/~/\t/g' >>${_dbf}/.sql
    echo "\\." >>${_dbf}/.sql
  done

  # интеграция процедуры обновления базы - последовательное подключение всех sql-файлов импорта
  ls ${dir}/import/*.sql | xargs -l readlink -f | xargs -l -I{} cat {} >>${_dbf}/.sql
  # вставка метки обновления
  echo "DO \$\$ begin raise notice '--- do insert kladr_upd ---'; end; \$\$;" >>${_dbf}/.sql
  echo "INSERT INTO kladr_upd(lm, hostname) VALUES('$lm', '`hostname`');" >>${_dbf}/.sql
fi
echo "COMMIT;" >>${_dbf}/.sql

cd $dir
rm -rf $tmp

echo "`date '+%F %T'` ==== Processing SQL"
psql -h $pghost -p $pgport -U $pguser -w -f ${_dbf}/.sql $pgbase
rv="$?"

if [ "$rv" = "0" ]; then
  rm -rf ${_dbf}/ 2>/dev/null
fi
echo "`date '+%F %T'` ==== Exit : $rv"

exit "$rv"

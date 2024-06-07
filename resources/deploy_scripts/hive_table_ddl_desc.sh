#!/bin/bash
. ${HOME}/.bash_profile
. /apps/config/common.ini

HIVE_DDL_FILE="HiveTableDDL.txt"

rm -f tableNames.txt
rm -f ${HIVE_DDL_FILE}

database=$1

kint -kt ${KEYTAB_FILE} ${CONNECT_ID}@${CONNECT_SERVER}

${beeline_conn} --showHeader=false --outputformat=tsv2 -e "use ${database}; show tables;" > tableNames.txt

wait

cat tableNames.txt | while read table_name
  do
    echo "${table_name}:" >> ${HIVE_DDL_FILE}
    ${beeline_conn} --showHeader=false --outputformat=tsv2 -e "use ${database}; describe formatted ${table_name};" >> ${HIVE_DDL_FILE}
    echo -e "\n" >> ${HIVE_DDL_FILE}
  done

rm -f tableNames.txt

echo  "Table DDL generated!"
#!/bin/bash
. ${HOME}/.bash_profile
. /apps/config/common.ini

script_path=${1}

echo "HiveQL execution begins"
echo "HiveQL script path : ${script_path}"

if [ -f ${script_path} ]; then
  kint -kt ${KEYTAB_FILE} ${CONNECT_ID}@${CONNECT_SERVER}
  ${beeline_conn} -f ${script_path}

  if [$? -ne 0]; then
    echo "Error while running SQL queries!"
    exit 2
  fi

else
    echo  "HiveQL file doesn't exists at path ${script_path}"
    exit 1
fi

echo "HiveQL execution completed successfully"
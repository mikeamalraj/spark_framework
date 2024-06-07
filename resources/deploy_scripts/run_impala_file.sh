#!/bin/bash
. ${HOME}/.bash_profile
. /apps/config/common.ini

script_path=${1}

echo "Impala execution begins"
echo "Impala script path : ${script_path}"

if [ -f ${script_path} ]; then
  kint -kt ${KEYTAB_FILE} ${CONNECT_ID}@${CONNECT_SERVER}
  ${impala_conn} -f ${script_path}

  if [$? -ne 0]; then
    echo "Error while running SQL queries!"
    exit 2
  fi

else
    echo  "Impala file doesn't exists at path ${script_path}"
    exit 1
fi

echo "Impala execution completed successfully"
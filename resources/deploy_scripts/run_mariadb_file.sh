#!/bin/bash
. ${HOME}/.bash_profile
. /apps/config/common.ini

script_path=${1}

echo "SQL execution begins"
echo "SQL script path : ${script_path}"

if [ -f ${script_path} ]; then

  ${md_config_conn} < ${script_path}

  if [$? -ne 0]; then
    echo "Error while running SQL queries!"
    exit 2
  fi

else
    echo  "SQL file doesn't exists at path ${script_path}"
    exit 1
fi

echo "SQL execution completed successfully"
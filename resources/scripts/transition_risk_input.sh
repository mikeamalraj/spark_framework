#!/bin/bash
. ${HOME}/.bash_profile
. /apps/config/common.ini

export PYSPARK_PYTHON=/opt/apcconda3/bin/python3

cd /apps/stre/cstm/source_code

spark3-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 8g \
--executor-memory 15g \
--num-executors 4 \
--num-cores 4 \
--queue root.queue_name \
--conf spark.driver.memoryOverhead=5120 \
--conf spark.executor.memoryOverhead=4096 \
--conf spark.yarn.keytab=${KEYTAB_FILE} \
--conf spark.yarn.principal=${CONNECT_ID}@${CONNECT_SERVER} \
--properties-file /apps/stre/cstm/config/spark-runtime-conf.properties \
--archieves /apps/stre/cstm/source_code/resources.zip#resources \
--files /apps/stres/cstm/config/runtime-config.json#runtime-config.json \
--py-files /apps/stre/cstm/source_code/src.zip \
--jars /apps/stre/cstm/lib/mysql-connector-j-8.0.32.jar \
/apps/stre/cstm/source_code/src/compute/service.py ${ARG1} ${ARG2} ${ARG3} --runtime_conf runtime-config.json

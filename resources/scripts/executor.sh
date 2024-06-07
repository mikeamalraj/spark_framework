#!/bin/bash
. ${HOME}/.bash_profile
. /apps/config/common.ini

tsnow=`date +%Y%m%d%H%M%S`
SPARK_JOB_NAME="${ARG1}_${ARG2}_${ARG3}"
SPARK_LOG_FILE="${LOG_ROOT}/spark_${tsnow}.log"
PARAMETERS="${PARAM1} ${PARAM2} ${PARAM3}"

SPARK_CMD=$( echo " \
spark3-submit \
--name ${SPARK_JOB_NAME} \
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
/apps/stre/cstm/source_code/src/compute/service.py ${PARAMETERS} --runtime_conf runtime-config.json \
")

eval "${SPARK_CMD} --stress_model '${model}' >> ${SPARK_LOG_FILE} 2>&1"

return_code=$?


#!/bin/bash
. ${HOME}/.bash_profile
. /apps/config/common.ini

version_key=$(date +"%Y%m%d%H%M%S%6N")

stress_models=("Power" "OG" "AV" "MM")

child=();

for ((i = 0; i < ${#stress_models[@]}; i++)); do
  echo "${stress_models[$i]}"
  /apps/stre/cstm/transition_risk_input.sh ${ARG1} ${ARG2} "${stress_models[$i]}" > /dev/null 2>&1 &
  child+=($!);
done

final_status=0;

for i in "${child[@]}"; do
    if ! wait "$i"; then final_status=1; fi
done

if [ $final_status -ne 0 ]; then
    echo "Error while running transition risk input"
fi

echo "Transition risk input tables have been refreshed!"
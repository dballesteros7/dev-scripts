#!/bin/bash

MONTH=$1
for file in $(ls workflowCatalog-${MONTH}/* | grep -v done | grep -v lock | grep -v submit)
do
    if [[ ! -e ${file}.done  && ! -e ${file}.lock ]]
    then
        echo "Not archived yet $file"
    elif [[ ! -e ${file}.done && -e ${file}.lock ]]
    then
        echo "Archival job in progress $file"
    elif [[ -e ${file}.done ]]
    then
        echo "Completed $file"
    fi
done

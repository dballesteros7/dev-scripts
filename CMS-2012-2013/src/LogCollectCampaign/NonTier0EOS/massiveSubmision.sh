#!/bin/bash

MONTH=$1

for file in $(ls workflowCatalog-${MONTH}/submit*)
do
    workflowFile=`echo $file | sed 's?'workflowCatalog-${MONTH}/'submit-??g' | sed 's?sh?txt?g'`
    if [[ -e workflowCatalog-${MONTH}/${workflowFile}.done || -e workflowCatalog-${MONTH}/${workflowFile}.lock ]]
    then
        continue
    fi
    bjobsOutput=`bjobs -w -g /vocms15/logCollect`
    nJobs=`bjobs -w -g /vocms15/logCollect | wc -l`
    rc=$?

    if [[ "$rc" != "0" ]]
    then
        echo Error getting the bjobs, skipping submission
        exit 0
    fi
    
    if [[ "x$bjobsOutput" != "x" ]]
    then    
        if [[ $nJobs -ge 100 ]]
        then
            echo "Too many jobs already"
            exit 0
        fi
    fi

    workflow=`echo $workflowFile | awk -F '.' '{print $1}'`
    bsub -q cmst0 -g /vocms15/logCollect -J LogCollect-$workflow -oo /afs/cern.ch/work/c/cmsprod/logs/logCollect-lsf/LogCollect-${workflow}-%J.log < $file
    touch workflowCatalog-${MONTH}/${workflowFile}.lock

done


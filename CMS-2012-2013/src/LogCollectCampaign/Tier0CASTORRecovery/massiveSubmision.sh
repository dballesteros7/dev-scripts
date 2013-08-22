#!/bin/bash

MONTH=$1

for file in $(ls runCatalogs-${MONTH}/submit*)
do
    runFile=`echo $file | awk -F '-' '{print $3}' | awk -F '.' '{print $1".txt"}'`
    if [[ -e runCatalogs-${MONTH}/${runFile}.done || -e runCatalogs-${MONTH}/${runFile}.lock ]]
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

    runNumber=`echo $runFile | awk -F '.' '{print $1}' | sed 's/Run//g'`
    bsub -q 1nw -g /vocms15/logCollect -J LogCollect-Run$runNumber -oo /afs/cern.ch/work/c/cmsprod/logs/logCollect-lsf/LogCollect-Job-Run${runNumber}-%J.log < $file
    touch runCatalogs-${MONTH}/${runFile}.lock

done


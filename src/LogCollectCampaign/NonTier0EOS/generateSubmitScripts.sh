#!/bin/bash

MONTH=$1

for workflow in $(cat workflowsToProcess.txt);
do 
    sed 's/REPLACEME/'${workflow}'.txt/g' submit.sh | sed 's/WORKFLOW/'$workflow'/g' | sed 's/MONTH/'${MONTH}'/g' > workflowCatalog-${MONTH}/submit-${workflow}.sh
done

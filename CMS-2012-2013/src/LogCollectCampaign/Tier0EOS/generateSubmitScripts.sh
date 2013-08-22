#!/bin/bash

MONTH=$1

for run in $(cat runToProcess.txt); do sed 's/REPLACEME/Run'${run}'.txt/g' submit.sh | sed 's/RUNNUMBER/'$run'/g' | sed 's/MONTH/'${MONTH}'/g' > runCatalogs-${MONTH}/submit-Run${run}.sh; done

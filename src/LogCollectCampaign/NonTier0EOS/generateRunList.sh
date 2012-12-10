#!/bin/bash

MONTH=$1
for file in $(ls workflowCatalog-${MONTH}/* | egrep "^workflowCatalog-${MONTH}/.*\.txt$")
do
    echo $file | awk -F '/' '{print $2}' | awk -F '.' '{print $1}'
done

#!/bin/bash

MONTH=$1
for file in $(ls runCatalogs-${MONTH}/* | egrep "^runCatalogs-${MONTH}/Run[0-9]{6}\.txt$")
do
    echo $file | awk -F '/' '{print $2}' | awk -F '.' '{print $1}' | sed 's/Run//g'
done

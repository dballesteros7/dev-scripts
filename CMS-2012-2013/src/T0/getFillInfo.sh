#!/bin/bash

FILLINFO=$1
DATES=$2
FILL=$(echo FILLINFO | awk -F '.' '{print $1}')
for run in $(cat $FILLINFO)
do
	for date in $(cat $DATES)
	do
		grep "'RUNNUMBER' => '${run}'" TransferTest.log.${date}* >> Run${run}.txt
	done
	sed -i 's?t0streamer?streamer?g' Run${run}.txt
	egrep "/castor/cern.ch/cms/store/streamer/Data/[A-Za-z0-9]*/0{3}(/[0-9]{3}){2}/([A-Za-z0-9]*\.){7}dat" -o  Run${run}.txt  | sort | uniq > Run${run}.files
	echo Run${run}.txt
    echo Run${run}.file
done

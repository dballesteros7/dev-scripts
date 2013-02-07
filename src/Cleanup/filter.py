'''
Created on Feb 7, 2013

@author: dballest
'''

import sys

from optparse import OptionParser

def main():
    myOptParser = OptionParser()
    myOptParser.add_option("-f", "--firstrun", dest = "firstrun")
    myOptParser.add_option("-l", "--lastrun", dest = "lastrun")
    myOptParser.add_option("-s", "--excludedstreams", dest = "excludedstreams")
    (options, args) = myOptParser.parse_args()

    if len(args) < 1:
        print "Error: Not enough arguments"
    inputFileList = args[0]
    outputFileList = '%s.filtered' % args[0]
    rejectsFileList = '%s.rejects' % args[0]
    firstRun = options.firstrun
    lastRun = options.lastrun
    exStreams = []
    if options.excludedstreams:
        exStreams = options.excludedstreams.split(',')

    fileHandle = open(inputFileList, 'r')
    outFileHandle = open(outputFileList, 'w')
    rejFileHandle = open(rejectsFileList, 'w')
    for line in fileHandle:
        tokens = line.split('/')
        if len(tokens) != 12:
            # Don't know how to handle, write to rejects
            rejFileHandle.write(line)
            continue
        stream = tokens[7]
        if stream in exStreams:
            continue
        runNumber = '%s%s%s' % (tokens[8], tokens[9], tokens[10])
        if int(runNumber) < int(firstRun):
            continue
        if int(runNumber) > int(lastRun):
            continue
        outFileHandle.write(line)
    fileHandle.close()
    outFileHandle.close()
    rejFileHandle.close()

if __name__ == '__main__':
    sys.exit(main())

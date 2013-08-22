'''
Created on Jan 23, 2013

@author: dballest
'''

import sys
import shlex
import tempfile
import os
from subprocess import call

def main():
    dataset = sys.argv[1]
    filesCall = 'python DASCLI.py --query="file dataset=%s" --limit=0' % dataset
    tempFileH, tempFilePath = tempfile.mkstemp()
    call(shlex.split(filesCall), stdout = tempFileH)
    fileList = open(tempFilePath)
    fileList.seek(0)
    runStructure = {}
    for fileName in fileList:
        runCall = 'python DASCLI.py --query="run file=%s" --limit=0' % fileName[:-1]
        tempFileH, tempFilePath = tempfile.mkstemp()
        call(shlex.split(runCall), stdout = tempFileH)
        runList = open(tempFilePath)
        runList.seek(0)
        for line in runList:
            run = int(line[:-1])
            if run not in runStructure:
                runStructure[run] = 0
        runList.close()
        lumiCall = 'python DASCLI.py --query="lumi file=%s" --limit=0' % fileName[:-1]
        tempFileH, tempFilePath = tempfile.mkstemp()
        call(shlex.split(lumiCall), stdout = tempFileH)
        lumiList = open(tempFilePath)
        lumiList.seek(0)
        for line in lumiList:
            runStructure[run] += 1
        lumiList.close()
        
    fileList.close()
    
    print runStructure
    

if __name__ == '__main__':
    sys.exit(main())
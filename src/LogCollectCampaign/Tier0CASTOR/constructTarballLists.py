'''
Created on Nov 18, 2012

@author: dballest
'''

import sys
import subprocess
import shlex
import os
import traceback
import re
from tempfile import mkstemp


def runCommand(command):
    args = shlex.split(command)
    (stdOutFd, stdOutPath) = mkstemp()
    (stdErrFd, stdErrPath) = mkstemp()
    subprocess.check_call(args, stdin = None, stdout = stdOutFd, stderr = stdErrFd)
    stdOut = os.fdopen(stdOutFd)
    stdErr = os.fdopen(stdErrFd)
    stdErr.close()
    stdOut.seek(0)
    return (stdOut, stdOutPath, stdErrPath)
    
def main():
    month = sys.argv[1]
    inputFileListPath = sys.argv[2]
    if not os.path.exists('%s/runCatalogs-%s' % (os.getcwd(), month)):
        os.mkdir('%s/runCatalogs-%s' % (os.getcwd(), month))
    try:
        inputFileList = open(inputFileListPath, 'r')
        runPromptRecoRegex = r'.*/2011/%s/[0-9]{1,2}/.*PromptReco-Run([0-9]{6}).*' % month
        runs = {}
        for line in inputFileList:
            runMatch = re.match(runPromptRecoRegex, line)
            if runMatch:
                run = int(runMatch.groups()[0])
                if run not in runs:
                    runHandle = open('%s/runCatalogs-%s/Run%s.txt' % (os.getcwd(), month, run), 'a')
                    runs[run] = runHandle
                runs[run].write(line)
    except Exception, ex:
        print traceback.format_exc()
        print str(ex)
    finally:
        for run in runs:
            runs[run].close()
        inputFileList.close()
    return 0

if __name__ == '__main__':
    sys.exit(main())

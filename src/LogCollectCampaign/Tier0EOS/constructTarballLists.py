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
    if not os.path.exists('%s/runCatalogs-%s' % (os.getcwd(), month)):
        os.mkdir('%s/runCatalogs-%s' % (os.getcwd(), month))
    try:
        listMonthDirectoriesCommand = '/afs/cern.ch/project/eos/installation/0.2.5/bin/eos.select find -d /store/unmerged/logs/prod/2012/%s' % month
        listFilesInDirectory = '/afs/cern.ch/project/eos/installation/0.2.5/bin/eos.select find -f %s'
        runRegex = r'.*Run([0-9]{6}).*'
        (stdOut, stdOutPath, stdErrPath) = runCommand(listMonthDirectoriesCommand)
        runs = {}
        for line in stdOut:
            tokens = line.split()
            nFiles = int(tokens[2].split('=')[1])
            nDirs  = int(tokens[1].split('=')[1])
            if nFiles > 0:
                try:
                    command = listFilesInDirectory % tokens[0]
                    (partialStdOut, partialStdOutPath, partialStdErrPath) = runCommand(command)
                    for filePath in partialStdOut:
                        runMatch = re.match(runRegex, filePath)
                        if runMatch:
                            run = int(runMatch.groups()[0])
                            if run not in runs:
                                runHandle = open('%s/runCatalogs-%s/Run%s.txt' % (os.getcwd(), month, run), 'a')
                                runs[run] = runHandle
                            runs[run].write(filePath)
                        else:
                            print filePath
                except Exception, ex:
                    print traceback.format_exc()
                    print str(ex)
                finally:
                    partialStdOut.close()
                    os.unlink(partialStdOutPath)
                    os.unlink(partialStdErrPath)
    except Exception, ex:
        print traceback.format_exc()
        print str(ex)
    finally:
        for run in runs:
            runs[run].close()
        stdOut.close()
        os.unlink(stdOutPath)
        os.unlink(stdErrPath)

    
    return 0

if __name__ == '__main__':
    sys.exit(main())

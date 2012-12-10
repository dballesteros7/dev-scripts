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
    try:
        listAllFilesCommand = '/afs/cern.ch/project/eos/installation/0.2.5/bin/eos.select find -f /store/unmerged/logs/prod/2012/%s' % month
        runRegex = r'.*Run([0-9]{6}).*'
        (stdOut, stdOutPath, stdErrPath) = runCommand(listAllFilesCommand)
        workflows = {}
        if not os.path.exists('%s/workflowCatalog-%s/' % (os.getcwd(), month)):
            os.mkdir('%s/workflowCatalog-%s/' % (os.getcwd(), month))
        for line in stdOut:
            tokens = line.split('/')
            workflow = tokens[10]
            if re.match(runRegex, workflow):
                continue
            if workflow not in workflows:
                workflowHandle = open('%s/workflowCatalog-%s/%s.txt' % (os.getcwd(), month, workflow), 'a')
                workflows[workflow] = workflowHandle
            workflows[workflow].write(line)
    except Exception, ex:
        print traceback.format_exc()
        print str(ex)
    finally:
        for workflow in workflows:
            workflows[workflow].close()
        stdOut.close()
        os.unlink(stdOutPath)
        os.unlink(stdErrPath)

    return 0

if __name__ == '__main__':
    sys.exit(main())

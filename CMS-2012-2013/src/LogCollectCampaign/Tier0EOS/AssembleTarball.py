'''
Created on Nov 19, 2012

@author: dballest
'''

import sys
import os
import subprocess
import traceback
import shlex
import tarfile
import uuid
import shutil
import time
from tempfile import mkstemp

FILE_LIMIT = 1000

def doLogCollection(runNumber, idx, stagedFiles, month):
    try:
        originalCWD = os.getcwd()
        os.chdir(os.path.join(os.getcwd(),'LogArchs'))
        unique = str(uuid.uuid1())
        logCollectTarball = 'Run%d-LogCollect-%s-%d.tar' % (runNumber, unique, idx)
        print "Tarring the logs now"
        tarball = tarfile.open(logCollectTarball, 'w')
        for stagedFile in stagedFiles:
            tarball.add(stagedFile)
        tarball.close()
        print "Tar completed, filesize: %s" % str(os.stat(logCollectTarball).st_size)
        print "Starting stage out of the tarball to root://castorcms//castor/cern.ch/cms/store/logs/prod/2012/%s/prodAgent/%s" % (month, logCollectTarball)
        command = 'xrdcp -s %s root://castorcms//castor/cern.ch/cms/store/logs/prod/2012/%s/prodAgent/%s' % (logCollectTarball, month, logCollectTarball)
        args = shlex.split(command)
        (stdOutFd2, _) = mkstemp(dir = os.getcwd())
        (stdErrFd2, _) = mkstemp(dir = os.getcwd())
        rc = subprocess.call(args, stdin = None, stdout = stdOutFd2, stderr = stdErrFd2)
        if rc == 0:
            print "Staged tarball to: root://castorcms//castor/cern.ch/cms/store/logs/prod/2012/%s/prodAgent/%s" %(month, logCollectTarball)
            return logCollectTarball
    except Exception, ex:
        print str(ex)
        print traceback.format_exc()
        return None
    finally:
        os.chdir(originalCWD)
        os.close(stdOutFd2)
        os.close(stdErrFd2)
        shutil.rmtree(os.path.join(os.getcwd(), 'LogArchs'))
        os.mkdir(os.path.join(os.getcwd(), 'LogArchs'))

def main():
    runNumber = int(sys.argv[1])
    month = sys.argv[2].zfill(2)
    print "Starting logCollect script..."
    print time.strftime('%m-%d-%Y %H:%M:%S')
    print "Run number is: %d" % runNumber
    print "Month is: %s" % month
    try:
        inputFileList = open(os.path.join(os.getcwd(), 'Run%d.txt' % runNumber))
        print "Using the following filelist: %s" % os.path.join(os.getcwd(), 'Run%d.txt' % runNumber)
        stagedFiles = []
        stagedOriginalPaths = []
        archivedFiles = []
        archivedTarballs = []
        idx = 0
        os.mkdir(os.path.join(os.getcwd(), 'LogArchs'))
        for logArch in inputFileList:
            try:
                logArch = logArch[:-1]
                print "Staging: root://eoscms//%s" % logArch
                command = 'xrdcp -s root://eoscms//%s ./LogArchs/' % logArch
                args = shlex.split(command)
                (stdOutFd, stdOutPath) = mkstemp(dir = os.getcwd())
                (stdErrFd, stdErrPath) = mkstemp(dir = os.getcwd())
                rc = subprocess.call(args, stdin = None, stdout = stdOutFd, stderr = stdErrFd)
                if rc == 0:
                    stagedFiles.append(logArch.split('/')[-1])
                    stagedOriginalPaths.append(logArch)
                    print "StageIn successful"
            except Exception, ex:
                print "Stage in failed"
                print str(ex)
                print traceback.format_exc()
            finally:
                os.close(stdOutFd)
                os.close(stdErrFd)
                os.unlink(stdOutPath)
                os.unlink(stdErrPath)
            if len(stagedFiles) > FILE_LIMIT:
                print "Reached file limit"
                collectedTarball = doLogCollection(runNumber, idx, stagedFiles, month)
                if collectedTarball:
                    archivedTarballs.append(collectedTarball)
                    archivedFiles.extend(stagedOriginalPaths)
                stagedFiles = []
                stagedOriginalPaths = []
                idx += 1
        collectedTarball = doLogCollection(runNumber, idx, stagedFiles, month)
        if collectedTarball:
            archivedTarballs.append(collectedTarball)
            archivedFiles.extend(stagedOriginalPaths)
        
        outputFile = open(os.path.join(os.getcwd(), 'Run%d.txt.done' % runNumber), 'w')
        for tarball in archivedTarballs:
            outputFile.write('/castor/cern.ch/cms/store/logs/prod/2012/%s/prodAgent/%s\n' % (month, tarball))
        outputFile.close()
    except Exception, ex:
        print str(ex)
        print traceback.format_exc()
    finally:
        inputFileList.close()
        shutil.rmtree(os.path.join(os.getcwd(), 'LogArchs'))

    return 0
    
if __name__ == '__main__':
    sys.exit(main())
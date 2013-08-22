'''
Created on Feb 4, 2013

@author: dballest
'''

from Queue import Queue
from subprocess import Popen
from threading import Thread
import shlex
import sys
import time
import tempfile
import os
import traceback


def deleteFile(path):
    out, _ = tempfile.mkstemp()
    err, _ = tempfile.mkstemp()
    try:
        namespaceRemove = 'nsrm %s' % path
        stagerRemove = 'stager_rm -S cmsprodlogs -M %s' % path
        namespaceArgs = shlex.split(namespaceRemove)
        stagerArgs = shlex.split(stagerRemove)
        proc = Popen(stagerArgs, stdout = out, stderr = err)
        startTime = time.time()
        while proc.poll() is None:
            time.sleep(0.5)
            currentTime = time.time()
            if currentTime - startTime > 15:
                proc.terminate()
                proc.wait()
                break
        proc = Popen(namespaceArgs, stdout = out, stderr = err)
        retCode = 1
        startTime = time.time()
        while proc.poll() is None:
            time.sleep(0.5)
            currentTime = time.time()
            if currentTime - startTime > 30:
                proc.terminate()
                retCode = proc.wait()
                break
        else:
            retCode = proc.wait()
        return retCode
    except:
        traceback.print_exc()
        return 1
    finally:
        os.close(out)
        os.close(err)

def broom():
    while True:
        logArch = toDelete.get()
        try:
            if logArch == "STOP":
                toDelete.task_done()
                break
            retCode = deleteFile(logArch)
            if retCode == 0:
                success.put(logArch)
            else:
                failed.put(logArch)
            toDelete.task_done()
        except:
            failed.put(logArch)
            toDelete.task_done()
    return

def bookkeeper(queueToUse):
    filePath = queueToUse.get()
    queueToUse.task_done()
    fileHandle = open(filePath, 'w')
    counter = 0
    while True:
        logArch = queueToUse.get()
        if logArch == "STOP":
            queueToUse.task_done()
            break
        fileHandle.write('%s\n' % logArch)
        queueToUse.task_done()
        if counter > 10:
            counter = 0
            fileHandle.flush()
        counter += 1
    fileHandle.close()
    return

def beanCounter():
    time.sleep(60)
    while not toDelete.empty():
        size = toDelete.qsize()
        totalSize = 1000.0
        percentage = size * 100.0 / totalSize
        print "Progress: %s" % percentage
        time.sleep(30)
    return

toDelete = Queue()
success = Queue()
failed = Queue()
n = 10

def main():
    inputFile = sys.argv[1]
    # Start the workers
    for _ in range(n):
        t = Thread(target = broom)
        t.daemon = True
        t.start()

    success.put("%s.success.log" % inputFile)
    failed.put("%s.failed.log" % inputFile)

    t = Thread(target = bookkeeper, args = (success,))
    t.daemon = True
    t.start()

    t = Thread(target = bookkeeper, args = (failed,))
    t.daemon = True
    t.start()

    t = Thread(target = beanCounter)
    t.daemon = True
    t.start()

    fileHandle = open(inputFile, 'r')
    for line in fileHandle:
        toDelete.put(line.strip('\n'))
    fileHandle.close()
    for _ in range(n):
        toDelete.put("STOP")

    toDelete.join()
    success.put("STOP")
    failed.put("STOP")
    success.join()
    failed.join()

    return 0

if __name__ == '__main__':
    sys.exit(main())

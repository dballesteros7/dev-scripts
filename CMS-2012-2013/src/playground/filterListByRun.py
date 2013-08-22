'''
Created on Nov 18, 2012

@author: dballest
'''

import sys

def main():
    maxRunToDelete = int(sys.argv[1])
    fileListPath = sys.argv[2]
    outputFile = sys.argv[3]
    runsToDelete = set()
    try:
        inputHandle = open(fileListPath)
        outputFileHandle = open(outputFile, 'w')
        currentDir = ""
        for line in inputHandle:
            if line.endswith(":\n"):
                # Then it is a directory
                currentDir = line[:-2]
            else:
                # It is a subdirectory or a file
                if line.endswith(".dat\n"):
                    # It's a streamer, find the run number
                    runNumber = int(''.join(currentDir[-8:].split('/')))
                    if runNumber < maxRunToDelete:
                        runsToDelete.add(runNumber)
                        outputFileHandle.write('%s/%s' % (currentDir, line))
        print "Runs in the list:"
        for run in sorted(list(runsToDelete)):
            print "Run: %d" % run
    except Exception, ex:
        print str(ex)
    finally:
        inputHandle.close()

if __name__ == '__main__':
    sys.exit(main())
'''
Created on Feb 11, 2013

@author: dballest
'''

import sys
import os
from socket import gethostname
from optparse import OptionParser

from WMCore.Database.CMSCouch import Database

submitBlob = """#!/bin/bash
# Initialize environment

. $VO_CMS_SW_DIR/cmsset_default.sh
scramv1 project %s
cd %s
eval `scramv1 runtime -sh`

# Transfer input files
%s
# Run the process, assume that one of the input files was a PSet.py
cmsRun -e PSet.py 1> cmsRun.stdout 2> cmsRun.stderr

# Transfer the output
%s
# Store the FWJR back
rfcp FrameworkJobReport.xml %s:%s/FrameworkJobReport.xml
exit 0
"""

def findParentJobs(jobId):
    # Connect to the Job and FWJR DBs
    jobDB = Database('wmagent_jobdump/jobs', 'http://dummy.cern.ch:5984')
    fwjrDB = Database('wmagent_jobdump/fwjrs', 'http://dummy.cern.ch:5984')

    # Get the document of the child job
    childJobDoc = jobDB.document(id = jobId)

    # Get the workflow and input files, transforms it into suitable keys [workflow, lfn]
    workflow = childJobDoc['workflow']
    inputLfns = [x['lfn'] for x in childJobDoc['inputfiles']]
    keys = [[workflow, x] for x in inputLfns]

    # Get the jobs that produced the input files for this job
    # Load the id and fwjr for these jobs since we have to re-run them
    result = fwjrDB.loadView('FWJRDump', 'jobsByOutputLFN', {}, keys)
    for entry in result['rows']:
        key = entry['key']
        jobId = entry['value']
        fwjrId = entry['id']
        result = fwjrDB.loadView('FWJRDump', 'logArchivesByJobID', {}, [[int(x) for x in fwjrId.split('-')]])
        logArch = result['rows'][0]['value']['lfn']

        # Check whether the logArch is in some LogCollect
        logCollectTarball = ''
        result = jobDB.loadView('JobDump', 'jobsByInputLFN', {}, [[workflow, logArch]])
        if result['rows']:
            logCollectJobId = result['rows'][0]['id']
            result = fwjrDB.loadView('FWJRDump', 'outputByJobID', {}, [int(logCollectJobId)])
            if result['rows']:
                logCollectTarball = result['rows'][0]['value']['lfn']
            else:
                print "WARNING: The logArchive for job %s was in a LogCollect job but not tarball was produced" % jobId

        # Print out the information
        print "Job %s produced %s, the logArch for it is %s in %s" % (jobId, key[1], logArch, logCollectTarball)

    return

def generateSubmitScript(filesToTransfer, cmsswRelease, desiredOutputs, outputPrefixPath):
    # Where are we running from?
    host = gethostname()

    # Transfer the necessary configuration files
    inTransferCommand = ""
    for filePath in filesToTransfer:
        inTransferCommand += "rfcp %s:%s .\n" % (host, filePath)

    # Primitive stage out
    outTransferCommand = ""
    for outputFile in desiredOutputs:
        if outputPrefixPath.startswith('/eos'):
            outTransferCommand += 'xrdcp -f -s "%s" "root://eoscms/%s/%s"' % (outputFile, outputPrefixPath, outputFile)

    # Input all in the template
    currentDir = os.getcwd()
    script = submitBlob % (cmsswRelease, cmsswRelease, inTransferCommand, outTransferCommand, host, currentDir)

    # Print it out
    print script

    return

def getFileInformation(workflow, lfn, outModule):

    # Connect to the FWJR DB
    fwjrDB = Database('wmagent_jobdump/fwjrs', 'http://dummy.cern.ch:5984')

    result = fwjrDB.loadView('FWJRDump', 'jobsByOutputLFN', {'include_docs' : True}, [[workflow, lfn]])
    if result['rows']:
        fwjrDoc = result['rows'][0]['doc']
        fwjrInfo = fwjrDoc['fwjr']
        for step in fwjrInfo['steps']:
            if step == 'cmsRun1':
                if outModule not in fwjrInfo['steps'][step]['output']:
                    print "WARNING: No output module %s in this job" % outModule
                    return
                outModuleInfo = fwjrInfo['steps'][step]['output'][outModule]
                for fileInfo in outModuleInfo:
                    if fileInfo['lfn'] == lfn:
                        print "File information, %s" % fileInfo['lfn']
                        print "Run/Lumis:"
                        for run in fileInfo['runs']:
                            print 'Run: %s, Lumi range: %s-%s' % (run, fileInfo['runs'][run][0], fileInfo['runs'][run][1])
                        print "Number of Events: %s" % fileInfo['events']
                        print "Filesize (bytes): %.1f" % (float(fileInfo['size']))
                        print "Adler32 Checksum: %s" % fileInfo['checksums']['adler32']
    else:
        print "WARNING: No file info in CouchDB"

    return

def getLogArchForJob(jobId, workflow):
    # Connect to the Job and FWJR DBs
    jobDB = Database('wmagent_jobdump/jobs', 'http://dummy.cern.ch:5984')
    fwjrDB = Database('wmagent_jobdump/fwjrs', 'http://dummy.cern.ch:5984')

    # Get the logArchives for the job
    result = fwjrDB.loadView('FWJRDump', 'logArchivesByJobID', {'startkey' : [int(jobId)], 'endkey' : [int(jobId), {}]})
    lastLogArch = sorted(result['rows'], key = lambda x: x['value']['retrycount'])[-1]['value']['lfn']

    # Get the logCollect job for the logArch, if any
    logCollectTarball = ''
    result = jobDB.loadView('JobDump', 'jobsByInputLFN', {}, [[workflow, lastLogArch]])
    if result['rows']:
        logCollectJobId = result['rows'][0]['id']
        result = fwjrDB.loadView('FWJRDump', 'outputByJobID', {}, [int(logCollectJobId)])
        if result['rows']:
            logCollectTarball = result['rows'][0]['value']['lfn']
        else:
            print "WARNING: The logArchive for job %s was in a LogCollect job but not tarball was produced" % jobId

    # Print out the information
    print "The logArch for job %s is %s in %s" % (jobId, lastLogArch, logCollectTarball)

    return

def main():
    optionParser = OptionParser()
    optionParser.add_option('-a', '--action', dest = 'action')
    optionParser.add_option('--jobId', dest = 'jobId')
    optionParser.add_option('--filesToTransfer', dest = 'filesToTransfer')
    optionParser.add_option('--cmsswRelease', dest = 'cmsswRelease')
    optionParser.add_option('--desiredOutputs', dest = 'desiredOutputs')
    optionParser.add_option('--outputPrefixPath', dest = 'outputPrefixPath')
    optionParser.add_option('--workflow', dest = 'workflow')
    optionParser.add_option('--lfn', dest = 'lfn')
    optionParser.add_option('--outputModule', dest = 'outputModule')
    options, _ = optionParser.parse_args()

    if options.action == 'FindParentJobs':
        findParentJobs(options.jobId)
    elif options.action == 'GenerateSubmitScript':
        generateSubmitScript(options.filesToTransfer.split(','), options.cmsswRelease,
                             options.desiredOutputs.split(','), options.outputPrefixPath)
    elif options.action == 'GetFileInformation':
        getFileInformation(options.workflow, options.lfn, options.outputModule)
    elif options.action == 'GetLogArchForJob':
        getLogArchForJob(options.jobId, options.workflow)
    else:
        print 'WARNING: No valid action supplied'

    return 0

if __name__ == '__main__':
    sys.exit(main())

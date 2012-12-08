'''
Created on Aug 20, 2012

@author: dballest
'''

import sys
import tarfile
import glob
import os
import tempfile
import re
import pickle

from optparse import OptionParser

from WMCore.FwkJobReport.Report import Report

def main():
    parser = OptionParser()
    parser.add_option("--archive", dest="archive", help="Archive to harvest")
    parser.add_option("--check-merged", dest="checkMerged", help="Tell the tool to harvest information about merge jobs",
                      default = False)
    (options, _) = parser.parse_args()
    
    pathPattern = os.path.join(options.archive, '*/*')
    filelist = glob.glob(pathPattern)   
    #print filelist
    
    totalEvents = 0
    totalInEvents = 0
    inFiles = set([])
    outFiles = set([])
    failedFWJRs = []
    totalProdJobs = 0
    noSites = 0
    missingFiles = 0
    for jobTarPath in filelist:
        tmpdir = tempfile.mkdtemp()

        tarFile = tarfile.open(jobTarPath)
        tarFile.extractall(path = tmpdir)
        tarFile.close()

        jobTar = jobTarPath.split('/')[-1]
        path = os.path.join(tmpdir, jobTar.split('.')[0])
        availableFiles = os.listdir(path)
        reports = filter(lambda x : re.match('Report\.[0-9]\.pkl', x), availableFiles)
        if reports:
            reports.sort(reverse = True)
            reportName = reports[0]
            fwjr = Report()
            fwjr.load(os.path.join(path, reportName))
            steps = fwjr.listSteps()
#            if fwjr.getTaskName() != '/spinoso_EXO-Summer12-01945_R1805_B169_20_LHE_120914_191121_2116/MonteCarloFromGEN':
#                continue
            totalProdJobs += 1
            if fwjr.getExitCode():
                failedFWJRs.append(fwjr)
                #print fwjr.data
#            print fwjr.getTaskName()
            if 'cmsRun1' in steps:
                outputFiles = fwjr.getAllFilesFromStep(step = 'cmsRun1')
                inputFiles = fwjr.getInputFilesFromStep(stepName = 'cmsRun1')
                inEvents = 0
                for inFile in inputFiles:
                    if inFile['input_type'] == 'primaryFiles':
                        inEvents += inFile['events']
                        totalInEvents += inFile['events']
                        inFiles.add(inFile['lfn'])
    
                for outFile in outputFiles:
    #                outEvents += outFile['events']
#                    if outFile['lfn'].count('/store/mc/Summer12/'):
                    totalEvents += outFile['events']
                    outFiles.add(outputFiles[0]['lfn'])
                    if outFile['lfn'] == '/store/unmerged/Run2012D/MinimumBias/RECO/Tier1PromptReco-v1/000/204/577/00000/BEFA10C5-F811-E211-9CEB-E0CB4E1A11A4.root':
                        print jobTarPath
    #                if inEvents != outEvents:
    #                    print 'GOTCHA!'    

        os.system('rm -r %s' % tmpdir)

    
    print totalProdJobs
    print totalEvents
    print len(inFiles)
    print len(outFiles)
    print noSites
    print missingFiles
    print len(failedFWJRs)
    

#    reportFile = open('/tmp/report3.log', 'w')
#    reportFile.writelines(['%s\n' % x for x in outFiles])
#    reportFile.close()
    
if __name__ == '__main__':
    sys.exit(main())
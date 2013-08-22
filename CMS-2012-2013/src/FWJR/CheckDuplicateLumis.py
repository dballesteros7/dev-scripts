'''
Created on Jan 30, 2013

@author: dballest
'''

import sys
from WMCore.Database.CMSCouch import Database

def lumiInMask(mask, lumi):
    for interval in mask:
        if lumi >= interval[0] and lumi <= interval[1]:
            return True
    return False

def main():
    print "A"
    db = Database('wmagent_jobdump/jobs', 'http://vocms202.cern.ch:5984')
    results = db.loadView('JobDump', 'jobsByWorkflowName', {'startkey': ['pdmvserv_PixelRecover53_537p4_130116_130722_4919'],
                                                            'endkey' : ['pdmvserv_PixelRecover53_537p4_130116_130722_4919', {}],
                                                             'include_docs' : True})
    rows = results['rows']
    fileInfo = {}
    for entry in rows:
        doc = entry['doc']
        jobType = doc['jobType']
        if jobType != 'Processing':
            continue
        mask = doc['mask']
        inputFiles = doc['inputfiles']
        rAndl = mask['runAndLumis']
        for file in inputFiles:
            lfn = file['lfn']
            if lfn not in fileInfo:
                fileInfo[lfn] = {}
            for run in file['runs']:
                runNumber = str(run['run_number'])
                if runNumber not in rAndl:
                    continue
                lumis = run['lumis']
                for lumi in lumis:
                    if not lumiInMask(rAndl[runNumber], lumi):
                        continue
                    if runNumber not in fileInfo[lfn]:
                        fileInfo[lfn][runNumber] = {}
                    if lumi in fileInfo[lfn][runNumber]:
                        print "ALERT: Lumi %s from run %s is processed twice for file %s" % (lumi, runNumber, lfn)
                        fileInfo[lfn][runNumber][lumi].append(entry['id'])
                        print "Jobs processing it so far: %s" % str(fileInfo[lfn][runNumber][lumi])
                    else:
                        fileInfo[lfn][runNumber][lumi] = [entry['id']]
                    
if __name__ == '__main__':
    sys.exit(main())
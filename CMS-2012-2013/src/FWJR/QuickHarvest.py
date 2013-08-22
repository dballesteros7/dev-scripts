"""
__FWJR.QuickHarvest__

Created on Mar 11, 2013

@author: dballest
"""

import sys
from WMCore.Database.CMSCouch import Database

def main():
    db = Database('wmagent_jobdump/fwjrs', 'http://vocms237.cern.ch:5984')
    results = db.loadView('FWJRDump', 'fwjrsByWorkflowName', {'startkey': ['pdmvserv_TOP-Summer12pLHE-00001_3_v0_STEP0ATCERN_130728_164313_3585'],
                                                              'endkey' : ['pdmvserv_TOP-Summer12pLHE-00001_3_v0_STEP0ATCERN_130728_164313_3585', {}],
                                                              'include_docs' : True})
    globalJobTime = 0.0
    globalEvents = 0.0
    globalCPUTime = 0.0
    globalCPUEventTime = 0.0
    count = 0
    rows = results['rows']
    for entry in rows:
        doc = entry['doc']
        fwjr = doc['fwjr']
        task = fwjr['task']
        if task == '/pdmvserv_TOP-Summer12pLHE-00001_3_v0_STEP0ATCERN_130728_164313_3585/Production':
            steps = fwjr['steps']
            breakLoop = False
            cmsRunStep = None
            for step in steps:
                if steps[step]['status'] != 0 and step != 'logArch1':
                    breakLoop = True
                    break
                if step == 'cmsRun1':
                    cmsRunStep = steps[step]
            if breakLoop:
                continue
            count += 1
            performance = cmsRunStep['performance']
            totalJobTime = float(performance['cpu']['TotalJobTime'])
            globalJobTime += totalJobTime
            cpuTime = float(performance['cpu']['TotalJobCPU'])
            globalCPUTime += cpuTime
            cpuEventTime = float(performance['cpu']['TotalEventCPU'])
            globalCPUEventTime += cpuEventTime
            events = 10000
            globalEvents +=  events

    timePerJob = globalJobTime/count
    if timePerJob > 3600:
        timePerJob = timePerJob/3600.0
        print 'Average job duration: %.2f hours' % timePerJob
    else:
        print 'Average job duration: %.0f seconds' % timePerJob
    print 'Job time per event: %.2f seconds' % (globalJobTime/globalEvents)
    print 'Average job CPU time: %.0f seconds' % (globalCPUTime/count)
    print 'Average event CPU time: %.8f seconds' % (cpuEventTime/globalEvents) 
    print 'Events processed: %d' % globalEvents
    print 'Jobs processed: %d' % count

if __name__ == '__main__':
    sys.exit(main())
"""
Created on Jun 13, 2013

@author: dballest
"""

import sys

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

def main():
    toCheckList = '/home/dballest/Dev-Workspace/dev-scripts/data/upgrade-vocms85-613.txt'
    handle = open(toCheckList, 'r')
    timePerJobFile = open('/home/dballest/Dev-Workspace/dev-scripts/data/upgrade-vocms85-tpj.data', 'w')
    eventsPerLumiFile = open('/home/dballest/Dev-Workspace/dev-scripts/data/upgrade-vocms85-epl.data', 'w')
    lumisPerMergeFile = open('/home/dballest/Dev-Workspace/dev-scripts/data/upgrade-vocms85-lpm.data', 'w')
    count = 0
    for request in handle:
        z = WMWorkloadHelper()
        z.load('https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/%s/spec' % request.strip())
        schema = z.data.request.schema
        requesType = schema.RequestType
        if requesType != 'MonteCarlo':
            continue
        timePerEvent = schema.TimePerEvent
        sizePerEvent = schema.SizePerEvent
        events = schema.RequestNumEvents
        eff = getattr(schema, "FilterEfficiency", 1.0)
        topTask = z.getTopLevelTask()[0]
        eventsPerJob = topTask.data.input.splitting.events_per_job
        if eff < 1.0:
            count += 1
            for childTask in topTask.childTaskIterator():
                if childTask.data.taskType == 'Merge':
                    mergeSizeLimit = childTask.data.input.splitting.max_merge_size
                    mergeEventLimit = childTask.data.input.splitting.max_merge_events
                    break
            sizeOfZeroEvent = 131091.0
            sizePerLumi = eventsPerJob*sizePerEvent*eff
            lumisPerMergedBySize = mergeSizeLimit/(sizePerLumi + sizeOfZeroEvent)
            lumisPerMergedByEvent = mergeEventLimit/(eventsPerJob*eff)
            timePerJobFile.write("%f\n" % (timePerEvent * eventsPerJob))
            eventsPerLumiFile.write("%f\n" % (eventsPerJob*eff))
            lumisPerMergeFile.write("%f\n" % min(lumisPerMergedBySize, lumisPerMergedByEvent))
    print count
    handle.close()
    timePerJobFile.close()
    eventsPerLumiFile.close()
    lumisPerMergeFile.close()

if __name__ == '__main__':
    sys.exit(main())
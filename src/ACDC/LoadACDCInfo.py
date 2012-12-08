"""
Created on Sep 27, 2012

@author: dballest
"""

import sys
import json
from optparse import OptionParser
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import loadWorkload


def main():
    parser = OptionParser()
    parser.add_option("-f", "--input-list", dest="acdcList")
    parser.add_option("-o", "--output", dest="outputFile")

    (options, _) = parser.parse_args()
    
    handle = open(options.acdcList, 'r')
    lines = handle.read()
    
    outputList = []
    for line in lines.splitlines():
        request = {'RequestWorkflow' : 'https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/%s/spec' % line}
        workload = loadWorkload(request)
        topLevelTask =  workload.getTopLevelTask()
        taskInput = topLevelTask[0].inputReference()
        owner = taskInput.splitting.owner
        group = taskInput.splitting.group
        collection_name = taskInput.acdc.collection
        fileset_name = taskInput.acdc.fileset
        originalOwner = workload.getOwner()
        outputList.append({'owner' : owner, 'group' : group, 
                           'collection_name' : collection_name,
                           'fileset_name' : fileset_name,
                           'original_dn' : originalOwner['dn']})
    handle.close()
    
    handle = open(options.outputFile, 'w')
    json.dump(outputList, handle)
    handle.close()
    
         
if __name__ == '__main__':
    sys.exit(main())
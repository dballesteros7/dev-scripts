"""
__DBS.splitTester__

Created on Jul 25, 2013

@author: dballest
"""

import pickle
import sys

from WMCore.Database.CMSCouch import Database
from WMCore.WorkQueue.Policy.Start.Block import Block
from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

def main():
    demPolicy = Block()
    reqmgr = RequestManager(dict = {'endpoint' : 'https://cmsweb.cern.ch/reqmgr/reqMgr'})
    result = reqmgr.getRequest('pdmvserv_HIG-Summer12DR53X-01392_T1_ES_PIC_MSS_1_v0__130724_063344_7207')
    workloadDB = Database(result['CouchWorkloadDBName'], result['CouchURL'])
    workloadPickle = workloadDB.getAttachment('pdmvserv_HIG-Summer12DR53X-01392_T1_ES_PIC_MSS_1_v0__130724_063344_7207', 'spec')
    spec = pickle.loads(workloadPickle)
    workload = WMWorkloadHelper(spec)
    x,y = demPolicy(wmspec = workload, task = workload.getTopLevelTask()[0])
    print x
    print y

if __name__ == '__main__':
    sys.exit(main())
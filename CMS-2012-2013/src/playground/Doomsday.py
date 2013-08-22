"""
__playground.Doomsday__

Created on May 31, 2013

@author: dballest
"""

import sys

from WMCore.Database.CMSCouch import CouchServer

def main():
    x = CouchServer(dburl = 'http://vocms202.cern.ch:5984')
    y = x.connectDatabase('wmagent_jobdump/jobs')
    results = y.loadView('JobDump', 'jobsByWorkflowName', {'startkey': ['pdmvserv_EXO-Summer12_DR53X_RD-00004_T1_UK_RAL_MSS_3_v1__130604_122630_4006'],
                                                 'endkey' : ['pdmvserv_EXO-Summer12_DR53X_RD-00004_T1_UK_RAL_MSS_3_v1__130604_122630_4006', {}],
                                                 'include_docs' : True})['rows']
    counts = {}
    for doc in results:
        realDoc = doc['doc']
        type = realDoc['jobType']
        if type not in counts:
            counts[type] = 0
        counts[type] += 1
    print counts

if __name__ == '__main__':
    sys.exit(main())
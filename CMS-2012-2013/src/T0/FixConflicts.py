'''
Created on Jan 22, 2013

@author: dballest
'''

import sys

from WMCore.Services.WMStats.WMStatsReader import  WMStatsReader
from WMCore.Database.CMSCouch import Database

def main():
    reader = WMStatsReader("http://dummy.cern.ch:5984", "wmagent_summary")
    wmstats = Database('wmagent_summary', 'http://dummy.cern.ch:5984')
    suspiciousWorkflows = reader.workflowsByStatus(["Processing Done"], stale = False)
    for entry in suspiciousWorkflows:
        requestDoc = wmstats.document(entry)
        statusList = requestDoc['request_status']
        if statusList[-2]['status'] == 'normal-archived':
            statusList = statusList[:-1]
            requestDoc['request_status'] = statusList
            wmstats.queue(requestDoc)
            
    wmstats.commit()
if __name__ == '__main__':
    sys.exit(main())

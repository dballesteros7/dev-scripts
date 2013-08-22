'''
Created on Feb 10, 2013

@author: dballest
'''

import sys
from WMCore.Database.CMSCouch import Database

def main():
    sum = 0
    x = Database('workqueue', 'http://vocms201.cern.ch:5984')
    y = x.loadView('WorkQueue', 'availableByPriority', {'include_docs' : True})
    loadDistribution = {}
    for entry in y['rows']:
        doc = entry['doc']
        element = doc['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement']
        key = frozenset(element['SiteWhitelist'])
        if key not in loadDistribution:
            loadDistribution[key] = 0
        loadDistribution[key] += element['Jobs']
    for site, jobs in loadDistribution.items():
        print "Site list %s has %d jobs" % (str(site), jobs)
if __name__ == '__main__':
    sys.exit(main())
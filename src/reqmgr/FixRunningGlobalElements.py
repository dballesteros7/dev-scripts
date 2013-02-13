'''
Created on Feb 13, 2013

@author: dballest
'''

import sys
from WMCore.Database.CMSCouch import Database

def main():
    requestName = sys.argv[1]
    x = Database('workqueue', 'https://cmsweb.cern.ch/couchdb')
    y = x.loadView('WorkQueue', 'elementsByParent', {'include_docs' : True}, [requestName])
    runningElements = []
    for entry in y['rows']:
        doc = entry['doc']
        element = doc['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement']
        if element['Status'] == 'Running':
            runningElements.append(doc)
    print "Found %d elements running, fix them?" % len(runningElements)
    inputData = raw_input("Type y/n: ")
    if inputData != "y":
        print "Aborting operation..."
        return 0
    for doc in runningElements:
        doc['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement']['Status'] = 'Done'
        x.queue(doc)
    x.commit()
    print "Operation complete!"
    return 0

if __name__ == '__main__':
    sys.exit(main())

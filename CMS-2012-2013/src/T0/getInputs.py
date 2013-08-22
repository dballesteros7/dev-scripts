'''
Created on Jan 29, 2013

@author: dballest
'''

from WMCore.Database.CMSCouch import Database
import sys

def main():
    myDB = Database('workqueue_inbox', 'https://vocms169.cern.ch/couchdb')
    document = myDB.document(sys.argv[1])
    inputs = document['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement']['Inputs']
    for block in inputs:
        print '                           "%s",' % block

if __name__ == '__main__':
    sys.exit(main())
'''
Created on Dec 3, 2012

@author: dballest
'''

import sys
import logging
import os
import threading

from WMCore.DAOFactory import DAOFactory
from WMCore.Configuration import loadConfigurationFile
from WMCore.WMInit import WMInit
from WMCore.Database.CMSCouch import Database

def main():
    config = loadConfigurationFile(os.environ['WMAGENT_CONFIG'])
    config.CoreDatabase.dialect = 'oracle'
    init = WMInit()
    init.setDatabaseConnection(config.CoreDatabase.connectUrl,
                               config.CoreDatabase.dialect)
    couchDB = Database('wmagent_jobdump/fwjrs', '')
    couchDB2 = Database('wmagent_jobdump/jobs', '')
    myThread = threading.currentThread()
    daofactory = DAOFactory(package = "WMCore.WMBS",
                            logger = logging,
                            dbinterface = myThread.dbi)
    getJobsDAO = daofactory(classname = "Jobs.GetAllJobs")
    completedJobs = getJobsDAO.execute(state = 'complete')
    candidates = []
    while len(completedJobs):
        candidates = []
        chunk = completedJobs[:500]
        completedJobs = completedJobs[500:]
        result = couchDB.loadView('FWJRDump', 'outputByJobID', keys = chunk)
        rows = result['rows']
        for entry in rows:
            candidates.append(entry['key'])
        for jobId in candidates:
            doc = couchDB2.document(str(jobId))
            last = max(map(int, doc['states'].keys()))
            lastState = doc['states'][str(last)]['newstate']
            if lastState == 'success':
                print jobId
            

if __name__ == '__main__':
    sys.exit(main())
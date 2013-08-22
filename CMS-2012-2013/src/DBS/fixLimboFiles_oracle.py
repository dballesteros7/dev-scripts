"""
__DBS.fixLimboFiles__

Created on Jul 5, 2013

@author: dballest
"""

import logging
import os
import socket
import sys
import threading

from WMCore.WMInit import connectToDB
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.CMSCouch import Database

def main():
    if "WMAGENT_CONFIG" not in os.environ:
        os.environ["WMAGENT_CONFIG"] = '/data/srv/wmagent/current/config/wmagent/config.py'
    myThread = threading.currentThread()
    connectToDB()
    formatter = DBFormatter(logging, myThread.dbi)
    limboFiles = formatter.formatDict(myThread.dbi.processData("""SELECT dbsbuffer_workflow.name, dbsbuffer_file.lfn
                                                                  FROM dbsbuffer_file
                                                                  INNER JOIN dbsbuffer_workflow ON
                                                                      dbsbuffer_file.workflow = dbsbuffer_workflow.id
                                                                  LEFT OUTER JOIN dbsbuffer_block ON
                                                                      dbsbuffer_file.block_id = dbsbuffer_block.id
                                                                  WHERE dbsbuffer_file.status = 'READY' AND
                                                                        dbsbuffer_block.id is NULL"""))
    if not limboFiles:
        print "There are no bad files to fix"
        return
    for entry in limboFiles:
        data = Database('wmagent_jobdump/fwjrs', 'http://%s:5984' % socket.gethostname())
        result = data.loadView('FWJRDump', 'jobsByOutputLFN', {'include_docs' : True},
                      [[entry['name'], entry['lfn']]])['rows']
        if result:
            result = result[0]
            fwjr = result['doc']['fwjr']
            for step in fwjr['steps']:
                if step == 'cmsRun1':
                    stepInfo = fwjr['steps'][step]
                    site = stepInfo['site']
                    break
        else:
            print "Could not find location for %s" % entry['lfn']
            continue
        se = myThread.dbi.processData("""SELECT wmbs_location_senames.se_name FROM
                                       wmbs_location_senames
                                       INNER JOIN wmbs_location ON
                                          wmbs_location.id = wmbs_location_senames.location
                                       WHERE wmbs_location.site_name = '%s'""" % site)
        se = formatter.formatDict(se)[0]
        insertQuery = """INSERT INTO dbsbuffer_location (se_name)
               SELECT '%s' AS se_name FROM DUAL WHERE NOT EXISTS
                (SELECT se_name FROM dbsbuffer_location WHERE se_name = '%s')""" % (se['se_name'], se['se_name'])
        myThread.dbi.processData(insertQuery)
        updateQuery = """INSERT INTO dbsbuffer_file_location (filename, location)
                           SELECT df.id, dl.id
                           FROM dbsbuffer_file df,  dbsbuffer_location dl
                           WHERE df.lfn = '%s'
                           AND dl.se_name = '%s'""" % (entry['lfn'], se['se_name'])
        myThread.dbi.processData(updateQuery)
        updateQuery = """UPDATE dbsbuffer_file SET status = 'NOTUPLOADED' WHERE lfn = '%s'""" % entry['lfn']
        myThread.dbi.processData(updateQuery)

if __name__ == '__main__':
    sys.exit(main())
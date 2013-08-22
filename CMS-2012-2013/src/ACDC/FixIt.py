"""
__ACDC.FixIt__

Created on Mar 29, 2013

@author: dballest
"""

import sys
from WMCore.Database.CMSCouch import CouchServer, Document

def main():
    server = CouchServer('http://vocms234.cern.ch:5984/')
    acdcDB = server.connectDatabase('wmagent_acdc', False)
    result = acdcDB.loadView('ACDC', 'owner_coll_fileset_docs',
                             {'reduce' : False, 'include_docs' : True},
                             [['ppd', 'vlimant',
                               'jen_a_ACDC2-234-Winter532012CLP_Jets1Prio7_537p5_130318_181057_7716',
                               '/jen_a_ACDC2-234-Winter532012CLP_Jets1Prio7_537p5_130318_181057_7716/DataProcessing']])
    for entry in result['rows']:
        doc = entry['doc']
        for lfn in doc['files']:
            doc['files'][lfn]['locations'].append('cmssrm.fnal.gov')
        couchDoc = Document(inputDict = doc)
        acdcDB.queue(couchDoc)
    acdcDB.commit()

if __name__ == '__main__':
    sys.exit(main())
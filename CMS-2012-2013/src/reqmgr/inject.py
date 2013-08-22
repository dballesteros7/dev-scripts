"""
__reqmgr.inject__

Created on Apr 30, 2013

@author: dballest
"""
from WMCore.ACDC.CouchCollection import CouchCollection
from WMCore.ACDC.CouchFileset import CouchFileset
from WMCore.ACDC.CouchService import CouchService
from WMCore.DataStructs.File import File
from WMCore.Services.UUID import makeUUID
import random
import time

def main():
    svc = CouchService(url = 'https://cmsoguatworld.cern.ch/couchdb',
                       database = 'acdcserver')

    ownerA = svc.newOwner("somegroup", "someuserA")
    ownerB = svc.newOwner("somegroup", "someuserB")

    testCollectionA = CouchCollection(database = 'acdcserver',
                                      url = 'https://cmsoguatworld.cern.ch/couchdb',
                                      name = "dballest_ReRecoCosmics-NoDelay_130218_044722_1152")
    testCollectionA.setOwner(ownerA)
    testCollectionB = CouchCollection(database = 'acdcserver',
                                      url = 'https://cmsoguatworld.cern.ch/couchdb',
                                      name = "dballest_MonteCarlo_130218_044707_4702")
    testCollectionB.setOwner(ownerA)
    testCollectionC = CouchCollection(database = 'acdcserver',
                                      url = 'https://cmsoguatworld.cern.ch/couchdb',
                                      name = "dballest_ReRecoCosmics-NoDelay_130218_044722_1152")
    testCollectionC.setOwner(ownerB)
    testCollectionD = CouchCollection(database = 'acdcserver',
                                      url = 'https://cmsoguatworld.cern.ch/couchdb',
                                      name = "dballest_ReRecoCosmics-NoDelay_130218_044722_1152")
    testCollectionD.setOwner(ownerB)

    testFilesetA = CouchFileset(database = 'acdcserver',
                                url = 'https://cmsoguatworld.cern.ch/couchdb',
                                name = "TestFilesetA")
    testCollectionA.addFileset(testFilesetA)
    testFilesetB = CouchFileset(database = 'acdcserver',
                                url = 'https://cmsoguatworld.cern.ch/couchdb',
                                name = "TestFilesetB")
    testCollectionB.addFileset(testFilesetB)
    testFilesetC = CouchFileset(database = 'acdcserver',
                                url = 'https://cmsoguatworld.cern.ch/couchdb',
                                name = "TestFilesetC")
    testCollectionC.addFileset(testFilesetC)
    testFilesetD = CouchFileset(database = 'acdcserver',
                                url = 'https://cmsoguatworld.cern.ch/couchdb',
                                name = "TestFilesetD")
    testCollectionC.addFileset(testFilesetD)

    testFiles = []
    for i in range(5):
        testFile = File(lfn = makeUUID(), size = random.randint(1024, 4096),
                        events = random.randint(1024, 4096))
        testFiles.append(testFile)

    testFilesetA.add(testFiles)
    time.sleep(1)
    testFilesetB.add(testFiles)
    time.sleep(1)
    testFilesetC.add(testFiles)
    time.sleep(2)
    testFilesetD.add(testFiles)

if __name__ == '__main__':
    main()
"""
__DBS.injectorTester__

Created on Jul 29, 2013

@author: dballest
"""
import json
import os
import sys
import tempfile

from FileInjection import Injector
from WMQuality.TestInit import TestInit

def createConfigFile():
    blob = """from WMCore.Configuration import Configuration
config = Configuration()
config.section_('CoreDatabase')
config.CoreDatabase.socket = '/var/run/mysqld/mysqld.sock'
config.CoreDatabase.connectUrl = 'mysql://wmagent-test:@localhost/WMAgent'\n"""
    fd, path = tempfile.mkstemp(suffix = ".py")
    os.close(fd)
    handle = open(path, 'w')
    handle.write(blob)
    handle.close()
    return path

def createInputData():
    inputDataArray = [{"dataset" : "/DummyDataset1/Summer91-TripleFiltered-v1/RECO",
                       "lfn" : "/store/data/Summer91/DummyDataset1/RECO/TripleFiltered-v1/00000/0C390645-DDF3-E211-8A8E-003048F2B2C6.root",
                       "size" : 100,
                       "events" : 20,
                       "globalTag" : "GT_Test_V1",
                       "location" : "srm-cms.cern.ch"},
                      {"dataset" : "/DummyDataset1/Summer91-TripleFiltered-v1/RECO",
                       "lfn" : "/store/data/Summer91/DummyDataset1/RECO/TripleFiltered-v1/00000/8C9F5C1B-E3F3-E211-871D-00A0D1EE8AF4.root",
                       "size" : 120,
                       "events" : 25,
                       "globalTag" : "GT_Test_V1",
                       "location" : "srm-cms.cern.ch"},
                      {"dataset" : "/DummyDataset2/Summer91-Processed-v2/AOD",
                       "lfn" : "/store/data/Summer91/DummyDataset2/RECO/Processed-v2/00000/8C9F5C1B-E3F3-E211-871D-00A0D1EE8AF4.root",
                       "runsAndLumis" : {"29" : [1,3,4,5],
                                         "21" : [12,31]},
                       "location" : "srm-cms.cern.ch"}]
    fd, path = tempfile.mkstemp(suffix = ".json")
    os.close(fd)
    handle = open(path, 'w')
    json.dump(inputDataArray, handle)
    handle.close()
    return path

class simpleObject():
    def __init__(self):
        self.configFilePath = None
        self.inputDataFilePath = None

def main():
    options = simpleObject()
    options.configFilePath = createConfigFile()
    options.inputDataFilePath = createInputData()

    testInit = TestInit(__file__)
    testInit.setLogging()
    testInit.setDatabaseConnection(destroyAllDatabase=True)
    testInit.setSchema(customModules = ["WMComponent.DBS3Buffer"],
                       useDefault = False)

    injector = Injector(options)
    injector.prepareDBSFiles()
    injector.createFilesInDBSBuffer()

if __name__ == '__main__':
    sys.exit(main())
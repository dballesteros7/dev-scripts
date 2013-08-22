'''
Created on Dec 3, 2012

@author: dballest
'''

import sys
import logging
import os
import threading

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.DAOFactory import DAOFactory
from WMCore.Configuration import loadConfigurationFile
from WMCore.WMInit import WMInit


def main():
    myPhedex = PhEDEx()
    config = loadConfigurationFile(os.environ['WMAGENT_CONFIG'])
    config.CoreDatabase.dialect = 'mysql'
    init = WMInit()
    init.setDatabaseConnection(config.CoreDatabase.connectUrl,
                               config.CoreDatabase.dialect,
                               config.CoreDatabase.socket)
    myThread = threading.currentThread()
    daofactory = DAOFactory(package = "WMComponent.PhEDExInjector.Database",
                            logger = logging,
                            dbinterface = myThread.dbi)

    getUninjectedDAO = daofactory(classname = "GetUninjectedFiles")
    uninjectedFiles = getUninjectedDAO.execute()
    for location in uninjectedFiles:
        for dataset in uninjectedFiles[location]:
            for block in uninjectedFiles[location][dataset]:
                result = myPhedex.getReplicaInfoForFiles(dataset = dataset, block = block)
                phedexBlock = result['phedex']['block']
                if not phedexBlock:
                    continue
                phedexBlock = phedexBlock[0]
                filesInjected = [x['name'] for x in phedexBlock['file']]
                for fileInfo in uninjectedFiles[location][dataset][block]['files']:
                    lfn = fileInfo['lfn']
                    if lfn in filesInjected:
                        print lfn
    return 0

if __name__ == '__main__':
    sys.exit(main())
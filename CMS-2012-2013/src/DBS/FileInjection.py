"""
__DBS.FileInjection__

Created on Jul 28, 2013

@author: dballest
"""
import collections
import json
import logging
import os
import threading
import time
import sys

from optparse import OptionParser

from WMComponent.DBS3Buffer.DBSBufferFile import DBSBufferFile
from WMCore.DataStructs.Run import Run
from WMCore.DAOFactory           import DAOFactory
from WMCore.WMConnectionBase     import WMConnectionBase
from WMCore.WMInit import connectToDB

class Injector(WMConnectionBase):
    def __init__(self, options):
        """
        __init__

        Initialize the object
        """
        self.configFilePath = options.configFilePath
        self.inputDataFilePath = options.inputDataFilePath
        self.filesPerBlock = options.filesPerBlock
        self.timeToClose = options.timeToClose

        self.setup()

        WMConnectionBase.__init__(self, "WMCore.WMBS")

        self.dbsFilesToCreate = []
        self.datasetAlgoID = collections.deque(maxlen = 1000)
        self.datasetAlgoPaths = collections.deque(maxlen = 1000)
        self.dbsLocations = collections.deque(maxlen = 1000)
        self.workflowIDs = collections.deque(maxlen = 1000)
        self.workflowPaths = collections.deque(maxlen = 1000)

        myThread = threading.currentThread()
        self.dbsDaoFactory = DAOFactory(package = "WMComponent.DBS3Buffer",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)

        self.dbsCreateFiles = self.dbsDaoFactory(classname = "DBSBufferFiles.Add")
        self.dbsSetLocation = self.dbsDaoFactory(classname = "DBSBufferFiles.SetLocationByLFN")
        self.dbsInsertLocation = self.dbsDaoFactory(classname = "DBSBufferFiles.AddLocation")
        self.dbsSetChecksum = self.dbsDaoFactory(classname = "DBSBufferFiles.AddChecksumByLFN")
        self.dbsSetRunLumi = self.dbsDaoFactory(classname = "DBSBufferFiles.AddRunLumi")
        self.dbsInsertWorkflow = self.dbsDaoFactory(classname = "InsertWorkflow")

        return

    def setup(self):
        """
        _setup_

        Setup the environment, the database connection and retrieve the
        input.
        """
        if "WMAGENT_CONFIG" not in os.environ:
            if self.configFilePath is not None:
                os.environ["WMAGENT_CONFIG"] = self.configFilePath
            else:
                raise RuntimeError("Config path option or the WMAGENT_CONFIG environment variable must be specified")
        try:
            connectToDB()
        except:
            logging.error("Failed to connect to the Database")
            raise
        inputDataFile = None
        try:
            inputDataFile = open(self.inputDataFilePath, 'r')
            self.inputData = json.load(inputDataFile)
        except:
            logging.error("Failed to load the input file with the information")
            raise
        finally:
            if inputDataFile is not None:
                inputDataFile.close()

    def prepareDBSFiles(self):
        """
        _prepareDBSFiles_

        Retrieve the information from the JSON input data
        and create DBSFile objects that can be registered in the
        database.
        """
        timestamp = time.strftime('%m%d%y_%H%M%S')
        for fileEntry in self.inputData:
            # Get all the info out of a standard named dataset
            datasetInfo = str(fileEntry["dataset"])
            tokens = datasetInfo.split('/')
            primDs = tokens[1]
            procDs = tokens[2]
            dataTier = tokens[3]
            procDsTokens = procDs.split('-')
            acqEra = procDsTokens[0]
            procVer = procDsTokens[-1][1:]

            ckSumInfo = fileEntry["checksums"]
            for entry in ckSumInfo:
                ckSumInfo[entry] = str(ckSumInfo[entry])

            # Build the basic dbsBuffer file
            dbsFile = DBSBufferFile(lfn = str(fileEntry["lfn"]),
                                    size = int(fileEntry.get("size", 0)),
                                    events = int(fileEntry.get("events", 0)),
                                    checksums = ckSumInfo,
                                    status = "NOTUPLOADED")
            dbsFile.setAlgorithm(appName = "cmsRun",
                                 appVer = str(fileEntry.get("cmssw", "LEGACY")),
                                 appFam = "Legacy",
                                 psetHash = "GIBBERISH",
                                 configContent = "None;;None;;None")

            dbsFile.setDatasetPath("/%s/%s/%s" % (primDs,
                                                  procDs,
                                                  dataTier))
            dbsFile.setValidStatus(validStatus = "PRODUCTION")
            dbsFile.setProcessingVer(ver = procVer)
            dbsFile.setAcquisitionEra(era = acqEra)
            dbsFile.setGlobalTag(globalTag = str(fileEntry.get('globalTag', "LEGACY")))

            # Build a representative task name
            dbsFile['task'] = '/LegacyInsertionTask_%s/Insertion' % timestamp

            # Get the runs and lumis
            runsAndLumis = fileEntry.get("runsAndLumis", {})
            for run in runsAndLumis:
                newRun = Run(runNumber = int(run))
                newRun.extend([int(x) for x in runsAndLumis[run]])
                dbsFile.addRun(newRun)

            # Complete the file information with the location and queue it
            dbsFile.setLocation(se = str(fileEntry["location"]), immediateSave = False)
            self.dbsFilesToCreate.append(dbsFile)
        self.inputData = None
        return

    def createFilesInDBSBuffer(self):
        """
        _createFilesInDBSBuffer_
        It does the actual job of creating things in DBSBuffer
        """
        if len(self.dbsFilesToCreate) == 0:
            # Whoops, nothing to do!
            return

        dbsFileTuples = []
        dbsFileLoc = []
        dbsCksumBinds = []
        runLumiBinds = []
        selfChecksums = None

        existingTransaction = self.beginTransaction()

        for dbsFile in self.dbsFilesToCreate:
            # Append a tuple in the format specified by DBSBufferFiles.Add
            # Also run insertDatasetAlgo
            assocID = None
            datasetAlgoPath = '%s:%s:%s:%s:%s:%s:%s:%s' % (dbsFile['datasetPath'],
                                                           dbsFile["appName"],
                                                           dbsFile["appVer"],
                                                           dbsFile["appFam"],
                                                           dbsFile["psetHash"],
                                                           dbsFile['processingVer'],
                                                           dbsFile['acquisitionEra'],
                                                           dbsFile['globalTag'])
            # First, check if this is in the cache
            if datasetAlgoPath in self.datasetAlgoPaths:
                for da in self.datasetAlgoID:
                    if da['datasetAlgoPath'] == datasetAlgoPath:
                        assocID = da['assocID']
                        break

            if not assocID:
                # Then we have to get it ourselves
                try:
                    assocID = dbsFile.insertDatasetAlgo()
                    self.datasetAlgoPaths.append(datasetAlgoPath)
                    self.datasetAlgoID.append({'datasetAlgoPath': datasetAlgoPath,
                                               'assocID': assocID})
                except Exception:
                    msg = "Unhandled exception while inserting datasetAlgo: %s\n" % datasetAlgoPath
                    logging.error(msg)
                    raise

            # Associate the workflow to the file using the taskPath and the requestName
            taskPath = dbsFile['task']
            workflowName = taskPath.split('/')[1]
            workflowPath = '%s:%s' % (workflowName, taskPath)
            if workflowPath in self.workflowPaths:
                for wf in self.workflowIDs:
                    if wf['workflowPath'] == workflowPath:
                        workflowID = wf['workflowID']
                        break
            else:
                result = self.dbsInsertWorkflow.execute(workflowName, taskPath,
                                                        self.timeToClose,
                                                        self.filesPerBlock,
                                                        250000000,
                                                        5000000000000,
                                                        conn = self.getDBConn(),
                                                        transaction = self.existingTransaction())
                workflowID = result

            self.workflowPaths.append(workflowPath)
            self.workflowIDs.append({'workflowPath': workflowPath, 'workflowID': workflowID})

            lfn = dbsFile['lfn']
            selfChecksums = dbsFile['checksums']
            jobLocation = dbsFile.getLocations()[0]

            dbsFileTuples.append((lfn, dbsFile['size'],
                                  dbsFile['events'], assocID,
                                  dbsFile['status'], workflowID))

            dbsFileLoc.append({'lfn': lfn, 'sename' : jobLocation})
            if dbsFile['runs']:
                runLumiBinds.append({'lfn': lfn, 'runs': dbsFile['runs']})

            if selfChecksums:
                # If we have checksums we have to create a bind
                # For each different checksum
                for entry in selfChecksums.keys():
                    dbsCksumBinds.append({'lfn': lfn, 'cksum' : selfChecksums[entry],
                                          'cktype' : entry})

        try:
            if not jobLocation in self.dbsLocations:
                self.dbsInsertLocation.execute(siteName = jobLocation,
                                               conn = self.getDBConn(),
                                               transaction = self.existingTransaction())
                self.dbsLocations.append(jobLocation)

            self.dbsCreateFiles.execute(files = dbsFileTuples,
                                        conn = self.getDBConn(),
                                        transaction = self.existingTransaction())

            self.dbsSetLocation.execute(binds = dbsFileLoc,
                                        conn = self.getDBConn(),
                                        transaction = self.existingTransaction())

            self.dbsSetChecksum.execute(bulkList = dbsCksumBinds,
                                        conn = self.getDBConn(),
                                        transaction = self.existingTransaction())

            if len(runLumiBinds) > 0:
                self.dbsSetRunLumi.execute(file = runLumiBinds,
                                           conn = self.getDBConn(),
                                           transaction = self.existingTransaction())
        except Exception:
            msg = "Got exception while inserting files into DBSBuffer!\n"
            logging.error(msg)
            logging.debug("Listing binds:")
            logging.debug("jobLocation: %s\n" % jobLocation)
            logging.debug("dbsFiles: %s\n" % dbsFileTuples)
            logging.debug("dbsFileLoc: %s\n" % dbsFileLoc)
            logging.debug("Checksum binds: %s\n" % dbsCksumBinds)
            logging.debug("RunLumi binds: %s\n" % runLumiBinds)
            self.rollbackTransaction(existingTransaction)
            raise

        # Now that we've created those files, clear the list
        self.commitTransaction(existingTransaction)
        self.dbsFilesToCreate = []
        return

def main():
    myOptParser = OptionParser()
    myOptParser.add_option("-c", "--config", dest = "configFilePath",
                           help = "Path to the WMAgent config file.")
    myOptParser.add_option("-j", "--files", dest = "inputDataFilePath",
                           help = "Path to the json file with the file data.")
    myOptParser.add_option("-f", "--inBlock", dest = "filesPerBlock",
                           default = 500,
                           help = "Maximum number of files per block.")
    myOptParser.add_option("-t", "--timeout", dest = "timeToClose",
                           default = 66400,
                           help = "Time before blocks are closed.")
    opts, _ = myOptParser.parse_args()

    injector = Injector(opts)
    injector.prepareDBSFiles()
    injector.createFilesInDBSBuffer()
    return

if __name__ == '__main__':
    sys.exit(main())

'''
Created on Aug 31, 2012

@author: dballest
'''

import sys
import os
import json
import shlex

from optparse import OptionParser

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Database.CMSCouch import Database
from WMCore.Algorithms.SubprocessAlgos import runCommand


def checkForMissingFiles(options):
    #Initialize stuff
    phedexAPI = PhEDEx({'cachepath' : options.cachepath})
    acdcCouch = Database('wmagent_acdc', options.acdcUrl)

    #Let's get the IDs of the ACDC documents for the task/request/group/user
    array = [options.group, options.user, options.request, options.task]
    result = acdcCouch.loadView('ACDC', 'owner_coll_fileset_docs', {'reduce' : False}, [array])

    documentsIDs = [x['id'] for x in result['rows']]
    
    badFiles = {}

    #Go through the documents
    for docID in documentsIDs:
        doc = acdcCouch.document(docID)

        #Are we going to change this doc? Better back it up
        if options.change:
            backupFile = os.open(os.path.join(options.backup, "%s.bkp" % doc["_id"]), 'w')
            json.dump(doc, backupFile)
            backupFile.close()

        #Go through the files
        files = doc["files"]
        for inputFile in files:

            #Use PhEDEx API to get site based on the SE
            se = files[inputFile]["locations"][0]
            siteLocation = phedexAPI.getBestNodeName(se)

            #Now get the PFN
            pfnDict = phedexAPI.getPFN(siteLocation, inputFile)
            inputPfn = pfnDict[(siteLocation, inputFile)]

            #Run lcg-ls commands and see what we get
            command = 'lcg-ls -b -D srmv2 --srm-timeout 60 %s' % inputPfn
            
            commandList = shlex.split(command)
            try:
                (stdout, stderr, exitCode) = runCommand(commandList, False, 70)
            except Exception, ex:
                exitCode = 99999
                stdout = ''
                stderr = str(ex)
            
            if exitCode:
                #Something went wrong with the command
                #Mark the file as bad
                if docID not in badFiles:
                    badFiles[docID] = []
                badFiles[docID].append(inputFile)
                print 'File %s is thought to be bad' % inputFile
                print 'Command was %s' % command
                print 'Return code was %i' % exitCode
                print 'Stdout was %s' % stdout
                print 'Stderr was %s' % stderr
                
                

def swapLocations(options):
    #Initialize stuff
    phedexAPI = PhEDEx({'cachepath' : options.cachepath})
    acdcCouch = Database('wmagent_acdc', options.acdcUrl)

    #Let's get the IDs of the ACDC documents for the task/request/group/user
    array = [options.group, options.user, options.request, options.task]
    result = acdcCouch.loadView('ACDC', 'owner_coll_fileset_docs', {'reduce' : False}, [array])

    documentsIDs = [x['id'] for x in result['rows']]

    #Load the map file saying what we want to change of location
    mapFile = open(options.map, 'r')
    locationMap = json.load(mapFile)
    mapFile.close()

    #Go through the documents
    for docID in documentsIDs:
        doc = acdcCouch.document(docID)

        #Are we going to change this doc? Better back it up
        if options.change:
            backupFile = os.open(os.path.join(options.backup, "%s.bkp" % doc["_id"]), 'w')
            json.dump(doc, backupFile)
            backupFile.close()

        #Go through the files
        files = doc["files"]
        for inputFile in files:

            #Use PhEDEx API to get site based on the SE
            #Then map that to the desired target
            se = files[inputFile]["locations"][0]
            siteLocation = phedexAPI.getBestNodeName(se)
            targetLocation = locationMap.get(siteLocation, siteLocation)

            if siteLocation == targetLocation:
                #Nothing to do with this one, move on
                continue

            if not options.change:
                #No changes, then give the commands to move the files
                #Get the PFN for both the current location and the target location
                pfnDict = phedexAPI.getPFN(siteLocation, inputFile)
                inputPfn = pfnDict[(siteLocation, inputFile)]
                pfnDict = phedexAPI.getPFN(targetLocation, inputFile)
                targetPfn = pfnDict[(targetLocation, inputFile)]

                #Print it to stdout
                print "lcg-cp -D srmv2 -b %s %s" % (inputPfn, targetPfn)

            else:
                #This is changes time, let's move the stuff
                targetSE = phedexAPI.getNodeSE(targetLocation)
                files[inputFile]["locations"][0] = targetSE
                print "Changing location of %s from %s to %s" % (inputFile, se, targetSE)

        #If specified, commit the changes
        if options.change:
            acdcCouch.commitOne(doc)

    return 0

def main():
    myOptParser = OptionParser()
    myOptParser.add_option("-r", "--request", dest = "request")
    myOptParser.add_option("-g", "--group", dest = "group")
    myOptParser.add_option("-u", "--user", dest = "user")
    myOptParser.add_option("-t", "--task", dest = "task")
    myOptParser.add_option("-a", "--acdc", dest = "acdcUrl")
    myOptParser.add_option("-m", "--map", dest = "map")
    myOptParser.add_option("-o", "--mode", dest = "mode",
                           default = "move")
    myOptParser.add_option("-b", "--backup-dir", dest = "backup",
                           default = "/tmp/backup")
    myOptParser.add_option("-c", "--cache-dir", dest = "cachepath",
                           default = "/tmp/")
    myOptParser.add_option("--commit-changes", action = 'store_true',
                           dest = 'change', default = False)
    (options, _) = myOptParser.parse_args()
    if options.mode == 'move':
        return swapLocations(options)
    elif options.mode == 'check':
        return checkForMissingFiles(options)

    return 0



if __name__ == '__main__':
    sys.exit(main())

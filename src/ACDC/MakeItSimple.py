"""
__ACDC.MakeItSimple__

Created on Mar 8, 2013

@author: dballest
"""

import sys
import json
import shlex
import time
import os
import traceback
import logging
import re

from ACDC.reqmgr import RESTClient
from ACDC.reqmgr import __file__ as reqmgrpy

from subprocess import Popen
from tempfile import mkstemp
from optparse import OptionParser

def buildJSON(acdcRequestName, task, host,
              originalRequest,baseJSONFile,
              doSplitting = False):
    
    jsonFileHandle = open(baseJSONFile, 'r')
    baseJSON = json.load(jsonFileHandle)
    
    # Edit creation parameters
    creationParams = baseJSON['createRequest']
    creationParams['RequestString'] = acdcRequestName
    creationParams['OriginalRequestName'] = originalRequest['RequestName']
    creationParams['InitialTaskPath'] = task
    creationParams['ACDCServer'] = host
    
    if doSplitting:
        splittingParams = {}
        baseJSON['changeSplitting'] = splittingParams
        taskParams = {}
        splittingParams[task.split('/')[-1]] = taskParams
        taskParams['SplittingAlgo'] = 'LumiBased'
        taskParams['lumis_per_job'] = 1
        taskParams['halt_job_on_file_boundaries'] = 'True'
        taskParams['include_parents'] = 'False'
    
    # Edit assignment parameters
    assignParams = baseJSON['assignRequest']
    assignParams['SiteWhitelist'] = originalRequest['Site Whitelist']
    assignParams['AcquisitionEra'] = originalRequest['AcquisitionEra']
    assignParams['Team'] = originalRequest['teams'][0]

    return baseJSON

def checkForACDC(acdcDB, requestName, owner, group):

    timeout = 60
    hostAddr = acdcDB.split('/')[2].split(':')[0]
    command = 'ssh {hostAddr} "curl -g -X GET \'{acdcDB}/_design/ACDC/_view/owner_coll_fileset_count?'
    command += 'startkey=[\\"{group}\\",\\"{owner}\\",\\"{request}\\"]'
    command += '&endkey=[\\"{group}\\",\\"{owner}\\",\\"{request}\\",{{}}]'
    command += '&reduce=true&group_level=4\'"'
    command = command.format(hostAddr = hostAddr, acdcDB = acdcDB,
                             group = group, owner = owner,
                             request = requestName)

    stdoutFileFd, stdoutFile = mkstemp()
    stderrFileFd, stderrFile = mkstemp()
    args = shlex.split(command)
    try:
        process = Popen(args, stderr = stderrFileFd, stdout = stdoutFileFd)
        returnCode = None
        initialTime = time.time()
        while returnCode is None:
            if time.time() - initialTime > timeout:
                process.kill()
            returnCode = process.poll()
        if returnCode != 0:
            raise RuntimeError
    except RuntimeError:
        stderrFileHandle = open(stderrFile)
        stderrFileHandle.seek(0)
        print "Error: %s" % stderrFileHandle.read()

    stdoutFileHandle = open(stdoutFile)
    stdoutFileHandle.seek(0)
    result = json.load(stdoutFileHandle)
    tasksByType = {'Merge' : [],
                   'Harvest' : [],
                   'Processing' : [],
                   'Skim' : []}
    for row in result['rows']:
        task = row['key'][3]
        endOfTask = task.split('/')[-1]
        if 'LogCollect' in endOfTask or 'Cleanup' in endOfTask:
            continue
        if 'Harvest' in endOfTask:
            tasksByType['Harvest'].append(task)
        elif 'Merge' in endOfTask:
            tasksByType['Merge'].append(task)
        elif 'Processing' in endOfTask:
            tasksByType['Processing'].append(task)
        else:
            tasksByType['Skim'].append(task)
        logging.info("Found %d records to ACDC for %s" % (row['value'], task))

    os.close(stdoutFileFd)
    os.close(stderrFileFd)

    return tasksByType

def main():
    logging.getLogger().setLevel(logging.INFO)
    myOptParser = OptionParser()
    myOptParser.add_option('-m', '--mapping', dest = 'mapFile',
                           help = 'File with the team to acdc host mapping',
                           default = 'test')
    myOptParser.add_option('-r', '--request', dest = 'request',
                           help = 'Request to create the ACDCs for',
                           default = 'vlimant_Winter532012DDoublePhoton_IN2P3Prio1_537p6_130226_124526_9730')
    myOptParser.add_option('-j', '--json', dest = 'baseJSON',
                           help = 'Base JSON file for the requests',
                           default = 'ACDCReReco.json')

    opts, _ = myOptParser.parse_args()
    
    teamToHostMap = {}
    try:
        mapFileHandle = open(opts.mapFile)
        for line in mapFileHandle:
            team,host = line.strip().split(':',1)
            if team not in teamToHostMap:
                teamToHostMap[team] = []
            teamToHostMap[team].append(host)
    except:
        traceback.print_exc()
        mapFileHandle.close()
        return 5

    restClient = RESTClient(url = 'https://cmsweb.cern.ch')
    _, data = restClient.httpRequest('GET', '/reqmgr/reqMgr/request?requestName=%s' % opts.request, 
                                          headers = {"Content-type": "application/x-www-form-urlencoded",
                                                     "Accept": "application/json"})

    requestData = json.loads(data)
    originalRequest = requestData
    while originalRequest['RequestType'] == 'Resubmission':
        logging.info("Found parent request %s for %s" % (originalRequest['OriginalRequestName'], originalRequest['RequestName']))
        _, data = restClient.httpRequest('GET', '/reqmgr/reqMgr/request?requestName=%s' % originalRequest['OriginalRequestName'], 
                                              headers = {"Content-type": "application/x-www-form-urlencoded",
                                                         "Accept": "application/json"})
        originalRequest = json.loads(data)
        
    owner = originalRequest['Requestor']
    group = originalRequest['Group']
    teams = requestData['teams']
    hosts = set()
    for team in teams:
        hosts |= set(teamToHostMap[team])
    acdcsToCreateByHost = {}
    for host in hosts:
        logging.info("Checking ACDC information for %s in %s" % (opts.request, host))
        acdcsToCreateByHost[host] = checkForACDC(acdcDB = host,
                                                 requestName = opts.request,
                                                 owner = owner,
                                                 group = group)
    logging.info("ACDCs will be created now...")

    for host in acdcsToCreateByHost:
        for taskType in acdcsToCreateByHost[host]:
            doSplitting = False
            if taskType == 'Processing':
                doSplitting = True
            for task in acdcsToCreateByHost[host][taskType]:
                hostNumber = re.match(r'.*(cmssrv|vocms)([0-9]{1,3})\.(cern\.ch|fnal\.gov).*', host).groups()[1]
                originalRequestName = originalRequest['RequestName']
                originalRequestPortion = '_'.join(originalRequestName.split('_')[1:-3])
                acdcRequestName = 'ACDC%s%s_%s' % (hostNumber, taskType[0:3], originalRequestPortion)
                jsonData = buildJSON(acdcRequestName, task, host, requestData, opts.baseJSON, doSplitting)
                _, jsonFilePath = mkstemp()
                jsonFileHandle = open(jsonFilePath, 'w')
                json.dump(jsonData, jsonFileHandle)
                cwd = os.getcwd()
                command = 'python2.6 %s ' % os.path.join(cwd, reqmgrpy[:-1])
                command += '-u https://cmsweb.cern.ch -c %s -k %s ' % (os.environ['X509_USER_PROXY'],
                                                                       os.environ['X509_USER_PROXY'])
                command += '-f %s ' % jsonFilePath
                command += '--createRequest --assignRequest'
                if doSplitting:
                    command += '--changeSplitting'
                print command
                print jsonFilePath
                jsonFileHandle.close()

if __name__ == '__main__':
    sys.exit(main())
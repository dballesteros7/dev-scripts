"""
__playground.commandRunner__

Created on Apr 17, 2013

@author: dballest
"""
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.StdSpecs.MonteCarlo import MonteCarloWorkloadFactory
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.PhEDEx import XMLDrop
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription
from WMCore.Services.PhEDEx.DataStructs.PhEDExDeletion import PhEDExDeletion

import subprocess
import os
import logging
from WMCore.FwkJobReport.Report import Report
import WMCore.WMBase
from dbs.apis.dbsClient import DbsApi


def something():
    f = os.path.join(WMCore.WMBase.getTestBase(),
                                                               "WMComponent_t/JobAccountant_t/fwjrs",
                                                               "MergeSuccess.pkl")
    x = Report()
    x.load(f)
    x.setAcquisitionProcessing("IansMagicMushroomSoup", 9, "T0Test-AnalyzeThisAndGetAFreePhD-PreScaleThingy10")
    x.setGlobalTag("GT:Super")
    x.setValidStatus("Production")
def getCMSSiteInfo(pattern):
    """
    _getCMSSiteInfo_

    Query SiteDB for the site and SE names matching the pattern.  Return a
    dictionary keyed by site name.
    """
    phedex = PhEDEx( responseType = "json")
    print phedex.subscriptions(dataset = '/HidjetQuenchedMinBias/HiWinter13-PtHat80_STARTHI44_V12-v1/GEN-SIM-RECODEBUG')
    print phedex.subscriptions(dataset = '/MinimumBias/Run2012D-v1/RAW')

def phedex():
    phedexIn = PhEDEx(dict = {'endpoint' : 'https://cmsweb.cern.ch/phedex/datasvc/json/dev/',
                              'logger' : logging},
                    responseType = "json")
#     requests =  phedex.getRequestList(dataset = ['/TauParked/Run2012C-LogError-22Jan2013-v1/RAW-RECO'],
#                           node = 'T2_RU_ITEP')['phedex']['request']
#     for request in requests:
#         requestId = request['id']
#         request =  phedex.getTransferRequests(request = requestId)['phedex']['request']
#         if request:
#             request = request[0]
#             print request
#    x = PhEDExSubscription('/TauParked/Run2012C-22Jan2013-v1/AOD',
#                           'T1_US_FNAL_MSS', 'DataOps', 'dataset', 'low', 'n', 'n', 'n', 'y', subscriptionId = 1)
#   print x.matchesExistingTransferRequest(phedex)
#    print x.matchesExistingSubscription(phedex)

    deletion = PhEDExDeletion('/071103be-7d80-11e0-90de-00163e010039/PromptReco-v1/RECO',
                              'T1_CH_CERN_Buffer',
                              level = 'block',
                              comments = 'Blocks automatically deleted from T2_CH_CERN as it has already been processed and transferred to a custodial location',
                              blocks = {'/071103be-7d80-11e0-90de-00163e010039/PromptReco-v1/RECO' : 
                                        ['/071103be-7d80-11e0-90de-00163e010039/PromptReco-v1/RECO#075ea9e8-7d80-11e0-90de-00163e010039']})
    xmlData = XMLDrop.makePhEDExXMLForBlocks('http://vocms09.cern.ch:8880/cms_dbs_int_local_yy_writer/servlet/DBSServlet',
                                             deletion.getDatasetsAndBlocks())
    print str(xmlData)
    response = phedexIn.delete(deletion, xmlData)
    print response
    requestId = response['phedex']['request_created'][0]['id']
    phedexIn.updateRequest(requestId, 'approve', 'T1_CH_CERN_Buffer')
    
def phedexIt():
    x = PhEDEx(responseType = "json")
    phedexNodes = x.getNodeMap()['phedex']['node']
    phedexMap = {}
    sePhedexMap = {}
    knownPhedexNodes = set()
    for node in phedexNodes:
        phedexMap[node['name']] = node['kind']
        #print '%s -> %s, %s' % (node['name'], node['kind'], node['se'])
        if node['se'] not in sePhedexMap:
            sePhedexMap[node['se']] = set()
        sePhedexMap[node['se']].add(node['name'])
        knownPhedexNodes.add(node['name'])
    y = SiteDBJSON()
    seNames = y.getAllSENames()
    cmsNamesMap = {}
    for se in seNames:
        cmsNames = y.seToCMSName(se)
        cmsNamesMap[se] = cmsNames
    seToNodeMap = {}
    for se in cmsNamesMap:
        candidates = set()
        for cmsName in cmsNamesMap[se]:
            phedexNodes = y.cmsNametoPhEDExNode(cmsName)
            candidates.update(set(phedexNodes))
        validCandidates = set()
        for candidate in candidates:
            if candidate in knownPhedexNodes:
                validCandidates.add(candidate)
        seToNodeMap[se] = validCandidates
        #print '%s to %s' % (se, candidates)
    for se in sePhedexMap:
        if se not in seToNodeMap:
            print "SE: %s is not in new mapping for sites %s" % (se, list(sePhedexMap[se]))
    for se in seToNodeMap:
        if se not in sePhedexMap:
            print "SE: %s is not in old mapping for sites %s" % (se, list(seToNodeMap[se]))
            continue
    for se in set(seToNodeMap.keys()).intersection(set(sePhedexMap.keys())):
        diff = sePhedexMap[se] - seToNodeMap[se]
        if diff:
            print "%s are in old mapping but not in new for %s" %(str(list(diff)), se)
        diff = seToNodeMap[se] - sePhedexMap[se]
        if diff:
            print "%s are in new mapping but not in old for %s" %(str(list(diff)), se)

def dbs3():
    dbsApi = DbsApi(url = 'https://cmsweb-testbed.cern.ch/dbs/prod/global/DBSWriter')
    result = dbsApi.listBlocks(block_name = '/PYTHIA6_Tauola_TTbar_TuneZ2star_14TeV/Summer13-UpgrdPhase2LB4PS_POSTLS261_V2-v1/GEN-SIM#1f7f8d76-c40a-11e2-83c6-003048f0e3f4')
    print result
def main():
    import logging
    logging.getLogger().setLevel('DEBUG')
    phedex()
if __name__ == '__main__':
    main()
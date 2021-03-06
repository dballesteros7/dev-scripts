'''
Created on Feb 14, 2013

@author: dballest
'''

from WMCore.Configuration import Configuration
from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.WorkQueue.WorkQueueUtils import queueConfigFromConfigObject, queueFromConfig
from socket import gethostname
import os
import sys


reqMgrEndpoint = 'https://cmsweb.cern.ch/reqmgr'
wmstatsEndpoint = 'https://cmsweb.cern.ch/couchdb/wmstats'
externalCouchDb = 'https://cmsweb.cern.ch/couchdb'
localCouchDb = 'http://admin:couch@127.0.0.1:5984'
workqueueDBName = 'workqueue'
workqueueInboxDbName = 'workqueue_inbox'
wmstatsDBName = 'wmstats'


def workqueueConfig(couchdb = localCouchDb):
    """
    Returns an usable workqueue config
    """
    config = Configuration()
    config.section_("Agent")
    config.Agent.hostName = gethostname()
    config.component_("WorkQueueManager")
    config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
    config.WorkQueueManager.couchurl = couchdb
    config.WorkQueueManager.dbname = workqueueDBName
    config.WorkQueueManager.wmstatDBName = wmstatsDBName
    config.WorkQueueManager.inboxDatabase = workqueueInboxDbName
    config.WorkQueueManager.level = "GlobalQueue"
    config.WorkQueueManager.queueParams = {'WMStatsCouchUrl': "%s/%s" % (couchdb, wmstatsDBName)}
    config.WorkQueueManager.queueParams['QueueURL'] = '%s/%s' % (couchdb, workqueueDBName)
    config.WorkQueueManager.reqMgrConfig = {}
    config.WorkQueueManager.reqMgrConfig['endpoint'] = reqMgrEndpoint
    
    return config

def getAssignedApprovedWork():
    """
    Split the un-split. Use a local couch for it.
    """
    workStatistics = {}
    wmstatsReader = WMStatsReader(wmstatsEndpoint)
    unAssignedRequests = wmstatsReader.workflowsByStatus(['assignment-approved'], stale = False)

    queueConfig = queueConfigFromConfigObject(workqueueConfig())
    workqueue = queueFromConfig(queueConfig)

    for requestName in unAssignedRequests:
        if 'TEST' in requestName:
            continue
        workqueue.queueWork('%s/reqmgr_workload_cache/%s/spec' % (externalCouchDb, requestName), requestName, 'notreallyateam')

    for requestName in unAssignedRequests:
        workStatistics[requestName] = 0
        workElements = workqueue.backend.getElementsForWorkflow(requestName)
        for element in workElements:
            jobs = element['Jobs']
            workStatistics[requestName] += jobs
        
    return workStatistics

def getAcquiredAvailableWork():
    """
    Get jobs from already split elements that are not yet
    in WMBS (i.e. no jobs created)
    """
    workStatistics = {}
    queueConfig = queueConfigFromConfigObject(workqueueConfig(couchdb = externalCouchDb))
    workqueue = queueFromConfig(queueConfig)
    acquiredAvailableElements = workqueue.backend.getElements(status = 'Acquired')
    acquiredAvailableElements.extend(workqueue.backend.getElements(status = 'Available'))
    
    for element in acquiredAvailableElements:
        workflow = element['ParentQueueId']
        if workflow not in workStatistics:
            workStatistics[workflow] = 0
        workStatistics[workflow] += element['Jobs']
        
    return workStatistics

def filterCampaign(campaign, statistics):
    """
    Filter statistics by campaign
    """
    # Campaign not working at the moment so let's do it by name, partial matching
    goldenNames ="""
vlimant_Winter532012ABTagPrio7_537p5_130122_195138_3798
vlimant_Winter532012ACommissioningPrio7_537p5_130122_195759_3470
vlimant_Winter532012ACosmicsPrio7_537p5_130122_195553_3984
vlimant_Winter532012ADoubleElectron_FNALPrio1_537p5_130122_195023_9113
vlimant_Winter532012ADoubleMuPrio7_537p5_130122_195346_5365
vlimant_Winter532012AElectronHadPrio7_537p5_130122_194943_1308
vlimant_Winter532012AHcalNZSPrio7_537p5_130122_195706_710
vlimant_Winter532012AHTPrio7_537p5_130122_195045_9421
vlimant_Winter532012AJet_IN2P3Prio2_537p5_130122_195230_6939
vlimant_Winter532012AMETPrio7_537p5_130122_195839_3121
vlimant_Winter532012AMinimumBias_CERNPrio1_537p5_130122_195407_5202
vlimant_Winter532012AMuEG_PICPrio1_537p5_130122_195159_5682
vlimant_Winter532012AMuHadPrio7_537p5_130122_195032_6194
vlimant_Winter532012AMultiJetPrio7_537p5_130122_195719_638
vlimant_Winter532012AMuOniaPrio7_537p5_130122_195900_7343
vlimant_Winter532012APhotonHadPrio7_537p5_130122_195122_5811
vlimant_Winter532012APhoton_KITPrio1_537p5_130122_195507_9939
vlimant_Winter532012ASingleElectronPrio7_537p5_130122_195332_7255
vlimant_Winter532012ASingleMu_CNAFPrio2_537p5_130122_195742_9786
vlimant_Winter532012ATauPlusXPrio7_537p5_130122_195244_5999
vlimant_Winter532012ATauPrio7_537p5_130122_195451_9359
vlimant_Winter532012BBJetPlusXPrio7_537p5_130122_195052_7469
vlimant_Winter532012BBTagPrio7_537p5_130122_195251_9076
vlimant_Winter532012BCommissioningPrio7_537p5_130122_195821_1288
vlimant_Winter532012BCosmicsPrio7_537p5_130122_195237_9340
vlimant_Winter532012BDoubleElectron_FNALPrio1_537p5_130122_195806_2852
vlimant_Winter532012BDoubleMuParked_CNAFPrio1_537p5_130122_195131_1641
vlimant_Winter532012BDoublePhotonHighPtPrio7_537p5_130122_194934_7894
vlimant_Winter532012BDoublePhoton_IN2P3Prio1_537p5_130122_195523_285
vlimant_Winter532012BElectronHadPrio7_537p5_130122_195339_3470
vlimant_Winter532012BHcalNZSPrio7_537p5_130122_195415_2739
vlimant_Winter532012BHTMHTParked_FNALPrio3_537p5_130122_195712_6451
vlimant_Winter532012BJetHT_FNALPrio2_537p5_130122_195617_9652
vlimant_Winter532012BJetMonPrio7_537p5_130122_195100_2815
vlimant_Winter532012BMETPrio7_537p5_130122_195846_9659
vlimant_Winter532012BMinimumBias_CERNPrio1_537p5_130122_195609_4089
vlimant_Winter532012BMuEG_PICPrio1_537p5_130122_195429_586
vlimant_Winter532012BMuHadPrio7_537p5_130122_195658_604
vlimant_Winter532012BMuOniaParked_ASGCPrio1_537p5_130122_195107_1235
vlimant_Winter532012BMuOniaPrio7_537p5_130122_195915_147
vlimant_Winter532012BNoBPTXPrio7_537p5_130122_195152_1786
vlimant_Winter532012BPhotonHadPrio7_537p5_130122_195538_3246
vlimant_Winter532012BSingleElectronPrio7_537p5_130122_195750_3248
vlimant_Winter532012BSingleMu_CNAFPrio2_537p5_130122_195853_9525
vlimant_Winter532012BSinglePhoton_KITPrio2_537p5_130122_195115_3752
vlimant_Winter532012BTauParked_RALPrio1_537p5_130122_195422_373
vlimant_Winter532012BTauPlusXPrio7_537p5_130122_195400_7271
vlimant_Winter532012BVBF1Parked_FNALPrio5_537p5_130122_195354_1258
vlimant_Winter532012CBJetPlusXPrio7_537p5_130122_195813_8718
vlimant_Winter532012CBTagPrio7_537p5_130122_195602_3841
vlimant_Winter532012CCommissioningPrio7_537p5_130122_195734_5730
vlimant_Winter532012CCosmicsPrio7_537p5_130122_194955_1226
vlimant_Winter532012CDoubleElectron_FNALPrio1_537p5_130122_195207_4395
vlimant_Winter532012CDoubleMuParked_CNAFPrio1_537p5_130122_195017_1947
vlimant_Winter532012CDoublePhotonHighPtPrio7_537p5_130122_195921_7606
vlimant_Winter532012CDoublePhoton_IN2P3Prio1_537p5_130122_195144_7212
vlimant_Winter532012CElectronHadPrio7_537p5_130122_195436_6838
vlimant_Winter532012CHcalNZSPrio7_537p5_130122_195531_1665
vlimant_Winter532012CHTMHTParked_FNALPrio6_537p5_130122_195216_5283
vlimant_Winter532012CJetHT_FNALPrio2_537p5_130122_195546_898
vlimant_Winter532012CJetMonPrio7_537p5_130122_195515_7397
vlimant_Winter532012CLP_ExclEGMUPrio7_537p5_130122_195308_9273
vlimant_Winter532012CLP_Jets1Prio7_537p5_130122_195319_5842
vlimant_Winter532012CLP_Jets2Prio7_537p5_130122_194924_8551
vlimant_Winter532012CLP_MinBias1Prio7_537p5_130122_194916_7992
vlimant_Winter532012CLP_MinBias2Prio7_537p5_130122_195010_1363
vlimant_Winter532012CLP_MinBias3Prio7_537p5_130122_195635_8619
vlimant_Winter532012CLP_RomanPotsPrio7_537p5_130122_195624_7574
vlimant_Winter532012CLP_ZeroBiasPrio7_537p5_130122_195630_2407
vlimant_Winter532012CMETPrio7_537p5_130122_195325_5323
vlimant_Winter532012CMinimumBias_CERNPrio1_537p5_130122_195649_9515
vlimant_Winter532012CMuEG_PICPrio1_537p5_130122_195829_7352
vlimant_Winter532012CMuHadPrio7_537p5_130122_195003_8084
vlimant_Winter532012CMuOniaParked_ASGCPrio1_537p5_130122_195500_3526
vlimant_Winter532012CMuOniaPrio7_537p5_130122_194852_9195
vlimant_Winter532012CNoBPTXPrio7_537p5_130122_195223_5386
vlimant_Winter532012CPhotonHadPrio7_537p5_130122_195444_1880
vlimant_Winter532012CSingleElectronPrio7_537p5_130122_195302_5784
vlimant_Winter532012CSingleMu_CNAFPrio2_537p5_130122_195641_8218
vlimant_Winter532012CSinglePhoton_KITPrio2_537p5_130122_195907_6678
vlimant_Winter532012CTauParked_RALPrio1_537p5_130122_195039_5904
vlimant_Winter532012CTauPlusXPrio7_537p5_130122_195726_3861
vlimant_Winter532012CVBF1Parked_FNALPrio5_537p5_130122_194900_6719
vlimant_Winter532012ABTagPrio7_537p5_130122_195138_3798
vlimant_Winter532012ACommissioningPrio7_537p5_130122_195759_3470
vlimant_Winter532012ACosmicsPrio7_537p5_130122_195553_3984
vlimant_Winter532012ADoubleElectron_FNALPrio1_537p5_130122_195023_9113
vlimant_Winter532012ADoubleMuPrio7_537p5_130122_195346_5365
vlimant_Winter532012AElectronHadPrio7_537p5_130122_194943_1308
vlimant_Winter532012AHcalNZSPrio7_537p5_130122_195706_710
vlimant_Winter532012AHTPrio7_537p5_130122_195045_9421
vlimant_Winter532012AJet_IN2P3Prio2_537p5_130122_195230_6939
vlimant_Winter532012AMETPrio7_537p5_130122_195839_3121
vlimant_Winter532012AMinimumBias_CERNPrio1_537p5_130122_195407_5202
vlimant_Winter532012AMuEG_PICPrio1_537p5_130122_195159_5682
vlimant_Winter532012AMuHadPrio7_537p5_130122_195032_6194
vlimant_Winter532012AMultiJetPrio7_537p5_130122_195719_638
vlimant_Winter532012AMuOniaPrio7_537p5_130122_195900_7343
vlimant_Winter532012APhotonHadPrio7_537p5_130122_195122_5811
vlimant_Winter532012APhoton_KITPrio1_537p5_130122_195507_9939
vlimant_Winter532012ASingleElectronPrio7_537p5_130122_195332_7255
vlimant_Winter532012ASingleMu_CNAFPrio2_537p5_130122_195742_9786
vlimant_Winter532012ATauPlusXPrio7_537p5_130122_195244_5999
vlimant_Winter532012ATauPrio7_537p5_130122_195451_9359
vlimant_Winter532012BBJetPlusXPrio7_537p5_130122_195052_7469
vlimant_Winter532012BBTagPrio7_537p5_130122_195251_9076
vlimant_Winter532012BCommissioningPrio7_537p5_130122_195821_1288
vlimant_Winter532012BCosmicsPrio7_537p5_130122_195237_9340
vlimant_Winter532012BDoubleElectron_FNALPrio1_537p5_130122_195806_2852
vlimant_Winter532012BDoubleMuParked_CNAFPrio1_537p5_130122_195131_1641
vlimant_Winter532012BDoublePhotonHighPtPrio7_537p5_130122_194934_7894
vlimant_Winter532012BDoublePhoton_IN2P3Prio1_537p5_130122_195523_285
vlimant_Winter532012BElectronHadPrio7_537p5_130122_195339_3470
vlimant_Winter532012BHcalNZSPrio7_537p5_130122_195415_2739
vlimant_Winter532012BHTMHTParked_FNALPrio3_537p5_130122_195712_6451
vlimant_Winter532012BJetHT_FNALPrio2_537p5_130122_195617_9652
vlimant_Winter532012BJetMonPrio7_537p5_130122_195100_2815
vlimant_Winter532012BMETPrio7_537p5_130122_195846_9659
vlimant_Winter532012BMinimumBias_CERNPrio1_537p5_130122_195609_4089
vlimant_Winter532012BMuEG_PICPrio1_537p5_130122_195429_586
vlimant_Winter532012BMuHadPrio7_537p5_130122_195658_604
vlimant_Winter532012BMuOniaParked_ASGCPrio1_537p5_130122_195107_1235
vlimant_Winter532012BMuOniaPrio7_537p5_130122_195915_147
vlimant_Winter532012BNoBPTXPrio7_537p5_130122_195152_1786
vlimant_Winter532012BPhotonHadPrio7_537p5_130122_195538_3246
vlimant_Winter532012BSingleElectronPrio7_537p5_130122_195750_3248
vlimant_Winter532012BSingleMu_CNAFPrio2_537p5_130122_195853_9525
vlimant_Winter532012BSinglePhoton_KITPrio2_537p5_130122_195115_3752
vlimant_Winter532012BTauParked_RALPrio1_537p5_130122_195422_373
vlimant_Winter532012BTauPlusXPrio7_537p5_130122_195400_7271
vlimant_Winter532012BVBF1Parked_FNALPrio5_537p5_130122_195354_1258
vlimant_Winter532012CBJetPlusXPrio7_537p5_130122_195813_8718
vlimant_Winter532012CBTagPrio7_537p5_130122_195602_3841
vlimant_Winter532012CCommissioningPrio7_537p5_130122_195734_5730
vlimant_Winter532012CCosmicsPrio7_537p5_130122_194955_1226
vlimant_Winter532012CDoubleElectron_FNALPrio1_537p5_130122_195207_4395
vlimant_Winter532012CDoubleMuParked_CNAFPrio1_537p5_130122_195017_1947
vlimant_Winter532012CDoublePhotonHighPtPrio7_537p5_130122_195921_7606
vlimant_Winter532012CDoublePhoton_IN2P3Prio1_537p5_130122_195144_7212
vlimant_Winter532012CElectronHadPrio7_537p5_130122_195436_6838
vlimant_Winter532012CHcalNZSPrio7_537p5_130122_195531_1665
vlimant_Winter532012CHTMHTParked_FNALPrio6_537p5_130122_195216_5283
vlimant_Winter532012CJetHT_FNALPrio2_537p5_130122_195546_898
vlimant_Winter532012CJetMonPrio7_537p5_130122_195515_7397
vlimant_Winter532012CLP_ExclEGMUPrio7_537p5_130122_195308_9273
vlimant_Winter532012CLP_Jets1Prio7_537p5_130122_195319_5842
vlimant_Winter532012CLP_Jets2Prio7_537p5_130122_194924_8551
vlimant_Winter532012CLP_MinBias1Prio7_537p5_130122_194916_7992
vlimant_Winter532012CLP_MinBias2Prio7_537p5_130122_195010_1363
vlimant_Winter532012CLP_MinBias3Prio7_537p5_130122_195635_8619
vlimant_Winter532012CLP_RomanPotsPrio7_537p5_130122_195624_7574
vlimant_Winter532012CLP_ZeroBiasPrio7_537p5_130122_195630_2407
vlimant_Winter532012CMETPrio7_537p5_130122_195325_5323
vlimant_Winter532012CMinimumBias_CERNPrio1_537p5_130122_195649_9515
vlimant_Winter532012CMuEG_PICPrio1_537p5_130122_195829_7352
vlimant_Winter532012CMuHadPrio7_537p5_130122_195003_8084
vlimant_Winter532012CMuOniaParked_ASGCPrio1_537p5_130122_195500_3526
vlimant_Winter532012CMuOniaPrio7_537p5_130122_194852_9195
vlimant_Winter532012CNoBPTXPrio7_537p5_130122_195223_5386
vlimant_Winter532012CPhotonHadPrio7_537p5_130122_195444_1880
vlimant_Winter532012CSingleElectronPrio7_537p5_130122_195302_5784
vlimant_Winter532012CSingleMu_CNAFPrio2_537p5_130122_195641_8218
vlimant_Winter532012CSinglePhoton_KITPrio2_537p5_130122_195907_6678
vlimant_Winter532012CTauParked_RALPrio1_537p5_130122_195039_5904
vlimant_Winter532012CTauPlusXPrio7_537p5_130122_195726_3861
vlimant_Winter532012CVBF1Parked_FNALPrio5_537p5_130122_194900_6719
"""
    filteredStatistics = 0
    for name in statistics:
        core = '_'.join(name.split('_')[1:-3])
        if core in goldenNames:
            filteredStatistics += statistics[name]
            print name
    return filteredStatistics

def main():
    assignedStatistics = getAssignedApprovedWork()
    acquiredAvailableStatistics = getAcquiredAvailableWork()
    
    print "Global Statistics:"
    print "Assignment-Approved jobs: %d" % sum(assignedStatistics.values())
    print "Assigned/Acquired jobs: %d" % sum(acquiredAvailableStatistics.values())
    print "22Jan2013 Campaign Statistics:"
    assignedCampaignStatistics = filterCampaign(None, assignedStatistics)
    acquiredCampaignStatistics = filterCampaign(None, acquiredAvailableStatistics)
    print "Assignment-Approved jobs: %d" % assignedCampaignStatistics
    print "Assigned/Acquired jobs: %d" % acquiredCampaignStatistics

        

if __name__ == '__main__':
    sys.exit(main())
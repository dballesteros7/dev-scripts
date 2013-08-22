'''
Created on Nov 26, 2012

@author: dballest
'''
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow
from WMCore.Services.UUID import makeUUID
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Job import Job
from WMCore.WMBS.File import File
from WMQuality.TestInit import TestInit
import threading
import unittest
import os
import pickle




class Test(unittest.TestCase):


    def setUp(self):

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection( destroyAllDatabase = True )
        self.testInit.setSchema(customModules = ["WMCore.WMBS", "WMCore.BossAir"],
                                useDefault = False)
        
        self.testDir = self.testInit.generateWorkDir()
        self.sites = ["somese"]
        
        myThread = threading.currentThread()        

        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.baDaoFactory =  DAOFactory(package = "WMCore.BossAir",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        return


    def tearDown(self):
        #self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        return
    
    
    def createJobGroups(self, nSubs, nJobs, task, workloadSpec, site = None, bl = [], wl = []):
        """
        Creates a series of jobGroups for submissions

        """

        jobGroupList = []

        testWorkflow = Workflow(spec = workloadSpec, owner = "mnorman",
                                name = makeUUID(), task="basicWorkload/Production",
                                owner_vogroup = 'phgroup', owner_vorole = 'cmsrole')
        testWorkflow.create()

        # Create subscriptions
        for i in range(nSubs):

            name = makeUUID()

            # Create Fileset, Subscription, jobGroup
            testFileset = Fileset(name = name)
            testFileset.create()
            testSubscription = Subscription(fileset = testFileset,
                                            workflow = testWorkflow,
                                            type = "Processing",
                                            split_algo = "FileBased")
            testSubscription.create()

            testJobGroup = JobGroup(subscription = testSubscription)
            testJobGroup.create()


            # Create jobs
            self.makeNJobs(name = name, task = task,
                           nJobs = nJobs,
                           jobGroup = testJobGroup,
                           fileset = testFileset,
                           sub = testSubscription.exists(),
                           site = site, bl = bl, wl = wl)



            testFileset.commit()
            testJobGroup.commit()
            jobGroupList.append(testJobGroup)

        return jobGroupList


    def makeNJobs(self, name, task, nJobs, jobGroup, fileset, sub, site = None, bl = [], wl = []):
        """
        _makeNJobs_

        Make and return a WMBS Job and File
        This handles all those damn add-ons

        """
        # Set the CacheDir
        cacheDir = os.path.join(self.testDir, 'CacheDir')

        for n in range(nJobs):
            # First make a file
            #site = self.sites[0]
            testFile = File(lfn = "/singleLfn/%s/%s" %(name, n),
                            size = 1024, events = 10)
            if site:
                testFile.setLocation(site)
            else:
                for tmpSite in self.sites:
                    testFile.setLocation('%s' % (tmpSite))
            testFile.create()
            fileset.addFile(testFile)


        fileset.commit()

        index = 0
        for f in fileset.files:
            index += 1
            testJob = Job(name = '%s-%i' %(name, index))
            testJob.addFile(f)
            testJob["location"]  = f.getLocations()[0]
            testJob['custom']['location'] = f.getLocations()[0]
            testJob['task']    = task
            testJob['sandbox'] = task
            testJob['spec']    = os.path.join(self.testDir, 'basicWorkload.pcl')
            testJob['mask']['FirstEvent'] = 101
            testJob['owner']   = 'mnorman'
            testJob["siteBlacklist"] = bl
            testJob["siteWhitelist"] = wl
            testJob['ownerDN'] = 'mnorman'
            testJob['ownerRole'] = 'cmsrole'
            testJob['ownerGroup'] = 'phgroup'

            jobCache = os.path.join(cacheDir, 'Sub_%i' % (sub), 'Job_%i' % (index))
            os.makedirs(jobCache)
            testJob.create(jobGroup)
            testJob['cache_dir'] = jobCache
            testJob.save()
            jobGroup.add(testJob)
            output = open(os.path.join(jobCache, 'job.pkl'),'w')
            pickle.dump(testJob, output)
            output.close()

        return testJob, testFile

    def testA(self):
        """
        This will give us a:
        
        TypeError: expecting numeric data
        """
        
        newBossAirState = self.baDaoFactory(classname = "NewState")
        
        states = ["Dead", "Alive", "Zombie", "Reanimated"]
        
        newBossAirState.execute(states)
        
        newUserDAO = self.daoFactory(classname = "Users.New")
        
        user = {'dn' : '/DC=mop/DC=azeroth/OU=Organic Units/OU=Users/CN=chen/CN=Chen Stormstout', 'hn' : 'chen',
                'owner' : 'chen', 'group' : 'brewmasters', 'group_name' : 'DEFAULT', 'role_name' : 'DEFAULT'}

        newUserDAO.execute(**user)
        
        newLocationDAO = self.daoFactory(classname = "Locations.New")
        
        location = {'siteName' : 'SomeSite', 'runningSlots' : 10, 'pendingSlots' : 20, 'seName' : 'somese', 'ceName' : 'yeahsomece',
                    'plugin' : 'THEplugin', 'cmsName' : 'notforthisone'}

        newLocationDAO.execute(**location)
        
        jobOne = {'jobid' : 1, 'gridid' : None, 'bulkid' : None, 'status' : 'Alive' , 'retry_count' : 0,
                  'userdn' : '/DC=mop/DC=azeroth/OU=Organic Units/OU=Users/CN=chen/CN=Chen Stormstout',
                  'usergroup' : 'DEFAULT', 'userrole'  : 'DEFAULT', 'siteName' : 'SomeSite'}
        
        jobTwo = {'jobid' : 2, 'gridid' : None, 'bulkid' : None, 'status' : 'Alive' , 'retry_count' : 0,
                  'userdn' : '/DC=mop/DC=azeroth/OU=Organic Units/OU=Users/CN=chen/CN=Chen Stormstout',
                  'usergroup' : 'DEFAULT', 'userrole'  : 'DEFAULT', 'siteName' : 'SomeSite'}
        
        self.createJobGroups(1, 2, "SomeTask", "Spec.xml")

        newRunJobDao = self.baDaoFactory(classname = "NewJobs")
        newRunJobDao.execute([jobOne, jobTwo])
        
        
        
        updateJobDAO = self.baDaoFactory(classname = "UpdateJobs")
        
        jobOneUpdate = {'jobid' : 1, 'gridid' : None, 'bulkid' : None, 'status' : 'Dead' , 'retry_count' : 0,
                  'userdn' : '/DC=mop/DC=azeroth/OU=Organic Units/OU=Users/CN=chen/CN=Chen Stormstout',
                  'usergroup' : 'DEFAULT', 'userrole'  : 'DEFAULT', 'id' : 1, 'status_time' : '0'}
        
        jobTwoUpdate = {'jobid' : 2, 'gridid' : None, 'bulkid' : None, 'status' : 'Dead' , 'retry_count' : 0,
                  'userdn' : '/DC=mop/DC=azeroth/OU=Organic Units/OU=Users/CN=chen/CN=Chen Stormstout',
                  'usergroup' : 'DEFAULT', 'userrole'  : 'DEFAULT', 'id' : 2, 'status_time' : 0}

        updateJobDAO.execute([jobTwoUpdate, jobOneUpdate])
        
    def testB(self):
        """
        This will give us a:
        
        TypeError: expecting string, unicode or buffer object
        """
        
        newBossAirState = self.baDaoFactory(classname = "NewState")
        
        states = ["Dead", "Alive", "Zombie", "Reanimated"]
        
        newBossAirState.execute(states)
        
        newUserDAO = self.daoFactory(classname = "Users.New")
        
        user = {'dn' : '/DC=mop/DC=azeroth/OU=Organic Units/OU=Users/CN=chen/CN=Chen Stormstout', 'hn' : 'chen',
                'owner' : 'chen', 'group' : 'brewmasters', 'group_name' : 'DEFAULT', 'role_name' : 'DEFAULT'}

        newUserDAO.execute(**user)
        
        newLocationDAO = self.daoFactory(classname = "Locations.New")
        
        location = {'siteName' : 'SomeSite', 'runningSlots' : 10, 'pendingSlots' : 20, 'seName' : 'somese', 'ceName' : 'yeahsomece',
                    'plugin' : 'THEplugin', 'cmsName' : 'notforthisone'}

        newLocationDAO.execute(**location)
        
        jobOne = {'jobid' : 1, 'gridid' : None, 'bulkid' : None, 'status' : 'Alive' , 'retry_count' : 0,
                  'userdn' : '/DC=mop/DC=azeroth/OU=Organic Units/OU=Users/CN=chen/CN=Chen Stormstout',
                  'usergroup' : 'DEFAULT', 'userrole'  : 'DEFAULT', 'siteName' : 'SomeSite'}
        
        jobTwo = {'jobid' : 2, 'gridid' : None, 'bulkid' : None, 'status' : 'Alive' , 'retry_count' : 0,
                  'userdn' : '/DC=mop/DC=azeroth/OU=Organic Units/OU=Users/CN=chen/CN=Chen Stormstout',
                  'usergroup' : 'DEFAULT', 'userrole'  : 'DEFAULT', 'siteName' : 'SomeSite'}
        
        self.createJobGroups(1, 2, "SomeTask", "Spec.xml")

        newRunJobDao = self.baDaoFactory(classname = "NewJobs")
        newRunJobDao.execute([jobOne, jobTwo])
        
        
        
        updateJobDAO = self.baDaoFactory(classname = "UpdateJobs")
        
        jobOneUpdate = {'jobid' : 1, 'gridid' : None, 'bulkid' : None, 'status' : 'Dead' , 'retry_count' : 0,
                  'userdn' : '/DC=mop/DC=azeroth/OU=Organic Units/OU=Users/CN=chen/CN=Chen Stormstout',
                  'usergroup' : 'DEFAULT', 'userrole'  : 'DEFAULT', 'id' : 1, 'status_time' : '0'}
        
        jobTwoUpdate = {'jobid' : 2, 'gridid' : None, 'bulkid' : None, 'status' : 'Dead' , 'retry_count' : 0,
                  'userdn' : '/DC=mop/DC=azeroth/OU=Organic Units/OU=Users/CN=chen/CN=Chen Stormstout',
                  'usergroup' : 'DEFAULT', 'userrole'  : 'DEFAULT', 'id' : 2, 'status_time' : 0}

        updateJobDAO.execute([jobOneUpdate, jobTwoUpdate])

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
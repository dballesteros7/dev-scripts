#!/usr/bin/evn python
'''
Created on Oct 28, 2012

@author: dballest
'''

import sys
import os
import re
from WMCore.Storage.SiteLocalConfig import SiteLocalConfig

def main():
    T1s = {}
    T2s = {}
    T3s = {}
    siteConfPath = '/home/dballest/Dev-Workspace/SITECONF/'
    siteList = os.listdir(siteConfPath)
    for site in siteList:
        if re.match('^T[1-3]_[A-Z]{2}_[A-Za-z]+$', site):
            siteLocalConfig = os.path.join(siteConfPath, site, 'JobConfig/site-local-config.xml')
            if os.path.exists(siteLocalConfig):
                siteConfig = SiteLocalConfig(siteLocalConfig)
                siteConfig.read()
                localStageoutCommand =  siteConfig.localStageOutCommand()
                tier = {}
                if re.match('^T[1]_.*$', site):
                    tier = T1s
                elif re.match('^T[2]_.*$', site):
                    tier = T2s
                else:
                    tier = T3s
                if localStageoutCommand in tier:
                    tier[localStageoutCommand] += 1
                else:
                    tier[localStageoutCommand] = 1
    print T1s
    print T2s
    print T3s
                
    

if __name__ == '__main__':
    sys.exit(main())
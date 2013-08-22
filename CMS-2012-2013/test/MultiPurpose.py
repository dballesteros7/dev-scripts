"""
__MultiPurpose__

Created on Mar 28, 2013

@author: dballest
"""
import logging
import sys
from WMCore.WorkQueue.DataLocationMapper import DataLocationMapper
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON

from WMCore.Services.EmulatorSwitch import EmulatorHelper

def main():
    logging.getLogger().setLevel(logging.DEBUG)
    #EmulatorHelper.setEmulators(dbs = True, siteDB = True)
    x = PhEDEx()
    y = SiteDBJSON()
#     mapperArgs = {'phedex' : x, 'sitedb' : y, 'locationFrom' : 'location'}
#     mapper = DataLocationMapper(**mapperArgs)
#     m = mapper([{'dbs_url' : 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet',
#                   'name' : '/SingleMu/Run2012C-v1/RAW#f4a88fc6-f8a8-11e1-847a-842b2b4671d8'}])[0].values()[0]
#     for a,b in m.items():
#         print a
#         print b
#     m = mapper([{'dbs_url' : 'http://cmsdbsprod.cern.ch/cms_dbs_caf_analysis_01/servlet/DBSServlet',
#                   'name' : '/SingleMu/Run2012C-v1/RAW#f4a88fc6-f8a8-11e1-847a-842b2b4671d8'}])[0].values()[0]
#     for a,b in m.items():
#         print a
#         print b
#     m = mapper([{'dbs_url' : 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet',
#                   'name' : '/SingleMu/Run2012C-v1/RAW'}], datasetSearch = True)[0].values()[0]
#     for a,b in m.items():
#         print a
#         print b
#     m = mapper([{'dbs_url' : 'http://cmsdbsprod.cern.ch/cms_dbs_caf_analysis_01/servlet/DBSServlet',
#                   'name' : '/SingleMu/Run2012C-v1/RAW'}], datasetSearch = True)[0].values()[0]
#     for a,b in m.items():
#         print a
#         print b
    #print mapper.locationsFromPhEDEx(['/SingleMu/Run2012C-v1/RAW#3ab70444-c1e4-11e1-9597-842b2b4671d8'], True, False)[0]
    #print mapper.locationsFromPhEDEx(['/SingleMu/Run2012C-v1/RAW'], True, True)[0]
    mapperArgs = {'phedex' : x, 'sitedb' : y}
    mapper = DataLocationMapper(**mapperArgs)
    #print mapper.locationsFromPhEDEx(['/SingleMu/Run2012C-v1/RAW#3ab70444-c1e4-11e1-9597-842b2b4671d8'], True, False)[0]
    #print mapper.locationsFromPhEDEx(['/SingleMu/Run2012C-v1/RAW'], True, True)[0]
    
    m = mapper([{'dbs_url' : 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet',
                  'name' : '/SingleMu/Run2012C-v1/RAW#f4a88fc6-f8a8-11e1-847a-842b2b4671d8'}])[0].values()[0]
    for a,b in m.items():
        print a
        print b
    m = mapper([{'dbs_url' : 'http://cmsdbsprod.cern.ch/cms_dbs_caf_analysis_01/servlet/DBSServlet',
                  'name' : '/SingleMu/Run2012C-v1/RAW#f4a88fc6-f8a8-11e1-847a-842b2b4671d8'}])[0].values()[0]
    for a,b in m.items():
        print a
        print b
    m = mapper([{'dbs_url' : 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet',
                  'name' : '/SingleMu/Run2012C-v1/RAW'}], datasetSearch = True)[0].values()[0]
    for a,b in m.items():
        print a
        print b
    m = mapper([{'dbs_url' : 'http://cmsdbsprod.cern.ch/cms_dbs_caf_analysis_01/servlet/DBSServlet',
                  'name' : '/SingleMu/Run2012C-v1/RAW'}], datasetSearch = True)[0].values()[0]
    for a,b in m.items():
        print a
        print b

if __name__ == '__main__':
    sys.exit(main())
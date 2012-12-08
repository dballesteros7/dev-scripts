'''
Created on Oct 23, 2012

@author: dballest
'''

import sys
from WMCore.Database.CMSCouch import Database

def main():
    
    globalwq = Database('workloadsummary_testdisplay', 'https://dballesteros.iriscouch.com')
    x = globalwq.document("an_id")
    print x['performance']['/linacre_ACDC2_ReReco13JulHT_120723_102457_7693_120810_203338_8896/DataProcessing']['cmsRun1']
    

if __name__ == '__main__':
    sys.exit(main())
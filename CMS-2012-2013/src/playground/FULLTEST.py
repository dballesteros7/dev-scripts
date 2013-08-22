"""
__playground.FULLTEST__

Created on Apr 3, 2013

@author: dballest
"""

import sys

from WMCore.Services.SiteDB.SiteDB import SiteDBJSON

def main():
    x = SiteDBJSON()
    print x.cmsNametoList('T1*', 'SE', file = 'result.json')
    print x.cmsNametoList('T1*', 'CE', file = 'result.json')


if __name__ == '__main__':
    sys.exit(main())
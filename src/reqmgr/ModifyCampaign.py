'''
_ModifyCampaign_

Utility script to modify the campaign in WMStats

Created on Feb 18, 2013

@author: dballest
'''

import sys
from optparse import OptionParser

try:
    from WMCore import __version__
    sys.stdout.write('Using WMCore %s\n' % __version__)
    from WMCore.Database.CMSCouch import CouchServer
except ImportError:
    sys.stderr.write('ERROR: Source WMCore libraries before using this script\n')
    sys.stderr.write('ERROR: Environment script: /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh\n')
    sys.exit(1)

def main():
    myOptParser = OptionParser()
    myOptParser.add_option('-f', '--file', dest = 'inputFile',
                           help = 'Input file with the campaing mapping')
    myOptParser.add_option('-u', '--url', dest = 'url',
                           help = 'WMStats URL',
                           default = 'https://cmsweb.cern.ch/couchdb')
    opts, _ = myOptParser.parse_args()
    campaignMapping = open(opts.inputFile, 'r')
    currentCampaign = None
    mapping  = {}
    for line in campaignMapping:
        line = line.strip()
        if line.startswith('[') and line.endswith(']'):
            campaign = line.strip('[').strip(']')
            if campaign:
                currentCampaign = campaign
        elif line:
            if currentCampaign is not None:
                if currentCampaign not in mapping:
                    mapping[currentCampaign] = []
                mapping[currentCampaign].append(line)
    
    if mapping:
        server = CouchServer(dburl = opts.url)
        database = server.connectDatabase('wmstats', False)
        for campaign in mapping:
            for request in mapping[campaign]:
                reqDoc = database.document(request)
                oldCampaign = reqDoc.get('campaign', None)
                reqDoc['campaign'] = campaign
                database.queue(reqDoc)
                print "Changing campaign from %s to %s for %s" % (oldCampaign, campaign, request)
        database.commit()

    campaignMapping.close()



if __name__ == '__main__':
    sys.exit(main())
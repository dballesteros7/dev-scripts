'''
Created on Dec 11, 2012

@author: dballest
'''

import sys
import traceback
import re
import subprocess
import tempfile

regex = re.compile(r'^.*/store/unmerged/logs/prod/([0-9]{4})/([0-9]{1,2})/[0-9]{1,2}/PromptReco-Run[0-9]{6}.*$') 

def main():
    # We need the list of CASTOR files
    castorDumpPath = sys.argv[1]
    try:
        castorDump = open(castorDumpPath, 'r')
        sortedStructure = {}
        for line in castorDump:
            matchTokens = regex.match(line)
            if matchTokens:
                tokens = matchTokens.groups()
                year = tokens[0]
                month = tokens[1]
                if int(year) < 2012:
                    # 2011 was logCollected, I trust it :)
                    continue
                # Verify that 2012 data was migrated to EOS and therefore logCollected
                if month not in sortedStructure:
                    sortedStructure[month] = set()
                sortedStructure[month].add(line.replace('/castor/cern.ch', '/eos'))
        print "Finished structuring castor data"
        print sortedStructure.keys()
        for month in sortedStructure:
            # Get eos listing for the month
            print "Starting to process %s" % month
            (tempFileFd, tempFilePath) = tempfile.mkstemp()
            subprocess.check_call(['/afs/cern.ch/project/eos/installation/0.2.5/bin/eos.select','find', '-f', '/store/unmerged/logs/prod/2012/%s' % month],
                                           stdout = tempFileFd) 
            
            tempFileHandle = open(tempFilePath, 'r')
            tempFileHandle.seek(0)
            eosData = set()
            for eosLine in tempFileHandle:
                if regex.match(eosLine):
                    eosData.add(eosLine)
            tempFileHandle.close()
            print "Finished processing EOS listing, found %d files" % len(eosData) 
            for castorFile in sortedStructure[month]:
                # every castor that looks like PromptReco must be in EOS
                if castorFile not in eosData:
                    print '%s is missing from EOS' % castorFile[:-1]
            else:
                print "%s month finished without issue" % month
    except:
        traceback.print_exc()
    finally:
        castorDump.close()
        
    return 0

if __name__ == '__main__':
    sys.exit(main())
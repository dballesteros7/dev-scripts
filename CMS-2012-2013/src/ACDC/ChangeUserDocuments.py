"""
Created on Sep 27, 2012

@author: dballest
"""

from WMCore.Database.CMSCouch import Database
from optparse import OptionParser
import sys
import json

def readACDCInput(inputFile):
    """
    _readACDCInput_
    
    This expects a text file JSON-compatible with the following structure:
    
    [ {'owner' : <owner>, 'group' : <group>, 'collection_name' : <collection_name>,
       'fileset_name' : <fileset_name>, "original_dn" : <original_dn>}, ]
    
    It just parses it out and returns it
    """
    try:
        handle = open(inputFile, 'r')
        acdcList = json.load(handle)
        return acdcList
    finally:
        handle.close()
    return []

def readUsersMap(inputFile):
    """
    _readACDCInput_
    
    This expects a text file JSON-compatible with the following structure:
    
    {<dn> : [<owner>,<group>],}
    
    It just parses it out and returns it
    """
    try:
        handle = open(inputFile, 'r')
        usersMap = json.load(handle)
        return usersMap
    finally:
        handle.close()
    return {}

def main():
    
    parser = OptionParser()
    parser.add_option("-f", "--input-acdc", dest="acdcList")
    parser.add_option("-m", "--input-mapfile", dest="mapFile")
    parser.add_option("-u", "--url", dest="url")
    parser.add_option("-d", "--dry-run", dest="dryRun",
                      action="store_true", default=False)
    parser.add_option("-l", "--log-file", dest="logFile")

    (options, _) = parser.parse_args()
    
    handle = open(options.logFile, 'w')
    
    url = options.url
    database = 'wmagent_acdc'
    acdcDB = Database(database, url)
    handle.write('Opening ACDC database in %s/%s\n' % (url, database))
    
    inputACDC = readACDCInput(options.acdcList)
    usersMap = readUsersMap(options.mapFile)
    handle.write('Have %d workflows to fix\n' % len(inputACDC))
    handle.write('=================================================================\n')
    for workflow in inputACDC:
        collection_name = workflow['collection_name']
        fileset_name = workflow['fileset_name']
        original_dn = workflow['original_dn']
        handle.write('Original workflow: %s\n' % collection_name)
        handle.write('Original task: %s\n' % fileset_name)
        handle.write('Original owner DN: %s\n' % original_dn)
        if original_dn in usersMap:
            handle.write('This DN maps to %s-%s\n' % (usersMap[original_dn][1], usersMap[original_dn][0]))
        else:
            handle.write('The original DN can not be found in the map file, skipping the workflow\n')
            continue
        params = {'reduce' : False,
                  'key' : [usersMap[original_dn][1], usersMap[original_dn][0], collection_name, fileset_name]}
        result = acdcDB.loadView('ACDC', 'owner_coll_fileset_docs', params)
    
        rows = result['rows']
        docIds = map(lambda x : x['id'], rows)
        handle.write('Found %d documents to change\n' % len(rows))
        handle.write('Changing from %s-%s to %s-%s\n' % (usersMap[original_dn][1], usersMap[original_dn][0],
                                                       workflow['group'], workflow['owner']))

        for docId in docIds:
            doc = acdcDB.document(docId)
            doc['owner'] = {'group' : workflow['group'], 'user' : workflow['owner']}
            if not options.dryRun:
                acdcDB.queue(doc)
        if not options.dryRun:
            response = acdcDB.commit()
        else:
            response = 'This is a dry-run no changes were made'
        
        handle.write('Response to write operation: %s\n'% str(response))
        handle.write('Response length: %d\n' % len(response))
        handle.write('=================================================================\n')
    
    handle.write('Finished script')
    handle.close()
            
if __name__ == '__main__':
    sys.exit(main())
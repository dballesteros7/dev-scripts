'''
Created on Aug 30, 2012

@author: dballest
'''
import httplib
import json
import os


def main():
    url = 'cmsweb.cern.ch'
    headers = {"Content-type": "application/json"}
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    conn.request("GET", "/couchdb/workloadsummary/_all_docs", None , headers)

    response = conn.getresponse()

    if response.status != 200:
        print "Couldn't contact couchdb"
        print "Reason: %s" % response.reason
        print "Message: %s" % response.msg

    result = json.loads(response.read())
    
    rows = result['rows']
    
    fileOut = '/tmp/summaries.list'
    fileOut = open(fileOut, 'w')
    for row in rows:
        fileOut.write('%s\n' % row['id'])
    fileOut.close()

if __name__ == '__main__':
    main()
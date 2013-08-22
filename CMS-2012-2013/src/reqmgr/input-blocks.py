"""
Created on Aug 30, 2012

@author: dballest
"""

import json
import httplib
import urllib
import os

def getRunningRequestInputs(request, url, outputFile = None):

    params = {'reduce' : False,
              'key' : request}
    jsonParams = {}
    for param in params:
        jsonParams[param] = json.dumps(params[param])

    encodedParams = urllib.urlencode(jsonParams, True)
    headers = {"Content-type": "application/json"}
    if 'X509_USER_PROXY' in os.environ:
        conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    elif 'X509_USER_CERT' in os.environ and 'X509_USER_KEY' in os.environ:
        conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_CERT'), key_file = os.getenv('X509_USER_KEY'))
    else:
        print "You need a valid proxy or cert/key files"
        return 1
    conn.request("GET", "/couchdb/workqueue/_design/WorkQueue/_view/elementsByWorkflow?%s" % encodedParams, None , headers)

    response = conn.getresponse()

    if response.status != 200:
        print "Couldn't contact the workqueue for info on the request"
        print "Reason: %s" % response.reason
        print "Message: %s" % response.msg

    result = json.loads(response.read())
        
    elements = result['rows']
    
    elementsIds = [x['value']['_id'] for x in elements]
    
    inputBlocks = []
    failedBlocks = []
    
    for elementId in elementsIds:
        conn.request("GET", "/couchdb/workqueue/%s" % elementId, None, headers)
        
        response = conn.getresponse()

        if response.status != 200:
            print "Couldn't contact the workqueue for info on the element %s" % elementId
            print "Reason: %s" % response.reason
            print "Message: %s" % response.msg
            print "Skipping"
            continue
        
        result = json.loads(response.read())
        print result
        workqueueElement = result.get('WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement', {})
        blocks = workqueueElement.get('Inputs', {}).keys()
        status = workqueueElement.get('Status', None)
        print workqueueElement
        
        if status == 'Failed':
            failedBlocks.extend(blocks)
        else:
            inputBlocks.extend(blocks)
        
    if not outputFile:
        print 'Failed blocks:\n-----------------------------\n'
        print '\n'.join(failedBlocks)
        print 'Running blocks:\n-----------------------------\n'
        print '\n'.join(inputBlocks)
    else:
        outFile = open(outputFile, 'w')
        for block in inputBlocks:
            outFile.write('%s\n' % block)
        outFile.close()

    conn.close()

if __name__ == '__main__':
    getRunningRequestInputs('franzoni_ReReco24AugDoublePhoton_120917_115752_1816', 
                            'cmsweb.cern.ch')

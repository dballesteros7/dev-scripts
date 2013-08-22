'''
Created on Nov 21, 2012

@author: dballest
'''

import sys
import json

def main():
    fileListPath = sys.argv[1]
    try:
        inputHandle = open(fileListPath)
        jsonObject = json.load(inputHandle)
        for key in jsonObject:
            print "Data Type: %s" % key
            print "==========================================="
            for secondKey in jsonObject[key]:
                print "    File type: %s" % secondKey
                if key != "UnknownStreamers":
                    for thirdKey in jsonObject[key][secondKey]:
                        print "        Stream: %s" % thirdKey
                        minRun = min(jsonObject[key][secondKey][thirdKey].keys(), key = int)
                        maxRun = max(jsonObject[key][secondKey][thirdKey].keys(), key = int)
                        print "            From run: %s" % minRun
                        print "            To run: %s" % maxRun
                        
                else:
                    print "    SubType: %s" % secondKey
            print "==========================================="
            print

    except Exception, ex:
        print str(ex)
    finally:
        inputHandle.close()
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
if __name__ == '__main__':
    sys.exit(main())
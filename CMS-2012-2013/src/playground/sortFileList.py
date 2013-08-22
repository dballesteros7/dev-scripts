'''
Created on Nov 21, 2012

@author: dballest
'''

import sys
import json

def main():
    fileListPath = sys.argv[1]
    outputFile = sys.argv[2]
    jsonStructure = {'UnknownStreamers' : {},
                     'RawData' : {},
                     'Streamers' : {}}
    try:
        inputHandle = open(fileListPath)
        outputFileHandle = open(outputFile, 'w')
        currentDir = ""
        for line in inputHandle:
            if line.endswith(":\n"):
                # Then it is a directory
                currentDir = line[:-2]
            else:
                # It is a subdirectory or a file
                if line.endswith(".dat\n") or line.endswith(".raw\n"):
                    # It's a streamer or RAW?
                    tokens = currentDir.split('/')
                    type = tokens[6]
                    stream = None
                    runNumber = None
                    if len(tokens) > 7:
                        try:
                            int(tokens[7])
                            runNumber = int('%s%s%s' % (tokens[7], tokens[8], tokens[9]))
                        except ValueError:
                            stream = tokens[7]
                            runNumber = int('%s%s%s' % (tokens[8], tokens[9], tokens[10]))
                    if stream is None and runNumber is None:
                        if type not in jsonStructure['UnknownStreamers']:
                            jsonStructure['UnknownStreamers'][type] = 0
                        jsonStructure['UnknownStreamers'][type] += 1
                        
                    elif line.endswith(".dat\n"):
                        if type not in jsonStructure['Streamers']:
                            jsonStructure['Streamers'][type] = {}
                        if stream not in jsonStructure['Streamers'][type]:
                            jsonStructure['Streamers'][type][stream] = {}
                        if runNumber not in jsonStructure['Streamers'][type][stream]:
                            jsonStructure['Streamers'][type][stream][runNumber] = 0
                        jsonStructure['Streamers'][type][stream][runNumber] += 1
                        
                    elif line.endswith(".raw\n"):
                        if type not in jsonStructure['RawData']:
                            jsonStructure['RawData'][type] = {}
                        if stream not in jsonStructure['RawData'][type]:
                            jsonStructure['RawData'][type][stream] = {}
                        if runNumber not in jsonStructure['RawData'][type][stream]:
                            jsonStructure['RawData'][type][stream][runNumber] = 0
                        jsonStructure['RawData'][type][stream][runNumber] += 1
        json.dump(jsonStructure, outputFileHandle)
    except Exception, ex:
        print str(ex)
    finally:
        inputHandle.close()
        outputFileHandle.close()

if __name__ == '__main__':
    sys.exit(main())
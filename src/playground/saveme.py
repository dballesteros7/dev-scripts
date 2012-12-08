'''
Created on Nov 21, 2012

@author: dballest
'''

"""
{'run' 207487: , 'datasetpath' : 337},
{'run' 207515: , 'datasetpath' : 337},
{'run' 207515: , 'datasetpath' : 380},
"""

x = """337 DoubleElectron                                                                                               Run2012D-PromptReco-v1                                                                                               ALCARECO
       380 SingleElectron                                                                                               Run2012D-PromptReco-v1                                                                                               ALCARECO
"""

def main():
    for line in x.splitlines():
        tokens = line.split()
        datasetPath =  tokens[0]
        primds =  tokens[1]
        procds =  tokens[2]
        datatier =  tokens[3]
        print "{'run' 207515: , 'datasetpath' : %s}," % (datasetPath)
    
if __name__ == '__main__':
    main()
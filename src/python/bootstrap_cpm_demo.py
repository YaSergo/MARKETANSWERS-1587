import sys
import json

def stringToList(line):
    result = line.strip('[]').split(',')
    if len(line) > 2:
        return(map(float, result))
    else:
        return(result)

for line in sys.stdin:
    line = line.strip()
    hyper_id, parents = line.split('\t', 1)
    parents = stringToList(parents)
    print '\t'.join([hyper_id, str(len(parents)), 'SERGO'])
    #print '\t'.join([hyper_id, parents])
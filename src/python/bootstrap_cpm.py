#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import numpy as np

from random import choice, seed

np.random.seed(1234)
seed(1234)

def stringToList(line):
    result = line.strip('[]').split(',')
    if len(line) > 2:
        return(list(map(float, result)))
    else:
        return(result)

def bootstrap(sample, samplesize = None, nsamples = 1000, alpha = 0.05):
    if samplesize is None:                                                                   
        samplesize=len(sample)

    # resample = [np.random.choice(sample, size = samplesize).mean() for _ in range(nsamples)]
    # At marmot have an old version of numpy........... np.random.choice doesn't work
    def random_choice(elements, size):
        return np.array([choice(elements) for _ in range(size)])

    resample = [random_choice(sample, size = samplesize).mean() for _ in range(nsamples)]

    result = [np.percentile(resample,50),
                np.percentile(resample,(alpha/2)*100.0),
                np.percentile(resample,100-(alpha/2)*100)]

    return result


for line in sys.stdin:
    line = line.strip()
    page_groupid_1, page_groupid_2, cpm, n, sd, cpms_string = line.split('\t', 5)
    cpms = stringToList(cpms_string)
    
    bs_result = bootstrap(cpms)
    # python 3.3 так не разрешает
    # print(page_groupid_1, page_groupid_2, cpm, n, sd, cpms, bs_result[0], bs_result[1], bs_result[2], sep="\t")

    # print("\t".join([page_groupid_1, page_groupid_2, cpm, n, sd, cpms_string, str(bs_result[0]), str(bs_result[1]), str(bs_result[2])]))
    print("\t".join([page_groupid_1, page_groupid_2, cpm, n, sd, str(bs_result[0]), str(bs_result[1]), str(bs_result[2])]))
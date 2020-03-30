#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 14 09:37:36 2020

@author: gourish
"""

from mrjob.job import MRJob


class MRWordFrequencyCount(MRJob):
    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1
    def reducer(self, key, values):
        yield key, sum(values)
        
        
if __name__ == '__main__':
    MRWordFrequencyCount.run()
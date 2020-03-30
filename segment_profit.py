#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 12:03:43 2020

@author: gourish
"""

from mrjob.job import MRJob
from mrjob.step import MRStep


class segmentProfit(MRJob):
   def steps(self):
       return [
           MRStep(mapper=self.mapper_get_segment,
                  reducer=self.reducer_cal_profit)
       ]

   def mapper_get_segment(self, _, line):
       lst= line.split(',')
       sgmt=lst[1].strip()
       profit=float(lst[2].strip())
      
       yield sgmt,profit 

   def reducer_cal_profit(self, key, values):
       yield key, sum(values)

if __name__ == '__main__':
   segmentProfit.run()
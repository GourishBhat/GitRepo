#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 12:20:39 2020

@author: gourish
"""

from mrjob.job import MRJob
from mrjob.step import MRStep


class orderCount(MRJob):
   def steps(self):
       return [
           MRStep(mapper=self.mapper_get_orders,
                  reducer=self.reducer_cal_orders)
       ]
          

   def mapper_get_orders(self, _, line):
       lst= line.split(',')
       city=lst[2].strip()
       order_id=lst[0].strip()
      
       yield city,order_id

   def reducer_cal_orders(self, key, values):
       count=0
       for x in values:
           count=count+1
       yield key,count
       
   #def reducer2(self,key,values):
       
if __name__ == '__main__':
   orderCount.run()
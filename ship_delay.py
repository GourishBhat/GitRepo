#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 09:43:44 2020

@author: gourish
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import date

class shipDelay(MRJob):
   def steps(self):
       return [
           MRStep(mapper=self.mapper_get_date,
                  reducer=self.reducer_count_delay)
       ]

   def mapper_get_date(self, _, line):
       lst= line.split(',')
      # print(Country,Units_sold)
       o_id=lst[0].strip()
       o_date=lst[1].strip()
       s_date=lst[2].strip()
       d1=int(o_date[0:2])
       m1=int(o_date[3:5])
       y1=int(o_date[6:8])
       d2=int(s_date[0:2])
       m2=int(s_date[3:5])
       y2=int(s_date[6:8])
       
       rd=date(y2,m2,d2)-date(y1,m1,d1)
       #print(rd.days)
       yield o_id,rd.days 

   def reducer_count_delay(self, key, values):
       yield key, values

if __name__ == '__main__':
   shipDelay.run()
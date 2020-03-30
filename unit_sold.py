#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 14 10:05:55 2020

@author: gourish
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class countUnits(MRJob):
   def steps(self):
       return [
           MRStep(mapper=self.mapper_get_units,
                  reducer=self.reducer_count_units)
       ]

   def mapper_get_units(self, _, line):
       lst= line.split(',')
      # print(Country,Units_sold)
       country=lst[1].strip()
       u=lst[3].strip()
       #print(type(us))
       #us=u[0:4]
       us=int(u)
       #print(type(us))
       #print(country,us)
       yield country, us

   def reducer_count_units(self, key, values):
       yield key, sum(values)

if __name__ == '__main__':
   countUnits.run()
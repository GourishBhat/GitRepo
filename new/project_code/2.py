#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 14 10:05:55 2020

@author: gourish
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class countMovie(MRJob):
   def steps(self):
       return [
           MRStep(mapper=self.mapper_get_movieID,
                  reducer=self.reducer_count_movieID)
       ]

   def mapper_get_movieID(self, _, line):
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

   def reducer_count_movieID(self, key, values):
       yield key, sum(values)

if __name__ == '__main__':
   countMovie.run()
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 11:23:43 2020

@author: gourish
"""

from mrjob.job import MRJob
from mrjob.step import MRStep


class country_product_sales(MRJob):
   def steps(self):
       return [
           MRStep(mapper=self.mapper_get_products,
                  reducer=self.reducer_list_products)
       ]

   def mapper_get_products(self, _, line):
       lst= line.split(',')
       cntry=lst[0].strip()
       prdct=lst[2].strip()
      
       yield cntry,prdct 

   def reducer_list_products(self, key, values):
       yield key, values

if __name__ == '__main__':
   country_product_sales.run()
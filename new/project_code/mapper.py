# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd

df = pd.read_csv("/home/gourish/tos/TOS_DI-20191031_1204-V7.3.1M3/workspace/output/units_sold.csv",usecols=['Country','Units_sold'])

for line in df:
    line=line.strip()
    line= line.split(',')
    country=line[0]
    print(country)
import csv
import sys
import os
import pandas as pd

path = 'C:/Users/Riley\Desktop/nflmodel/'
xlsx_url = 'http://www.aussportsbetting.com/historical_data/nfl.xlsx'
file = "nfl.xlsx"

# convert xlsx to csv
xlsx_file = pd.read_excel(xlsx_url, engine='openpyxl')
xlsx_file.to_csv('C:/Users/Riley\Desktop/nflmodel/data/nfl.csv')

#with open (path)

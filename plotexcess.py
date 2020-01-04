#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import pandas as pd

import matplotlib as mpl
# mpl.use('TkAgg')
from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

import datetime
import time
import sys
import os
from dateutil import tz
from tzlocal import get_localzone
tzl = get_localzone().zone
mdates.rcParams['timezone'] = tzl
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()


def getExs(infile="estexcess_all_processed.xls"):
	""" read csv data for regions
Region  datetime        npm2.5  exsHosp exsBeddays      exsDeaths       fromfile        npopused
RANDWICK        03/10/2019_21:00        26.7    0.02    0.13    0.00
RANDWICK        06/10/2019_14:00        28.5    0.05    0.26    0.01

	"""

	df = pd.read_csv(infile,sep='\t')
	df['date'] = pd.to_datetime(df.iloc[:,1],format = '%d/%m/%Y_%H:%M')
	
	df.set_index(df['date'],inplace=True)
	#df = df.tz_localize(tz=tzl)
	df = df.sort_index()
	regions = df.Region.unique()
	print(regions)
	sumd = pd.DataFrame(columns=['Date','Region','pm2.5'])
	for r in regions:
		region = df[df.Region==r]
		pm = region['npm2.5'].resample('H').median()
		daze = region['date'].resample('H').last()
		sumd = sumd.append(pd.DataFrame({'date': daze ,'Region': r, 'pm2.5':pm},columns=['Date','Region','pm2.5']))
	sns.set(rc={'figure.figsize':(20, 16)})
	# Use seaborn style defaults and set the default figure size
	fig, ax = plt.subplots()

	for r in regions:
		print(r)
		if r.startswith('ALL'):
			city = sumd[sumd['Region']==r]
			ax.plot(city['pm2.5'],
				marker='X', markersize=0.9,linestyle='-', linewidth=0.4, label='%s mean PM2.5' % r) # 
			ax.set_ylabel('Mean PM2.5')
	ax.legend();
	plt.show()
	
if __name__ == "__main__":
	getExs()

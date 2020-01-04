#!/usr/bin/python3
# -*- coding: utf-8 -*-
""" read pm2.5 data and annotate with estimates of effects

hourly average pm2.5 data are available from https://www.dpie.nsw.gov.au/air-quality/search-for-and-download-air-quality-data


According to https://www.bmj.com/content/367/bmj.l6258.long
the annual estimates for the US population for hospitalisations, days and deaths for each 1mg
increase in lagged pm2.5 are 5692, 32314 and 684 respectively

For Australia, we scale these down from 372M population to about 24M and for hours, we 
scale down by 365.25*24


Python 3.6.9 (default, Nov  7 2019, 10:44:02) 
[GCC 8.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> usa = 372000000.0
>>> hpermg = 5692/usa
>>> hpermg
1.5301075268817205e-05
>>> dayspermg = 32314/usa
>>> dayspermg
8.686559139784946e-05
>>> mpermg = 684/usa
>>> rates = [hpermg,dayspermg,mpermg]
>>> [x*500 for x in rates]
[0.007650537634408603, 0.043432795698924734, 0.0009193548387096775]
>>> [x*500*24000000 for x in rates]
[183612.90322580645, 1042387.0967741936, 22064.51612903226]
>>> [x*500*24000000/(365*24) for x in rates]
[20.96037708057151, 118.99396081897187, 2.518780380026514]
>>> [x*50*24000000/(365*24) for x in rates]
[2.0960377080571515, 11.899396081897185, 0.25187803800265135]
>>> [x*200*24000000/(365*24) for x in rates]
[8.384150832228606, 47.59758432758874, 1.0075121520106054]
>>> 

so if for example, the hourly average pm2.5 is 225, that's about 200 above the typical Randwick level so there would be about
[8.4 hospitalisations, 47.6 days in hospital and 1.008 deaths excess predicted during that hour.


data can only be grabbed for about 25 regions at a time for 3/12 so 
need at least 2 data sets for entire state
"""
import sys

infnames = sys.argv[1:]
pm25ignore = 10.0
usa = 372000000.0 # populations or so
aus = 7500000.0 # aus is 25000000.0 nsw is about 1/3 of that
# event excess counts are annual estimate excess rates per mg lagged of pm2.5 
# for the USA so must be scaled for hours in australia. Age weighted adjustment will make some differences
# but this is just a quick and dirty method
hpermg = aus*5692/(usa*24*365) # scale to NSW (Australian) population and to events per hour per mg pm2.5
dayspermg = aus*32314/(usa*24*365)
mpermg = aus*684/(usa*24*365)
auratespermghour = [hpermg,dayspermg,mpermg]
print('hosp,beddays,death rates per mg per hour',auratespermghour)

ofname = 'estexcess_all_processed.xls'
cdat = {}
lastpm25 = None
sums = {}
nhit = {}
nmiss = {}
ncalc = {}
nrec = {}
allmeans = []
allplaces = []
for infname in infnames:
	try:
		f = open(infname,'r')
	except:
		print('Was a valid file path passed on the command line? %s' % sys.argv[1])
		sys.exit(1)
	places = []
	dat = [x.rstrip().split('\t') for x in f]
	headers = dat[0]		
	p = headers[2:]
	places = [x.split(' ')[0] for x in p]
	meanname = 'ALL_%s' % infname.split('_')[0]
	places.append(meanname)
	allplaces += places
	for p in places:
		sums[p] = [0,0,0]
		nhit[p] = 0
		nmiss[p] = 0
		cdat[p] = []
		ncalc[p] = 0
		nrec[p] = 0
	for rec in dat[1:]:
		date,hour = rec[:2]
		if hour == '24:00': # nope, won't parse in pandas :(
			hour = '23:59' 
		rectot = 0
		recn = 0
		for i,pm25 in enumerate(rec[2:]):
			place = places[i]
			nrec[place] += 1
			if len(pm25.strip()) > 0:
				nhit[place] += 1
				npm25 = float(pm25)
				recn += 1
				rectot += npm25 # running total for means
				if (npm25 > pm25ignore):
					ncalc[place] += 1
					increase = npm25-pm25ignore
					excess = [x*increase for x in auratespermghour]
					row = '%s\t%s\t%.1f\t%.2f\t%.2f\t%.2f\t%s\t%d\n' % (place,'%s_%s' % (date,hour),npm25,excess[0],excess[1],excess[2],infname,aus)
					cdat[place].append(row)
					for i,exs in enumerate(excess):
						sums[place][i] += exs
			else:
				nmiss[place] += 1
		if recn > 0:
			place = meanname
			m = rectot/float(recn) - pm25ignore
			if m > 0:
				#print('mean',m)
				allmeans.append(m)
				excess = [x*m for x in auratespermghour]
				row = '%s\t%s\t%.1f\t%.2f\t%.2f\t%.2f\t%s\t%d\n' % (place,'%s_%s' % (date,hour),m,excess[0],excess[1],excess[2],infname,aus)
				cdat[place].append(row)
				for i,exs in enumerate(excess):
					sums[place][i] += exs
				ncalc[place] += 1
				nrec[place] += 1
		else:
			nmiss[meanname] += 1
			nrec[meanname] += 1

print('### mean pm2.5 over entire period for NSW=',sum(allmeans)/len(allmeans))			
f = open('summary_%s' % ofname,'w')
f.write('region\tnhours25\tnhoursmiss\tnhourstot\thospitalisations\tbeddays\tdeaths\tdatafile\tnswpop\n')
for p in cdat.keys():
	s = '%s\t%d\t%d\t%d\t%.1f\t%.1f\t%.1f\t%s\t%d\n' % (p,ncalc[p],nmiss[p],nrec[p],sums[p][0],sums[p][1],sums[p][2],infname,aus)
	f.write(s)
f.close()
fo = open(ofname,'w')
fo.write('Region\tdatetime\tnpm2.5\texsHosp\texsBeddays\texsDeaths\tfromfile\tnpopused\n')
for p in cdat.keys():
	fo.write(''.join(cdat[p]))
fo.write('\n')
fo.close()
	

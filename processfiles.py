#!/usr/bin/env python
# encoding: utf-8
"""
changefiles.py

Created by Peter Bakker on 2017-10-04.
Copyright (c) 2017 . All rights reserved.
"""

import sys
import os
import numpy  as np
import pandas as pd
import datetime
from pytz import timezone
import pytz
import zipline.utils.calendars as cal
from zipline.data.session_bars import SessionBarReader
from zipline.data.bar_reader import (NoDataAfterDate,NoDataBeforeDate,NoDataOnDate)
from zipline.utils.calendars import get_calendar
from pandas import (DataFrame,DatetimeIndex,isnull,NaT,read_csv,read_sql,to_datetime,Timestamp,)
utc = pytz.utc
dirs = ['/root/data/minute','/root/data/daily']
calcal = cal.get_calendar( name='NYSE')


def check_sessions(table, frequency='daily'):
    calendar = get_calendar('NYSE')
#    from IPython import embed; embed()
    earliest_date = table.index[0]
    # Calculate the index into the array of the first and last row
    # for this asset. This allows us to efficiently load single
    # assets when querying the data back out of the table.
    asset_first_day = table.index[0]
    asset_last_day = table.index[-1]
    sessions = calendar.sessions_in_range(asset_first_day ,asset_last_day)
    asset_sessions = sessions[sessions.slice_indexer(asset_first_day, asset_last_day)]

    if frequency =='minute':
        minutes_passed = len(table)
        asset_first_day = calendar.minute_to_session_label(asset_first_day, direction='next')
        asset_last_day = calendar.minute_to_session_label(asset_last_day, direction='previous')
        minutes_in_session = calendar.minutes_for_sessions_in_range(asset_first_day, asset_last_day)
        table = table[table.index.isin(minutes_in_session)]
        if (minutes_passed) > len(minutes_in_session): print 'Removed '+ str((minutes_passed) - len(minutes_in_session))+' minutes'
    elif frequency =='daily' and len(table) != len(asset_sessions):
	missing_sessions = asset_sessions.difference(to_datetime(np.array(table.index),unit='s',utc=True,) ).tolist()
	extra_sessions = to_datetime(np.array(table.index),unit='s',utc=True,).difference(asset_sessions).tolist()
	for missing_session in missing_sessions:
           #add stuff
           prev_date = calendar.previous_session_label(missing_session)
           row_to_copy = table[(table.index == prev_date)]
           row_to_copy_val = row_to_copy.values
#           from IPython import embed; embed()
           table.loc[missing_session] = row_to_copy_val[0]
           table.loc[missing_session].volume = 0
           #row = row_to_copy
           #table.append(row)
           print 'Added session at '+str(missing_session)
        
	for extra_session in extra_sessions:
	   #delete stuff
 	   table.drop(extra_session)
  	   print 'Removed session at '+str(extra_session)
    return table

for dir in dirs:
        print "parsing ", dir
	for item in os.listdir(dir):
	   if item.endswith('TRADES_RTH.csv'):
	      symbol = item.split('_')[0]
	      exchange = 'SMART'
	      asset_type = 'STK'
	      if symbol in ['GDX','GLD']: exchange = 'ARCA'
	      if symbol in ['VIX','VIX3M', 'VXMT', 'VXST', 'GVZ']:
		  exchange = 'CBOE'
		  asset_type ='IND'
              print "parsing ", symbol, " in file ",item
              if  dir == dirs[1]:
                 dfData=pd.read_csv(dir+"/"+item, parse_dates={'dates':[0]}, header=None, index_col=0).sort_index()
                 dfData.rename(columns={1:'open',2:'high', 3:'low',4:'close',5:'volume',6:'temp'}, inplace=True)
                 dfData.index=pd.to_datetime(dfData.index, utc=True)
                 dfData=dfData.tz_localize(utc, axis=0, level=None, copy=False, ambiguous='raise')
                 dfData = check_sessions(dfData)
              else:
                 dfData=pd.read_csv(dir+"/"+item, parse_dates={'dates':[0, 1]}, header=None, index_col=0).sort_index()
                 dfData.rename(columns={2:'open',3:'high', 4:'low',5:'close',6:'volume',7:'temp'}, inplace=True)
                 dfData=dfData.tz_localize(utc, axis=0, level=None, copy=False, ambiguous='raise')
		 dfData =check_sessions(dfData,'minute')
#                 remove= []
#                 for dt in dfData.index:
#                #    if not calcal.is_open_on_minute(dt):
#                     #  remove.append(pd.to_datetime(str(dt)))
#                 dfData = dfData[~dfData.index.isin(remove)]

              dfData = dfData.drop('temp', axis=1, errors='ignore')
#              dfData = check_sessions(dfData)
#	      from IPython import embed; embed()
	      #dfData.index=dfData.index.tz_localize('UTC')
	      #dfData.index=dfData.index.tz_convert('UTC')
	      dfData.index=dfData.index.tz_convert(None)
	      with open(dir+'/'+symbol+'_'+exchange+'_'+asset_type+'.csv','w') as f:
	         dfData.to_csv(f)
	         f.close()
	         os.rename(dir+'/'+item, dir+'/temp/'+item)





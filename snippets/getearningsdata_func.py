
import sys
import os
import pandas as pd
import numpy as np
import datetime
from shutil import copy
from yahoo_earnings_calendar import YahooEarningsCalendar
from zipline.data import bundles as bundles_module
from zipline.data.bundles.core import load, bundles
import imp
ext = imp.load_source('ext', '/root/.zipline/extension.py')


def get_tickers_from_bundle(bundle_name):
    """Gets a list of tickers from a given bundle courtesy PBHARRIN"""
    bundle_data = load(bundle_name, os.environ, None)

    # get a list of all sids
    lifetimes = bundle_data.asset_finder._compute_asset_lifetimes()
    all_sids = lifetimes.sid

    # retreive all assets in the bundle
    all_assets = bundle_data.asset_finder.retrieve_all(all_sids)

    # return only tickers
    return map(lambda x: (x.symbol, x.sid), all_assets)


def get_ticker_sid_dict_from_bundle(bundle_name):
    """Packs the (ticker,sid) tuples into a dict."""
    all_equities = get_tickers_from_bundle(bundle_name)
    return dict(all_equities) 

def getearnings(b='quantopian-quandl'):
    today =  datetime.datetime.today()
    oneweeks = datetime.timedelta(days=21)
    threeweeks = datetime.timedelta(days=21)
    yec = YahooEarningsCalendar()
    earningsDF = None
    frames = []
    threeweeksago = today - threeweeks 
    inthreeweeks = today + threeweeks
    earnings = yec.earnings_between(threeweeksago, inthreeweeks)

    df = pd.DataFrame(earnings)
    print 'Found '+str(len(df))+' results for between '+ str(threeweeksago) + ' and '+str(inthreeweeks)
    if ('startdatetime' not in df.columns) or len(df) == 0:
        print "no startdatetime detected/no data"
        return None
    df['startdatetime'] = pd.to_datetime(df['startdatetime']) # TODO make timezone aware and convert to UTC
    pdtoday = pd.to_datetime(today)
    df['asof_date'] = pdtoday
    df['temp'] = df['startdatetime'].view('int64')-df['asof_date'].view('int64')   
    df['days'] = df['temp'].apply(lambda x: np.timedelta64(x, 'ns').astype('timedelta64[D]')/np.timedelta64(1, 'D') )
    tickers = get_tickers_from_bundle(b)
    t = pd.DataFrame(tickers)
    t.rename(columns = {0:'symbol',1:'sid'}, inplace=True)
    data = pd.merge(left=df, right=t,on=['symbol','symbol'], how= 'left').dropna(subset=['sid'])
    data['sid'] = data['sid'].astype(int)
    
    return data

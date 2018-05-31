
"""
getearnings.py

Created by Peter Bakker on 2017-10-26.
Copyright (c) 20188 unhedged. All rights reserved.
"""

import sys
import os
import pandas as pd
import numpy as np
import datetime
from shutil import copy
from yahoo_earnings_calendar import YahooEarningsCalendar
from zipline.data import bundles as bundles_module
from zipline.data.bundles.core import load, bundles
from zipline.utils.math_utils import nanmean, nanstd
#import zipline.pipeline.loaders.blaze
from IPython import embed
import imp
ext = imp.load_source('ext', '/root/.zipline/extension.py')




import getopt

symbols = ["A","AAL","AAOI","AAP","AAPL","ABB","ABBV","ABC","ABT","ACN","ADBE","ADI","ADM","ADP","ADS","ADSK","AEE","AEP","AES","AET","AFL","AGN","AGNC","AIA","AIG","AIV","AIZ","AJG","AKAM","AKAO","ALB","ALE","ALGN","ALK","ALL","ALLE","ALRM","ALXN","AMAT","AMBA","AMD","AME","AMG","AMGN","AMP","AMT","AMZN","ANDV","ANSS","ANTM","AON","AOS","APA","APC","APD","APH","APTV","ARE","ARNC","ASML","ATVI","AUTO","AVB","AVGO","AVX","AVY","AWK","AXP","AYI","AZO","BA","BABA","BAC","BAX","BBT","BBY","BDX","BEN","BHF","BHGE","BIDU","BIIB","BK","BKNG","BLD","BLK","BLL","BMY","BP","BSX","BUX","BWA","BX","BXP","BZUN","C","CA","CAG","CAH","CARA","CAT","CB","CBOE","CBRE","CBS","CCI","CCL","CDNS","CELG","CERN","CF","CFG","CG","CHD","CHK","CHRW","CHTR","CHW","CI","CINF","CL","CLX","CMA","CMCSA","CME","CMG","CMI","CMS","CNA","CNC","CNP","COF","COG","COHR","COL","COO","COP","COST","COTY","CPB","CRM","CSCO","CSX","CTAS","CTL","CTRP","CTSH","CTXS","CUTR","CVS","CVX","CXO","D","DAL","DAR","DE","DFS","DG","DGX","DHI","DHR","DIS","DISCA","DISCK","DISH","DLR","DLTR","DOV","DPS","DRE","DRI","DTE","DUK","DVA","DVN","DWDP","DXC","EA","EBAY","ECL","ED","EDV","EE","EFX","EIX","EL","EME","EMN","EMR","EOG","EPZM","EQIX","EQR","EQT","ERV","ES","ESRX","ESS","ETFC","ETN","ETR","EVHC","EW","EXC","EXPD","EXPE","EXR","F","FAST","FB","FBHS","FCX","FDX","FE","FFIV","FIS","FISV","FITB","FL","FLIR","FLR","FLS","FMC","FOX","FOXA","FRT","FTI","FTV","GALT","GD","GDX","GE","GGP","GILD","GIS","GKOS","GLD","GLW","GM","GOOG","GOOGL","GPC","GPN","GPRO","GPS","GRMN","GS","GT","GWW","HAL","HAS","HBAN","HBI","HCA","HCP","HD","HES","HF","HIG","HII","HLT","HOG","HOLX","HON","HOP","HP","HPE","HPQ","HRB","HRL","HRS","HSIC","HST","HSY","HUM","HW","IBM","ICE","IDXX","IFF","IJH","IJR","ILMN","INCY","INFO","INO","INTC","INTU","IP","IPG","IPGP","IQV","IR","IRM","ISRG","IT","ITW","IVB","IVZ","IWM","JBHT","JCI","JEC","JM","JNJ","JNPR","JNUG","JPM","JWN","K","KEY","KHC","KIM","KLAC","KMB","KMI","KMPR","KMX","KNDI","KO","KORS","KR","KSS","KSU","L","LABL","LB","LEG","LEN","LG","LH","LITE","LKQ","LLL","LLY","LMT","LNC","LNT","LOW","LRCX","LUK","LUV","LYB","M","MA","MAA","MAC","MAR","MAS","MAT","MCD","MCHP","MCK","MCO","MDLZ","MDT","MET","MGM","MGPI","MHK","MKC","MLM","MMC","MMM","MNST","MO","MOMO","MON","MOS","MOV","MP","MPC","MRCC","MRK","MRO","MRVL","MS","MSCI","MSFT","MSI","MTB","MTD","MU","MYL","NA","NANO","NAP","NAVI","NBL","NCLH","NDAQ","NEE","NEM","NFLX","NFX","NI","NKE","NKTR","NLSN","NM","NOC","NOV","NPS","NRG","NSC","NTAP","NTRS","NUE","NUGT","NVDA","NVLN","NWL","NWS","NWSA","O","OCLR","OKE","OLED","OMC","ORCL","ORLY","OXY","PANW","PAYX","PBCT","PCAR","PCG","PEG","PEP","PFE","PFG","PG","PGI","PGR","PH","PHM","PIR","PKG","PKI","PLAY","PLD","PLV","PM","PNC","PNR","PNW","PPG","PPL","PRFT","PRGO","PRU","PSA","PSX","PVH","PWR","PX","PXD","PY","PYG","PYPL","Q","QCOM","QQQ","QRVO","QTM","RCL","RE","REG","REGN","RF","RH","RHI","RHT","RJF","RL","RMD","ROK","ROP","ROST","RRC","RSG","RTN","SO","T","TAP","TDG","TEL","TEVA","TGT","THO","TI","TIF","TJX","TLO","TLT","TMF","TMK","TMO","TPR","TQQQ","TRIP","TROW","TRV","TSCO","TSN","TSS","TT","TTWO","TWLO","TWTR","TWX","TX","TXN","TXT","TZ","UA","UAA","UAL","UDR","UHS","ULTA","UNH","UNM","UNP","UPRO","UPS","URBN","URI","USB","USO","UTX","UVXY","V","VAR","VFC","VGIT","VHT","VIAB","VIX","VIX3M","VLO","VMC","VNO","VRSK","VRSN","VRTX","VRX","VTR","VXMT","VXST","VXX","VZ","WAT","WBA","WDC","WEC","WELL","WFC","WHR","WIX","WK","WKS","WLTW","WM","WMB","WMT","WR","WRK","WRLD","WU","WY","WYN","WYNN","XEC","XEL","XIV","XL","XLNX","XLP","XLY","XOM","XRAY","XRX","XYL","YF","YK","YMC","YUM","YY","ZBH","ZION","ZIV","ZTS"]

def get_tickers_from_bundle(bundle_name):
    """Gets a list of tickers from a given bundle"""
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

def main(argv=None):
    DL_earningsdates()
    convert_to_blaze()
    #convert_to_blaze(bundle,'/root/data/temp/earningsdata.csv')
    

def DL_earningsdates(argv=None):
    today =  datetime.datetime.today()
    oneweeks = datetime.timedelta(days=21)
    threeweeks = datetime.timedelta(days=21)
    yec = YahooEarningsCalendar()
    earningsDF = None
    frames = []

    for i in xrange(1,26,1):
        if i > 1: today = today - (threeweeks)
        threeweeksago = today - threeweeks 
        inthreeweeks = today + threeweeks
        earnings = yec.earnings_between(threeweeksago, inthreeweeks)

        df = pd.DataFrame(earnings)
        print 'Found '+str(len(df))+' results for between '+ str(threeweeksago) + ' and '+str(inthreeweeks)
        if ('startdatetime' not in df.columns) or len(df) == 0:
            print "no startdatetime detected/no data"
            continue
        df['startdatetime'] = pd.to_datetime(df['startdatetime']) # TODO make timezone aware and convert to UTC
        pdtoday = pd.to_datetime(today)
        df['today'] = pdtoday
        df['temp'] = df['startdatetime'].view('int64')-df['today'].view('int64')   
        df['days'] = df['temp'].apply(lambda x: np.timedelta64(x, 'ns').astype('timedelta64[D]')/np.timedelta64(1, 'D') )
        frames.append(df)
        df.to_csv('/root/data/temp/earningsdata-'+str(threeweeksago)+'-'+str(inthreeweeks)+'.csv')
        
    earningsDF = pd.concat(frames)
    earningsDF.to_csv('/root/data/temp/earningsdata.csv')
               
def convert_to_blaze(bundle=None, filename= '/root/data/temp/earningsdata.csv'):
    df = pd.read_csv(filename)
    df.rename(columns = {'ticker':'symbol', 'today':'asof_date'}, inplace=True)
    bu = bundles
    cleaned_days_dfs =[]
    cleaned_date_dfs =[]
    for b in bu.keys():
        data = df.copy()
        tickers = get_tickers_from_bundle(b)
        t = pd.DataFrame(tickers)
        t.rename(columns = {0:'symbol',1:'sid'}, inplace=True)
        data = pd.merge(left=data, right=t,on=['symbol','symbol'], how= 'left').dropna(subset=['sid'])
        data['sid'] = data['sid'].astype(int)
        
        cleaned_days_df = data['asof_date','sid','days']
        cleaned_days_df.rename(columns = {'days':'value'}, inplace=True)
        cleaned_days_df.to_csv('/root/data/temp/earningsdata_days_'+b+'.csv')

        
        cleaned_date_df = data['asof_date','sid','startdatetime']
        cleaned_date_df.rename(columns = {'startdatetime':'value'}, inplace=True)
        cleaned_date_df.to_csv('/root/data/temp/earningsdata_date_'+b+'.csv')


def addfiles(dir='/root/data/temp/'):
    frames = []
    for item in os.listdir(dir):
        if item.startswith('earningsdata-') and item.endswith('.csv'):
            df = pd.read_csv(dir+item)
            frames.append(df)
    earningsDF = pd.concat(frames)
    earningsDF.to_csv('/root/data/temp/earningsdata.csv')
	    
   



if __name__ == '__main__':
	main()
	

#!/usr/bin/env python
# encoding: utf-8
"""
getsymbols.py

Created by Peter Bakker on 2017-10-26.
Copyright (c) 2017 __MyCompanyName__. All rights reserved.
"""

import sys
import os
from finsymbols import symbols as sym
import pandas as pd

import getopt


help_message = 'Use -d for day or -m for minute; -o for the output filename'


class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "ho:v", ["help", "output="])
        except getopt.error, msg:
            raise Usage(msg)
    
        # option processing
        for option, value in opts:
            if option == "-v":
                verbose = True
            if option in ("-h", "--help"):
                raise Usage(help_message)
            if option in ("-o", "--output"):
                output = value
            else:
                output = "zipline.txt"
    
    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        return 2
        
    create_symbol_lists()


def create_symbol_lists():
    sp500 = sym.get_sp500_symbols()
    nyse = sym.get_nyse_symbols()
    amex = sym.get_amex_symbols()
    nasdaq = sym.get_nasdaq_symbols()
    
    Df_sp500 = pd.DataFrame(sp500)
#    Df_nyse = pd.DataFrame(nyse)
#    Df_amex = pd.DataFrame(amex)
#    Df_nasdaq = pd.DataFrame(nasdaq)
    
    start = ""
    with open('/root/jTWSdump_707/requests/base.txt','r') as f:
        start = f.read()
#	start += "\n"
    f.close()   
    s_sp500 = ""
    for symbol in Df_sp500.symbol:
        if "^" in symbol or "." in symbol:
            continue
        #s_sp500 += '"'+symbol.strip()+'" "STK" "SMART" "" "" "" "USD" "" "10 D" "1 min" "1" "TRADES" "10" ""'+'\n'
        s_sp500 += '"'+symbol.strip()+'" "STK" "SMART" "" "" "" "USD" "" "5 Y" "1 day" "1" "TRADES" "1" ""'+'\n'

    s_nyse = ""
#    for symbol in Df_nyse.symbol:
#        if "^" in symbol or "." in symbol:
#            continue
#       # s_nyse += '"'+symbol.strip()+'" "STK" "SMART/NYSE" "" "" "" "USD" "" "10 D" "1 min" "1" "TRADES" "10" ""'+'\n'
#        s_nyse += '"'+symbol.strip()+'" "STK" "SMART/NYSE" "" "" "" "USD" "" "5 Y" "1 day" "1" "TRADES" "1" ""'+'\n'
#
    s_amex = ""
#    for symbol in Df_amex.symbol:
#        if "^" in symbol or "." in symbol:
#            continue
#        #s_amex += '"'+symbol.strip()+'" "STK" "SMART" "" "" "" "USD" "" "10 D" "1 min" "1" "TRADES" "10" ""'+'\n'
#        s_amex += '"'+symbol.strip()+'" "STK" "SMART" "" "" "" "USD" "" "5 Y" "1 day" "1" "TRADES" "1" ""'+'\n'
#
    s_nasdaq = ""
#    
#    for symbol in Df_nasdaq.symbol:
#        if "^" in symbol or "." in symbol:
#            continue
#        #s_nasdaq += '"'+symbol.strip()+'" "STK" "SMART/NASDAQ" "" "" "" "USD" "" "10 D" "1 min" "1" "TRADES" "10" ""'+'\n'
#        s_nasdaq += '"'+symbol.strip()+'" "STK" "SMART/NASDAQ" "" "" "" "USD" "" "5 Y" "1 day" "1" "TRADES" "1" ""'+'\n'

    with open('/root/jTWSdump_707/requests/zipline.txt','w+') as f:
        f.write(start)
        f.write(s_sp500)
        f.write(s_nyse)
        f.write(s_amex)
        f.write(s_nasdaq)
        
        f.close()

    

if __name__ == '__main__':
	main()
	

#!/usr/bin/env python
# encoding: utf-8


#Created by Peter Bakker on 2017-10-04.
#Copyright (c) 2017 . All rights reserved.

import sys
import os
from zipline.data.bundles import register
sys.path.append(os.path.dirname(__file__))
from zipline.data.bundles.csvdir import csvdir_equities

os.environ['CSVDIR'] = "/root/data"
register('csvdir',csvdir_equities(["daily"], "/root/data"))
register('csvdirmin',csvdir_equities(["minute"], "/root/data"))

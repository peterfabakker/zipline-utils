#!/bin/bash
cd /root/jTWSdump_707
cp /root/data/subject.txt /root/data/output.log
python /root/data/getsymbols.py &>> /root/data/output.log
./RunUX /root/jTWSdump_707/requests/zipline.txt &>> /root/data/output.log
cp /root/jTWSdump_707/dump/*_1_day_* /root/data/daily
cp /root/jTWSdump_707/dump/*_1_min_* /root/data/minute
python /root/data/processfiles.py &>> /root/data/output.log
/usr/local/bin/zipline ingest -b csvdir &>> /root/data/output.log
/usr/local/bin/zipline ingest -b csvdirmin &>> /root/data/output.log
/usr/local/bin/zipline ingest -b quantopian-quandl &>> /root/data/output.log
/usr/local/bin/zipline clean -b csvdir --keep-last 2 &>> /root/data/output.log
/usr/local/bin/zipline clean -b csvdirmin --keep-last 2 &>> /root/data/output.log
/usr/local/bin/zipline clean -b quantopian-quandl --keep-last 2 &>> /root/data/output.log
/usr/sbin/sendmail peter.f.a.bakker@gmail.com < /root/data/output.log

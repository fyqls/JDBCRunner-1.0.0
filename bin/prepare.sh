#!/bin/bash

echo "the program is used test hengfeng...."
#read -p "please input threadNum which need to be tested: " threadNum 
#read -p "please input case  HYPE1|HYPE2|HYPE3|HYPE4|HYPE5:" hype

threadNum=25
hype=$1

basedir=`dirname $0`

lineNum=`cat ${basedir}/../conf/hbase-test.xml|grep -n "<name>io.transwarp.threadnum</name>"|awk -F ':' '{print $1}'`
echo $lineNum
lineNum=$(($lineNum+1))
#<value>50</value>
#sed "${lineNum}s/^.*$/    <value>20<\/value>/" hbase-test.xml
sed -i "${lineNum}s/[0-9]\{1,\}/${threadNum}/" ${basedir}/../conf/hbase-test.xml

lineNum2=`cat ${basedir}/../conf/hbase-test.xml|grep -n "<name>io.transwarp.sql.sqlcreator</name>"|awk -F ':' '{print $1}'`
echo $lineNum2
lineNum2=$(($lineNum2+1))
#<value>io.transwarp.sql.hengfeng.HYPERBASE$HYPE5</value>
sed -i "${lineNum2}s/HYPE[0-9]\{1,2\}/${hype}/" ${basedir}/../conf/hbase-test.xml

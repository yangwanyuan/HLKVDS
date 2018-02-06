#!/bin/bash

if [ ! -f "test.img" ]
then
    dd if=/dev/zero of=test.img bs=1M count=500
fi
if [ ! -f "test_file0" ]
then
    dd if=/dev/zero of=test_file0 bs=1M count=4
fi
if [ ! -f "test_file1" ]
then
    dd if=/dev/zero of=test_file1 bs=1M count=4
fi
if [ ! -f "test_file2" ]
then
    dd if=/dev/zero of=test_file2 bs=1M count=4
fi

#echo "start test ..."
for i in `find ./ -maxdepth 1 -type f -executable -exec file -i '{}' \;| grep binary|grep -v sh|grep -v "\.o"|awk -F":" '{print $1}'`
do  
	echo "================================"
	echo "start test  "$i
	./$i --gtest_output="xml:$i.xml"
	echo "done. =========================="
done 

rm -fr test.img
rm -fr testfile0
rm -fr testfile1
rm -fr testfile2

echo "done."

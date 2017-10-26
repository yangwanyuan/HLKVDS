#!/bin/bash

if [ ! -f "test.img" ]
then
    dd if=/dev/zero of=test.img bs=1M count=500
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

echo "done."

#!/bin/bash

if [ `lsblk |grep loop2|wc -l` -eq 0 ]
then 
	dd if=/dev/zero of=test.img bs=1M count=500
	losetup /dev/loop2 test.img
fi 

echo "start test ..."
#exit 0
for i in `find -type f -executable -exec file -i '{}' \;| grep binary|awk -F":" '{print $1}'`
do  
	echo $i
	./$i
done 
echo "done."

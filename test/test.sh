#!/bin/bash

echo "start test ..."
for i in `find -type f -executable -exec file -i '{}' \;| grep binary|awk -F":" '{print $1}'`
do  
	echo $i
	/bin/sh $i
done 
echo "done."

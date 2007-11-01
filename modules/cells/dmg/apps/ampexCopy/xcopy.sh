#!/bin/sh
c=0
cat T* |\
while read x 
do

(
echo "Starting $x" 
sleep 5 
echo "Finished $x"
)  &

c=`expr $c + 1`
if [ $c -gt 3 ] 
then
   echo "Waiting ... "
   jobs
   wait 
   echo "Done ... "
   c=0;
fi
done 

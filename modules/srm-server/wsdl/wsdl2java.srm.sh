#!/bin/sh
   command=`which $0`
   commanddir=`dirname $command`
   SRM_PATH=`dirname $commanddir`
CP=$CLASSPATH
for i in `ls ${SRM_PATH}/lib/axis/*.jar` 
do
	CP="${i}:${CP}"
done
echo CLASSPATH is ${CP}
java -cp ${CP} org.apache.axis.wsdl.WSDL2Java $* 

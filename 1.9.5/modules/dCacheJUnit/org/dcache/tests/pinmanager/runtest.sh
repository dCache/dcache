#!/bin/sh
top=../../../../../..
externadir=${top}/modules/external/
distdir=${top}/dist
classpath=${top}/modules/dCacheJUnit
for i in `find $externadir -name \*jar` ; do
classpath=$classpath:$i
done
for i in `find $distdir/classes -name \*jar` ; do
classpath=$classpath:$i
done
#echo $classpath
javac -cp $classpath PinManagerTest.java
java -cp $classpath -Dlog4j.configuration=logConfig.xml  org.dcache.tests.pinmanager.PinManagerTest


here=`pwd`
CP="$here/build/classes:$here/etc"
for j in lib/*.jar
do
    CP="$CP:$here/$j"
done

if [ "X$CLASSPATH" = "X" ]; then
    export CLASSPATH=${CP}
else
    export CLASSPATH=${CP}:${CLASSPATH}
fi

#!/bin/sh
#
#
# split the arguments into the options -<key>=<value> and the 
# positional arguments.
#
export packSet
packSet=protocols
#
args=""
while [ $# -gt 0 ] ; do
  if expr "$1" : "-.*" >/dev/null ; then
     a=`expr "$1" : "-\(.*\)" 2>/dev/null`
     key=`echo "$a" | awk -F= '{print $1}' 2>/dev/null`
     value=`echo "$a" | awk -F= '{print $2 }' 2>/dev/null`
     if [ -z "$value" ] ; then a="${key}=true" ; fi
     eval "$a"
     a="export ${key}"
     eval "$a"
  else
     args="${args} $1"
  fi
  shift 1
done
#
if [ ! -z "$args" ] ; then
   set `echo "$args" | awk '{ for(i=1;i<=NF;i++)print $i }'`
fi
#
which javac >/dev/null 2>/dev/null
if [ $? -ne 0 ]  ; then
   echo " * Java compiler not found"
   exit 0
fi
#     end of init
cd ..
n=`find . -name "*.class" 2>/dev/null`

#echo \( -z "${n}" \) -a \( -z "${compile}" \)
if [ \( -z "${n}" \) -a \( -z "${compile}" \) ] ; then
    echo " ! Package not yet compiled (enforcing compilation)" 
    compile=yes
fi

packFile="./packageSets/${packSet}.pl"
if [ ! -f ${packFile} ] ; then
   echo " * Sorry, can't file Package Set at ${packFile}" 1>&2
   exit 4
fi
if [ ! -z "${compile}" ] ; then 
    echo " ! Compiling Package Set : ${packSet}"
    export CLASSPATH
    CLASSPATH=`pwd`
    cat ${packFile} | while read pack ; do
       echo "   Compiling Package ${pack}"
       javac -g ${pack}/*.java
    done
fi
echo "   Creating Jar file"
JAR=./cells-protocols.jar
rm -rf ${JAR}
touch ${JAR}
cat ${packFile} | while read pack ; do
   echo "   Jar'ing Package ${pack}"
   jar -u0f ${JAR} ${pack}/*.class
done
echo "   Done, File at `pwd`/${JAR}"
exit 0

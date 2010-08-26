#!/bin/sh
#
#
# split the arguments into the options -<key>=<value> and the 
# positional arguments.
#
export packSet
packSet=core
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
if [ ! -z "${help}" ] 
   then
       echo "Usage : ... [options]"
       echo "     Options :"
       echo "        -help : this text"
       echo "        -version=[no|yes]  ; default : -version=yes"
       echo "        -packSet=<package> : default : -packSet=core"
       echo "        -compile=[no|yes]  : deafult : -compile=yes"
       echo ""
       exit 1
   fi
#
#   'version' default is true
#
[  -z "${version}" ] && version=true
[ \( "${version}" = "no" \) -o \( "${version}" = "off" \) ] && version=""
#
[  -z "${compile}" ] && compile=true
[ \( "${compile}" = "no" \) -o \( "${compile}" = "off" \) ] && compile=""
#
#
#echo "version : >${version}<"
#
#   checking for java
#
problem() {
    echo ""
    echo "  Problem "
    echo ""
    echo "   $2"
    echo ""
    exit $1
}
#
MANIFEST=manifest-tmp
#
getJavaVersion() {
   x=`java -version  2>&1 >/dev/null | tr -d "\"" | awk '/java version/{ print $3 }'`
   echo $x
}
makeManifest() {
   ver=`echo $1 | awk -F- '{ printf "%s.%s.%s\n",$2,$3,$4 }'`
   echo "Name: dmg/cells/nucleus/"
   if [ -z "$2" ] ; then
      echo "Specification-Title: Java Cell Nucleus"
   else
      echo "Specification-Title: Java Cell Nucleus (compiled java $2)"
   fi
   echo "Specification-Version: ${ver}"
   echo "Specification-Vendor: Desy, Hamburg Data Management Group"
   echo "Package-Title: dmg.cells.nucleus"
   echo "Package-Version: $1"
   echo "Package-Vendor:  Desy Hamburg, Data Management Group"
}
#
#     end of init
#
cd ..
#

if [ ! -z "${version}" ]
  then
      cd jobs
      echo "  Versioning ENABLED"
      rm -rf ${MANIFEST} 2>/dev/null
      echo "Main-Class: dmg.cells.services.Domain" >${MANIFEST}
      echo "" >>${MANIFEST}
      which cvs >/dev/null 2>/dev/null
      [ $? -ne 0 ]  && problem  4 " * Code Versioning Systen (cvs) not found"
      javaVersion=`getJavaVersion`
      echo "Java Version : ${javaVersion}"
      tag=`cvs status makeCells.sh | awk '/Sticky Tag:/{ print $3 }' 2>/dev/null`
      #
      # version=<version>
      #   overwrites tag from cvs
      #
      [ \( "${version}" != "true"  \) -a \( "${version}" != "on" \) ] && tag=${version}
      #
      echo "Tag : ${tag}"
      #
      if [ -z "${tag}" ] 
        then
            makeManifest cells-0-0-0 ${javaVersion} >>${MANIFEST}
      elif [ "${tag}" = "(none)" ] 
        then
            makeManifest cells-0-0-0  ${javaVersion} >>${MANIFEST}
      else
            makeManifest ${tag} ${javaVersion} >>${MANIFEST}
      fi
      if [ ! -z "${verbose}" ] 
      then
        echo ""
        echo "   <-- manifest -->"
        echo ""
        cat ${MANIFEST}   
        echo ""
        echo "   <-------------->"
        echo ""
      fi
      cd ..

  else
      echo "  Versioning DISABLED"
  fi
#
packFile="./packageSets/${packSet}.pl"
if [ ! -f ${packFile} ] ; then
   echo " * Sorry, can't file Package Set at ${packFile}" 1>&2
   exit 4
fi
if [ ! -z "${compile}" ] ; then 
    echo "  Complitation ENABLED"
    which javac >/dev/null 2>/dev/null
    [ $? -ne 0 ]  && problem  4 " * Java compiler not found"
    echo "   Compiling Package Set : ${packSet}"
    export CLASSPATH
    CLASSPATH=`pwd`
    cat ${packFile} | while read pack ; do
       echo "   Compiling Package ${pack}"
       javac -g ${pack}/*.java
    done
else
    echo "  Complitation DISABLED"
fi
echo "   Creating Jar file"
rm -rf ./cells.jar
touch ./cells.jar
cat ${packFile} | while read pack ; do
   echo "   Jar'ing Package ${pack}"
   jar -uf ./cells.jar ${pack}/*.class
done
[ -f jobs/manifest-tmp ] && jar -umf jobs/manifest-tmp ./cells.jar
echo "   Done, File at `pwd`/cells.jar"
exit 0

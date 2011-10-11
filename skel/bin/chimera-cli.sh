#!/bin/sh

if [ $# -lt 2 ]
then
    echo "Usage <command> <path> [options]"
    exit 1
fi

class_for_command() # in $1 command name, out $2 class
{
    case $1 in
	Chgrp|chgrp)
	    class=Chgrp
	    ;;

	Chmod|chmod)
	    class=Chmod
	    ;;

	Chown|chown)
	    class=Chown
	    ;;

	Ls|ls)
	    class=Ls
	    ;;

	Lstag|lstag)
	    class=Lstag
	    ;;

	Mkdir|mkdir)
	    class=Mkdir
	    ;;

	Readtag|readtag)
	    class=Readtag
	    ;;

	Writetag|writetag)
	    class=Writetag
	    ;;

        Setfacl|setfacl)
            class=Setfacl
            ;;

        Getfacl|getfacl)
            class=Getfacl
            ;;

	Writedata|writedata)
	    class=Writedata
	    ;;

        Checksum|checksum)
            class=Checksum
            ;;

	*)
	    echo "Unknown command $1.  Available commands are:"
            echo "    chgrp chmod chown ls lstag mkdir readtag writetag writedata"
            echo "    setfacl getfacl checksum"
            exit 1
	    ;;
    esac

    cmd=$2=org.dcache.chimera.cli.$class
    eval $cmd
}

class_for_command "$1" class
shift


# Initialize environment. /etc/default/ is the normal place for this
# on several Linux variants. For other systems we provide
# /etc/dcache.env. Those files will typically declare JAVA_HOME and
# DCACHE_HOME and nothing else.
[ -f /etc/default/dcache ] && . /etc/default/dcache
[ -f /etc/dcache.env ] && . /etc/dcache.env

# Set home path
if [ -z "$DCACHE_HOME" ]; then
    DCACHE_HOME="@dcache.home@"
fi
if [ ! -d "$DCACHE_HOME" ]; then
    echo "$DCACHE_HOME is not a directory"
    exit 2
fi

. @dcache.paths.bootloader@/loadConfig.sh

dbpass=$(getProperty chimera.db.password)

CLASSPATH="$(getProperty dcache.paths.classpath)" \
    ${JAVA} $(getProperty dcache.java.options) \
    "-Dlogback.configurationFile=$(getProperty dcache.paths.share)/xml/logback-cli.xml" \
    ${class}  $(getProperty chimera.db.driver) \
    $(getProperty chimera.db.url) \
    $(getProperty chimera.db.dialect) \
    $(getProperty chimera.db.user) \
    ${dbpass:-""} \
    "$@"

#!/bin/sh
#if script returns to quickly java does not see error code!!!
sleep 1
#default values
globus_location=$GLOBUS_LOCATION
if [ "$globus_location" = "" ]
then
    which grid-proxy-init >/dev/null 2>&1
    rc=$?
    if [ "$rc" = "0" ]
    then
      globus_location=`which grid-proxy-init`
      globus_location=`dirname $globus_location`
      globus_location=`dirname $globus_location`
    fi
fi
src_protocol="gsiftp"
dst_protocol="gsiftp"
use_kftp="true"

function usage()
{
	echo "usage: url_copy.sh -src-protocol <protocol> -src-host-port <host:port> -src-path <path> \
-dst-protocol <protocol> -dst-host-port <host:port> -dst-path <path> -proxy <path to proxy> \
-use-kftp <true/false> -debug <true/false> -globus-location <globus location>" 
        echo " "
        echo "  options  -src-host-port -src-path -dst-protocol and -dst-host-port are mandatory"
	echo "  default protocols are gsiftp"
	echo "  default values:"
	echo "    src-protocol gsiftp"
	echo "    dst-protocol gsiftp"
        echo "    use-kftp false"
	echo "    debug false"
	echo "    globus-location /usr/local/globus"
	echo "  if proxy is not specified, assume the default proxy location"
}

function test_that_arg_exists()
{
	decho "test_that_arg_exists ( $1,$2)"
	if [ "$2" = "" ]
	then
		echo $1 option must be specified
		usage
		exit 1
	fi
}

function decho()
{
	if [ "$debug" = "true" ] 
	then
		echo $*
	fi
}


while [ ! -z "$1" ]
do
    decho parsing option $1 $2
    case "$1" in
        -src-protocol) 
	       src_protocol=$2
	       decho "setting src_protocol to $2"
	       ;;
	-src-host-port) 
	       src_host_port=$2
	       decho "setting  src_host_port to $2"
	       ;;
        -src-path)  
		src_path=$2
		decho "setting src_path to $2"
		;;
        -dst-protocol) 
	        dst_protocol=$2
		decho "setting dst_protocol to $2"
		;;
	-dst-host-port) 
	        dst_host_port=$2
		decho "setting dst_host_port to $2"
		;;
        -dst-path)  
	        dst_path=$2
		decho "setting dst_path to $2"
		;;
	-proxy) 
	        proxy=$2
		decho "setting proxy to $2"
		;;
	-use-kftp) 
	        use_kftp=$2
		decho "setting use_kftp to $2"
		;;
	-globus-location) 
	        globus_location=$2
		decho "setting globus_location to $2"
		;;
	-debug) 
	       debug=$2
	       decho "debug is $debug"
	      ;; 
        *) 
	    	echo "unknown option : $1" >&2
	    	exit 1
	    	;;
    esac
    shift
    shift
done

if [ "$globus_location" = "" ]
then
	echo can not determine globus location >&2
	exit 1
fi
test_that_arg_exists -src-host-port $src_host_port
test_that_arg_exists -src-path  $src_path
test_that_arg_exists -dst-host-port $dst_host_port
test_that_arg_exists  -dst-path  $dst_path
if [ "$src_protocol" = "dcap" ]
then
	if [ "$dst_protocol" != "file" ] 
	then
		echo can only copy from dcap url to file >&2
		exit 1
	fi
	which dccp >/dev/null 2>&1
	dccp_found=$?
	if [  "$dccp_found" != "0" ]
	then
		. /usr/local/etc/setups.sh
		setup dcap
	fi
	which dccp >/dev/null 2>&1
	dccp_found=$?
	if [  "$dccp_found" != "0" ]
	then
		echo can not find dccp >&2
		exit 1
	fi
	cmd="dccp -d 3 dcap://$src_host_port/$src_path $dst_path"
	decho $cmd
	exec $cmd
	#this will not be executed if we use exec
        exit_code=$?
        decho dccp exit code is $exit_code
	exit $exit_code
fi

if [ "$dst_protocol" = "dcap" ]
then
	if [ "$src_protocol" != "file" ] 
	then
		echo can only copy to dcap url from file >&2
		exit 1
	fi
	which dccp >/dev/null 2>&1
	dccp_found=$?
	if [  "$dccp_found" != "0" ]
	then
		. /usr/local/etc/setups.sh
		setup dcap
	fi
	which dccp >/dev/null 2>&1
	dccp_found=$?
	if [  "$dccp_found" != "0" ]
	then
		echo can not find dccp >&2
		exit 1
	fi
	cmd="dccp -d 3 $src_path dcap://$dst_host_port/$dst_path"
	decho $cmd
	exec $cmd
	#this will not be executed if we use exec
        exit_code=$?
        decho dccp exit code is $exit_code
	exit $exit_code
fi


if [ "$proxy" != "" ]
then
    export X509_USER_PROXY=$proxy
    chmod 600 $X509_USER_PROXY
fi

decho X509_USER_PROXY = $X509_USER_PROXY

if [ "$src_protocol" = "gsiftp" ]
then
	if [ "$use_kftp" = "true" ]
	then
		if [ "$dst_protocol" = "gsiftp" ]
		then
	        	which urlcp >/dev/null 2>&1
	 	        kftpcp_found=$?
			if [  "$kftpcp_found" != "0" ]
			then
				. /usr/local/etc/setups.sh
				setup kftp
				setup gsspy_gsi
			fi
			which urlcp >/dev/null 2>&1
			kftpcp_found=$?
			if [  "$kftpcp_found" != "0" ]
			then
				echo can not find kftpcp >&2
			   	exit 1
                        fi
			decho found kftpcp
                        cmd="urlcp gsiftp://${src_host_port}/${src_path} gsiftp://${dst_host_port}/${dst_path}"
			decho $cmd
			exec $cmd
                	#this will not be executed if we use exec
			exit_code=$?
			decho urlcp exit code is $exit_code
			exit $exit_code
		fi
		
                if [ "$dst_protocol" = "file" ]
                then
	        	which kftpcp >/dev/null 2>&1
	 	        kftpcp_found=$?
			if [  "$kftpcp_found" != "0" ]
			then
				. /usr/local/etc/setups.sh
				setup kftp
				setup gsspy_gsi
			fi
			which kftpcp >/dev/null 2>&1
			kftpcp_found=$?
			if [  "$kftpcp_found" != "0" ]
			then
				echo can not find kftpcp >&2
				exit 1
			fi
			decho found kftpcp
			src_host=$(echo "$src_host_port" | tr ":" " " | awk '{printf $1 }')
			src_port=$(echo "$src_host_port" | tr ":" " " | awk '{printf $2 }')
			cmd="kftpcp -mp -xe -v -p $src_port $src_host:$src_path $dst_path"
			decho $cmd
			exec $cmd
                	#this will not be executed if we use exec
			exit_code=$?
			decho kftpcp exit code is $exit_code
			exit $exit_code
 		fi
                echo kftp can copy from gsiftp url to file or to another gsiftp url only >&2
                exit 1

	fi
fi


if [ "$dst_protocol" = "gsiftp" ]
then
	if [ "$use_kftp" = "true" ]
	then
		if [ "$src_protocol" != "file" ]
		then
			echo kftp can copy to gsiftp url from file only >&2
			exit 1
		fi
			
	        which kftpcp >/dev/null 2>&1
 	        kftpcp_found=$?
		if [  "$kftpcp_found" != "0" ]
		then
			. /usr/local/etc/setups.sh
			setup kftp
			setup gsspy_gsi
		fi
		which kftpcp >/dev/null 2>&1
		kftpcp_found=$?
		if [  "$kftpcp_found" != "0" ]
		then
			echo can not find kftpcp >&2
			exit 1
		fi
		decho found kftpcp
		dst_host=$(echo "$dst_host_port" | tr ":" " " | awk '{printf $1 }')
		dst_port=$(echo "$dst_host_port" | tr ":" " " | awk '{printf $2 }')
		cmd="kftpcp -mp -xe -v -p $dst_port $src_path $dst_host:$dst_path" 
		decho $cmd
		exec $cmd
                #this will not be executed if we use exec
		exit_code=$?
		decho kftpcp exit code is $exit_code
		exit $exit_code
		
	fi
fi

decho using globus
export GLOBUS_LOCATION=$globus_location
. $GLOBUS_LOCATION/etc/globus-user-env.sh

src="$src_protocol://$src_host_port/$src_path"
dst="$dst_protocol://$dst_host_port/$dst_path"

cmd=globus-url-copy
if [ "$debug" = "true" ]
then
	cmd="$cmd -dbg"
fi
cmd="$cmd -nodcau $src $dst"
decho $cmd
exec $cmd
#this will not be executed if we use exec
exit_code=$?
decho globus-url-copy exit code is $exit_code
exit $exit_code

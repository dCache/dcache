#!/bin/sh
#if script returns to quickly java does not see error code!!!
sleep 1
#default values

node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`

if [ -r /usr/local/bin/ENSTORE_HOME ]; then
   . /usr/local/bin/ENSTORE_HOME
else
   echo `date` ERROR: Can NOT determine E_H.  Add /usr/local/bin/ENSTORE_HOME link
   exit 1
fi
															
globus_location=$E_H/globus
src_protocol="gsiftp"
dst_protocol="gsiftp"
use_kftp="false"

function usage()
{
	echo "usage is out of date"
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
	echo "    globus-location $globus_location"
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
	-proxy)
	       X509_USER_PROXY=$2
               chmod 600 $X509_USER_PROXY
               export X509_USER_PROXY
	       decho "X509_USER_PROXY is $X509_USER_PROXY"
	       X509_USER_CERT=$2
               export X509_USER_CERT
	       decho "X509_USER_CERT is $X509_USER_CERT"
	       X509_USER_KEY=$2
               export X509_USER_KEY
	       decho "X509_USER_KEY is $X509_USER_KEY"
	      ;; 
	-x509_user_proxy) 
	       X509_USER_PROXY=$2
               chmod 600 $X509_USER_PROXY
               export X509_USER_PROXY
	       decho "X509_USER_PROXY is $X509_USER_PROXY"
	      ;; 
	-x509_user_key) 
	       X509_USER_KEY=$2
               export X509_USER_KEY
	       decho "X509_USER_KEY is $X509_USER_KEY"
	      ;; 
	-x509_user_cert) 
	       X509_USER_CERT=$2
               export X509_USER_CERT
	       decho "X509_USER_CERT is $X509_USER_CERT"
	      ;; 
	-x509_user_certs_dir) 
	       x509_user_certs_dir=$2
	       decho "x509_user_certs_dir is $x509_user_certs_dir"
	      ;; 
	-buffer_size) 
	       buffer_size=$2
	       decho "buffer-size is $buffer_size"
	      ;; 
	-tcp_buffer_size) 
	       tcp_buffer_size=$2
	       decho "tcp-buffer-size is $tcp_buffer_size"
	      ;; 
 	-parallel_streams) 
	       parallel_streams=$2
	       decho "parallel_streams is $parallel_streams"
	      ;; 
       -src_username)
	      src_username=$2
	      decho "src_username is $src_username"
	      ;;
	-src_userpasswd)
	      src_userpasswd=$2
	      decho "src user passwrd is specified"
	      ;;
        -dst_username)
	      dst_username=$2
	      decho "dst username is $username"
	      ;;
	-dst_userpasswd)
	      dst_userpasswd=$2
	      decho "dst user passwrd is specified"
	      ;;
        *) 
	    	echo "unknown option : $1" >&2
	    	exit 1
	    	;;
    esac
    shift
    shift
done
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
                unset DCACHE_IO_TUNNEL
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
                unset DCACHE_IO_TUNNEL
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



if [ "$src_protocol" = "gsiftp" ]
then
        if [ \( "$dst_protocol" = "gsiftp"  -o  "$dst_protocol" = "file" \) -a "$use_kftp" = "true" ]
	then
                if [ "$dst_protocol" = "gsiftp" ]
		then
                    which urlcp >/dev/null 2>&1
                    urlcp_found=$?
                    if [  "$urlcp_found" != "0" ]
                    then
                            . /usr/local/etc/setups.sh
                            setup kftp
                            setup gsspy_gsi
                    fi
                    which urlcp >/dev/null 2>&1
                    urlcp_found=$?
                    if [  "$urlcp_found" != "0" ]
                    then
                            echo can not find urlcp >&2
                            exit 1
                    fi
                    src="$src_protocol://"
                    if [ "$src_username" != "" ]
                    then
                            src="${src}${src_username}"
                            if [ "$src_userpasswd" != "" ]
                            then
                                    src="${src}:${src_userpasswd}"
                            fi
                            src="${src}@"
                    fi

                    src="${src}${src_host_port}/${src_path}"

                    dst="$dst_protocol://"
                    if [ "$dst_username" != "" ]
                    then
                            dst="${dst}${dst_username}"
                            if [ "$dst_userpasswd" != "" ]
                            then
                                    dst="${dst}:${dst_userpasswd}"
                            fi
                            dst="${dst}@"
                    fi
                    dst="${dst}$dst_host_port/$dst_path"
                    cmd="urlcp -t unitree:unitree $src $dst"
                    decho $cmd
                    exec $cmd
                    #this will not be executed if we use exec
                    exit_code=$?
                    decho urlcp exit code is $exit_code
                    exit $exit_code
		else
			
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

src="$src_protocol://"
if [ "$src_username" != "" ]
then
	src="${src}${src_username}"
	if [ "$src_userpasswd" != "" ]
	then
		src="${src}:${src_userpasswd}"
	fi
	src="${src}@"
fi
	
src="${src}${src_host_port}/${src_path}"

dst="$dst_protocol://"
if [ "$dst_username" != "" ]
then
	dst="${dst}${dst_username}"
	if [ "$dst_userpasswd" != "" ]
	then
		dst="${dst}:${dst_userpasswd}"
	fi
	dst="${dst}@"
fi
dst="${dst}$dst_host_port/$dst_path"

cmd=globus-url-copy
if [ "$debug" = "true" ]
then
	cmd="$cmd -dbg"
fi
if [ "$tcp_buffer_size" != "" ]
then
    cmd="$cmd -tcp-bs $tcp_buffer_size"
fi 

if [ "$buffer_size" != "" ]
then
    cmd="$cmd -bs $buffer_size"
fi 

if [ "$parallel_streams" != "" ]
then
    cmd="$cmd -p $parallel_streams"
fi 

cmd="$cmd -nodcau $src $dst"
decho $cmd
exec $cmd
#this will not be executed if we use exec
exit_code=$?
decho globus-url-copy exit code is $exit_code
exit $exit_code

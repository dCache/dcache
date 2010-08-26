###############################################################################
#    scripts sets up srm product execution environment                        #
###############################################################################
version=1.17
if [ -z "$SRM_PATH" ] 
then 
    if [ -d  /opt/f-srmcp-$version ]
    then 
         export SRM_PATH=/opt/f-srmcp-$version
    fi
fi

if [ ! -z "$SRM_PATH" ]
then
    if [ -z "$PATH" ]
    then 
    	export PATH=$SRM_PATH/bin
    else
    	export PATH=$PATH:$SRM_PATH/bin
    fi
    if [ -z "$MANPATH"  ]
    then 
    	export MANPATH=$SRM_PATH/man
    else
    	export MANPATH=$MANPATH:$SRM_PATH/man
    fi
else 
   echo "can't determine location of srm client, please define SRM_PATH enviroment variable" >&2
fi

         
       

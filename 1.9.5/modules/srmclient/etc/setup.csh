###############################################################################
#    scripts sets up srm product execution environment                        #
###############################################################################
set version=1.17
if ( ! "$?SRM_PATH" ) then 
    if ( -d  /opt/f-srmcp-$version ) then 
         setenv SRM_PATH "/opt/f-srmcp-${version}"
    endif
endif

if ( "$?SRM_PATH" ) then
    if ( ! "$?PATH" ) then 
    	setenv PATH "${SRM_PATH}/bin"
    else
        set DELIM=:
    	setenv PATH "${PATH}${DELIM}$SRM_PATH/bin"
    endif
    if ( ! "$?MANPATH" )  then 
    	setenv MANPATH "$SRM_PATH/man"
    else
        set DELIM=:
    	setenv MANPATH "$MANPATH${DELIM}$SRM_PATH/man"
    endif
else  
   echo "can't determine location of srm client, please define SRM_PATH enviroment variable" 
endif

         
       

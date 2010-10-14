
set DIRLIBS=(lib/*.jar)

set CP="build/classes:etc"
foreach i (${DIRLIBS})
      set CP = ${CP}:"$i"
end

if ( ! $?CLASSPATH ) then
    setenv CLASSPATH ${CP}
else
    setenv CLASSPATH ${CP}:${CLASSPATH}
endif

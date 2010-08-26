#!/bin/sh


ourHomeDir=/opt/d-cache
ourHomeDir=/opt/d-cache

. ${ourHomeDir}/classes/extern.classpath
. ${ourHomeDir}/config/dCacheSetup

${java} ${java_options} -classpath ${externalLibsClassPath} \
              -Xmx512M org.dcache.chimera.acl.client.GetAclClient ${ourHomeDir}/config/acl.properties

------------------------------------------------------------------------------------
  README-gplazma.txt 
  Last major-update March 31, 2005
  Last minor-update April 21, 2005
  Abhishek Singh Rana (rana@fnal.gov)
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
Interim important note: Timur & I will commit the following files to reflect
integrated changes only after testing early next week. Please wait before 
compiling/testing this module till then.

 - DCacheAuthorization.java
 - Storage.java
 - AbstractFtpDoorV1.java
 - Configuration.java
 - GssFtpDoorV1.java
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------

Suggested location for Site|Storage provider's policy configuration file for this module:

   ${dcache-home}/etc/dcachesrm-gplazma.policy

Please fetch the above file from CVS if not available in your dCache-SRM distribution.
Please declare path to it in dCacheSetup, and add to srm and gridftpdoor batch files.
  dCacheSetup          --add-->    
     gplazmaPolicy=<a valid path here>
  gridftpdoor's batch  --add-->
    -use-gplazma-authorization-module=true \
    -gplazma-authorization-module-policy=${gplazmaPolicy} \
  srm server's batch   --add-->
    -use-gplazma-authorization-module=true \
    -gplazma-authorization-module-policy=${gplazmaPolicy} \
------------------------------------------------------------------------------------
Need 'at least one' of the following sets of repositories:

    Legacy (No Role) functionality
    [grid-proxy-init needed]
    ------------------------------
 -> dcache.kpwd
 -> grid-mapfile + storage-authzdb

    Fine-grain (Grid VO Role) functionality 
    [voms-proxy-init needed, backward-compatible with grid-proxy-init]
    ------------------------------------------------------------------
 -> a remote [VO identity mapping + storage-authzdb]
 -> grid-vorolemap + storage-authzdb

------------------------------------------------------------------------------------
In dcachesrm-gplazma.policy, please configure the switches|priorities. Add valid 
configuration details for all ON switches, or else they may be auto-rendered OFF. 
------------------------------------------------------------------------------------

In dCacheSetup, please add the following:

[1] Create a directory 'vomsdir' in /etc/grid-security/ and copy hostcert.pem into it, 
so that /etc/grid-security/vomsdir exists with a copy of hostcert.pem 

[2] Remove the -Dlog4j.configuration line in the following java-options in [3], log4j
implementation in VOMS seems buggy.

[3] Replace PATHTODCACHEHOME by actual path in java_options

java_options=" .................
              -Daxis.socketSecureFactory=org.glite.security.trustmanager.axis.AXISSocketFactory \
              -DsslCAFiles=${X509_CERT_DIR-/etc/grid-security/certificates}/*.0 \
              -DsslCertFile=/etc/grid-security/hostcert.pem \
              -DsslKey=/etc/grid-security/hostkey.pem \
              -Dvalidate=false \
              -Dvoms.cert.dir=/etc/grid-security/vomsdir \
              -Djava.endorsed.dirs=PATHTODCACHEHOME/classes/gplazma/vo-mapping/endorsed \
              ....................."
			  
## buggy removed
## -Dlog4j.configuration=PATHTODCACHEHOME/classes/gplazma/vo-mapping/etc/log4j.cfg \

[4]
classpath= ..................

${thisDir}/../classes/gplazma/gplazmalite/gplazmalite-services-suite-0.1.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/org.glite.security.voms-api-java.jar:${thisDir}/../classes/gplazma/vo-mapping/endorsed/dom3-xml-apis-2.5.0.jar:${thisDir}/../classes/gplazma/vo-mapping/endorsed/dom3-xercesImpl-2.5.0.jar:${thisDir}/../classes/gplazma/vo-mapping/endorsed/xalan-2.4.1.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/axis-saaj-1.2-RC2.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/bcprov-jdk14-127.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/log4j-1.2.8.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/cog-jglobus-.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/bcprov-jdk14-122.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/org.glite.security.voms-api-java-.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/privilege-1.0.1.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/opensaml-1.0.1.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/glite-security-util-java-1.0.0dev.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/axis-1.2-RC2.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/axis-jaxrpc-1.2-RC2.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/axis-wsdl4j-1.2-RC2.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/xmlsec-1.1.0.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/sunxacml-1.2.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/commons-discovery-0.2.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/glite-security-trustmanager-1.6.3dev.jar:${thisDir}/../classes/gplazma/vo-mapping/lib/commons-logging-1.0.3.jar


------------------------------------------------------------------------------------
Important note about VOMS client's version 
------------------------------------------------------------------------------------
voms-proxy-init required later than -- Version: 1.3.3
voms-proxy-init successfully tested with -- Version: 1.3.4
------------------------------------------------------------------------------------
NOTE:
------------------------------------------------------------------------------------
[A] Built-in gPLAZMAlite Service Suite's Grid-VO-Role Mapping Service has dependencies as follows:

    org.glite.security.voms-api-java.jar:glite-security-util-java-1.0.0dev.jar:cog-jglobus-.jar:bcprov-jdk14-127.jar

These jars are included in the comprehensive classpath listed above for
dCacheSetup. Please do not worry about these.

[B] Built-in gPLAZMAlite Service Suite's source files are not compiled with 
the preparePackage.sh script. Instead, a pre-prepared jar file has been 
already included in the classes/gplazma/gplazmalite directory.

[C] The preparePackage.sh has been updated to reflect the above logic. 
Just uncomment the build_gplazma line, you are all set.

[D] Attempt has been made to make dcachesrm-gplazma.policy self-explanatory.
Try various combinations of Switches|Priorities.

[E] If your site does not have a remote SAML-based mapping service, try the 
Built-in gPLAZMAlite Service Suite. VO-Role-mapping out-of-the-box.

[F] Correct version of voms-proxy-init is required to make avail of VO-Role-mapping.

------------------------------------------------------------------------------------
EOF





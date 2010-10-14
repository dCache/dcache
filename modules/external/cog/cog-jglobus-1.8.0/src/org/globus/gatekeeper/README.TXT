[Note: The documentation is very limited and rough at this point]

Requirements: J2SE 1.3.1. Some components will not compile with older versions.

This is an implementation of globus-personal-gatekeeper. It supports fork job
manager as well as any other shell-based job managers such as pbs, etc.
The shell job manager works with the same scripts as used in regular Globus 
distribution. 
The current implementation of the gatekeeper acts more as 
globus-personal-gatekeeper. Multipe users can authenticate to the gatekeeper
but all jobs/services will execute as the same user that is running the
gatekeeper.

Currently the implementation does not support:

 o Caching of files (using gass cache)
 o GRAM 1.5 protocol
 o Poe or mpirun for fork job manager, or condor submissions
 o Running services as authenticated user (all services/jobs run
   as the same user - because no setuid() support in Java)

The implementation has the following problems or limitations:

 o Synchronization problem - might not get output sometimes when
   stdout/err redirected to client.
 o If the gridmap file is updated the server must be restarted.
   The gridmap file is not automatically refreshed.

[explain configuration file]
[explain GLOBUS system properties options]

Generic Job Manager debugging:
  
  Add '(debug=yes)' to the RSL string to enable Job Manager debugging. A log 
  file will be created in user's home directory named jgram_job_mgr_<jobid>

Shell-based Job Manager issues:

  Initialization:

   Each shell-based job manager must be initialized with job manager type
   (such as lsf, condor, etc.) and the location of libexec/ directory where
   the required scripts for the job manager type reside.

   The job manager scripts will only run successfully if GLOBUS_LOCATION 
   (for Globus 2.0) or GLOBUS_INSTALL_PATH (for Globus 1.1.3) are set
   through Java system properties. Make sure to initialize this property
   with appropriate Globus installation path.

  Backend job manager script debugging:

   Add '(scriptLogFile=<file>)' to the RSL string to enable backend-script
   debugging for shell-based job managers. A log file will be created as
   indicated by the <file> value.

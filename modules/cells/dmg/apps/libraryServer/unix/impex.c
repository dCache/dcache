#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mtio.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <setjmp.h>
#include <syslog.h>
#include <pwd.h>
#include <sys/param.h>
#include "ocs.h"
#include "mocs.h"

int do_the_allocate( char *hostg , char *devg , char *devt ,
                     int timeout ,  devDesc *result             );
int do_the_deallocate( devDesc *dev ) ;
int do_the_mount( devDesc *dev , char *vsn , char *rw ,
                  char *oprmsg , int timeout            );
int do_wait_ready( devDesc *dev ); 
int do_open_door( devDesc *dev ) ;
void watch_it( devDesc *dev ) ;
void clean_it_up( devDesc *dev ) ;
void do_usage( char *name , char *more );
int  string2devDesc( devDesc *dev , char *string );
void devDesc2string( char *string , devDesc *dev );
void devDesc2log( devDesc *dev );
void deleteLog( devDesc *dev  );
void getLogFileName( devDesc *dev , char *logFile );
void impex_allocate( impex_args *args ) ;
void impex_device( impex_args *args );
void impex_deallocate( impex_args *args );
void impex_unload( impex_args *args );
void impex_check( impex_args *args );
void impex_offline( impex_args *args );
void impex_rewind( impex_args *args );
void impex_signals( int sig );
void impex_notify( impex_args *args );
void impex_toscsi( impex_args *args );
int getDeviceSpec( char *group , char *host , char *device );
int getDeviceSpec2( char *group , char *host , char *device , int *flags );
void get_user( char *user );
int do_theWait( int fd , int maxCount );

int dbg = { 0 } ;

sigjmp_buf saveIt  ;
int doLongJump = { 0 } ;

main (int argc, char **argv)
{
  int c , n , rc ;
  extern char *optarg;
  extern int optind;
  int errflg = 0;
  impex_args  args ;
  char command[128] , *volLabel , *devGroup , result[128];
  struct sigaction sa ;
  sigset_t set ;
  
/*
 * first try to find out who we are.
 */
  if( args.pn = strrchr( argv[0] , '/' ) )args.pn++ ; else args.pn  = argv[0] ;
/*
 * ------------------------------------------------------------------------
 *                      the the option stuff
 */  
  args.readFlag        = 0 ;
  args.writeFlag       = 0 ;
  args.overWriteFlag   = 0 ;
  args.errorOutputFlag = 0 ;
  args.timeoutValue    = -1 ;
  
  while ((c = getopt(argc, argv, "exrwn:")) != EOF)
    switch (c) {
       case 'e':
          args.errorOutputFlag = 1 ;
          break ;
       case 'x':
          args.overWriteFlag = 1 ;
          break ;
       case 'r':
          if( args.writeFlag ) errflg++;
          else                 args.readFlag++;
          break;
       case 'w':
          if( args.readFlag ) errflg++;
          else                args.writeFlag++;
          break;
      case 'n':
          (void)sscanf( optarg , "%d" , &args.timeoutValue ) ;
          break;
      case '?': errflg++;
  }
  if (errflg) do_usage(argv[0],NULL) ;

  args.argc = argc - optind ;
  args.argv = argv + optind ;
/*
 *
 */
  if( ! args.errorOutputFlag ){
    (void)freopen( "/dev/null" , "w" , stderr ) ; 
  }
  openlog( "impex" , LOG_PID | LOG_NOWAIT , LOG_USER ) ;
/*  
 * ----------------------------------------------------------------------
 *                   now do this stupid signal stuff.
 */

 sigemptyset( &set ) ;
 sa.sa_handler = SIG_IGN ;
 sa.sa_mask    = set ;
 sa.sa_flags   = 0 ;

 sigaction( SIGHUP , &sa , NULL ) ;
 
   sigemptyset( &set ) ;
   sa.sa_handler = impex_signals ;
   sa.sa_mask    = set ;
   sa.sa_flags   = 0 ;

   sigaction( SIGTERM , &sa , NULL ) ;
   sigaction( SIGUSR1 , &sa , NULL ) ;
   sigaction( SIGUSR2 , &sa , NULL ) ;
   sigaction( SIGINT  , &sa , NULL ) ;
   sigaction( SIGQUIT , &sa , NULL ) ;
   sigaction( SIGHUP  , &sa , NULL ) ;

 /*
  * ----------------------------------------------------------------------
  *                  the distributer
  */
  /*
   * as long as we are using the orginal name, the first argument is
   * the command. if the progam name is something different caused by
   * a link, the name itself is asumed to be the command.
   */
  if( ! strcmp( args.pn , "impex" ) ){
     if( args.argc < 1 )do_usage(args.pn,NULL) ;
     sprintf( command , "impex_%s" , args.argv[0] ) ;
     (args.argv)++ ;
     (args.argc)-- ;
  }else{
     strcpy( command ,args.pn ) ;
  }
  
  if( ! strcmp( "impex_allocate" , command ) ){ 
      impex_allocate( &args ) ;
  }else if( ! strcmp( "impex_device" , command ) ){ 
      impex_device( &args ) ;
  }else if( ! strcmp( "impex_deallocate" , command ) ){ 
      impex_deallocate( &args ) ;
  }else if( ! strcmp( "impex_check" , command ) ){ 
      impex_check( &args ) ;
  }else if( ! strcmp( "impex_unload" , command ) ){ 
      impex_unload( &args ) ;
  }else if( ! strcmp( "impex_offline" , command ) ){ 
      impex_offline( &args ) ;
  }else if( ! strcmp( "impex_rewind" , command ) ){ 
      impex_rewind( &args ) ;
  }else if( ! strcmp( "impex_notify" , command ) ){ 
      impex_notify( &args ) ;
  }else if( ! strcmp( "impex_toscsi" , command ) ){ 
      /* impex_toscsi( &args ) ; */
  }else{
      printf( " Sorry, command %s totally unknown\n" , command ) ;
      exit(1) ;
  }
  exit(0) ;
}
/*
void impex_notify( impex_args *args )
{
 char errmsg[128] , message[256] ;
 int rc ;
 
   if(  args -> argc != 2 )do_usage(args->pn,NULL) ;
   
  exit(0) ;
  
}
*/
void impex_notify( impex_args *args )
{
 char errmsg[128] , message[256] ;
 int rc ;
 
   if(  args -> argc != 2 )do_usage(args->pn,NULL) ;

   sprintf( message , " %s : %s\n" , args -> argv[0] , args -> argv[1] ) ;
   
   syslog(LOG_INFO," Message to operator : %s\n" ,message ) ;
   rc = ocs_oper_message( "any" , message , errmsg ) ;
   if(rc){
      syslog(LOG_INFO," MTO Failed with : %s\n" ,errmsg ) ;
   }
   exit(rc?1:0) ;
}
void impex_check( impex_args *args )
{
 int fd ;
 struct  mtget buf ;

  if(  args -> argc != 1 )do_usage(args->pn,NULL) ;
  printf( " Trying to open %s\n" , args -> argv[0] ) ;
  fd = open( args -> argv[0] , O_RDONLY ) ;
  if( fd < 0 ){
    printf( " open failed with %d\n" , errno ) ;
    exit(1);
  }   
  printf( " O.K. ( handle is %d)\n" , fd ) ;
  if(ioctl(fd, MTIOCGET, &buf) == -1){
     printf( " ioctl failed : %d\n" , errno ) ;
     close(fd);
     exit(1) ;
  }
  printf( " mt_type        : %d\n" , buf.mt_type ) ;
  printf( " mt_dsreg       : %d\n" , buf.mt_dsreg ) ;
  printf( " mt_erreg       : %d\n" , buf.mt_erreg ) ;
  printf( " mt_resid       : %d\n" , buf.mt_resid ) ;
  printf( " mt_fileno      : %d\n" , buf.mt_fileno ) ;
  printf( " mt_blkno       : %d\n" , buf.mt_blkno ) ;
  close(fd) ;
}

void impex_rewind( impex_args *args )
{
 int fd ;
 struct mtop op;
 
  if(  args -> argc != 1 )do_usage(args->pn,NULL) ;
  
  fd = open( args -> argv[0] , O_RDONLY | O_NDELAY ) ;
  if( fd < 0 ){
    printf( " open failed with %d\n" , errno ) ;
    exit (1) ;
  }   
  op.mt_op = MTREW ;
  if(ioctl(fd, MTIOCTOP, &op) == -1){
     printf( " ioctl MTREW failed : %d\n" , errno ) ;  
  }
                        
  close(fd) ;
}
void impex_unload( impex_args *args )
{
 int fd ;
 struct mtop op;
 
  if(  args -> argc != 1 )do_usage(args->pn,NULL) ;
  
  fd = open( args -> argv[0] , O_RDONLY | O_NDELAY ) ;
  if( fd < 0 ){
    printf( " open failed with %d\n" , errno ) ;
    exit (1) ;
  }   
  op.mt_op = MTUNLOAD ;
  if(ioctl(fd, MTIOCTOP, &op) == -1){
     printf( " ioctl MTUNLOAD failed : %d\n" , errno ) ;  
  }
                        
  close(fd) ;
}
void impex_offline( impex_args *args )
{
 int fd ;
 struct mtop op;
 
  if(  args -> argc != 1 )do_usage(args->pn,NULL) ;
  
  fd = open( args -> argv[0] , O_RDONLY | O_NDELAY ) ;
  if( fd < 0 ){
    printf( " open failed with %d\n" , errno ) ;
    exit (1) ;
  }   
  op.mt_op = MTOFFL ;
  if(ioctl(fd, MTIOCTOP, &op) == -1){
     printf( " ioctl MTOFFL failed : %d\n" , errno ) ;  
  }
                        
  close(fd) ;
}
void impex_deallocate( impex_args *args )
{
   devDesc dev ;
   char  *handle ;
   int rc ;

  if(  args -> argc != 1 )do_usage(args->pn,NULL) ;
  handle = args -> argv[0] ;
  
  if( string2devDesc( &dev , handle ) ){
     printf( " Illegal Device Handle : %s\n" , handle ) ;
     exit(1) ;
  }
  
  syslog(LOG_INFO," Deallocate for parent %d\n" , dev.ppid ) ;
  clean_it_up( &dev ) ;
  
  exit(0) ;

}
void impex_device( impex_args *args )
{
   devDesc dev ;
   char filename[128] , *handle , *attr , tmpAttr[128] ;
   int rc ;
/*
  if( ( args -> argc < 1 ) || ( args -> argc > 2 ) )do_usage(args->pn,NULL) ;
*/
  if(  args -> argc > 2 )do_usage(args->pn,NULL) ;
  else if( args -> argc < 1 ) do_usage(args->pn,NULL) ;
  else if( args -> argc == 1 ){
     if( ! strchr( args -> argv[0] , '.' ) )do_usage(args->pn,NULL) ;
     handle = args -> argv[0] ;
     attr   = "default" ;
  }else{
     attr   = args -> argv[0] ;
     handle = args -> argv[1] ;
  }
  
  if( ! strcmp( attr , "default" ) ){
     strcpy( tmpAttr , "8500" ) ;
  }else{
     sprintf( tmpAttr , "%s:8500" , attr ) ;
  }
  attr = tmpAttr ;
  
  if( string2devDesc( &dev , handle ) ){
     printf( " Illegal Drive Handle %s\n" , handle  ) ;
     exit(1) ;
  }
 
  rc = ocs_devfile_set( attr , dev.message ) ;
  if( rc ){
     printf( "Illegal Attribute : %s\n" , dev.message ) ;
     exit(1) ;
  }
  
  rc = ocs_devfile_get( dev.host , dev.device , filename , dev.message ) ;
  if(rc){
     printf( "%s\n" , dev.message ) ;
     exit(1) ;
  }
  
  printf( "%s\n" , filename ) ; fflush(stdout) ;
  exit(0) ;
    
}
void impex_allocate( impex_args *args )
{
  int rc , i , flags ;
  char *volLabel , *devGroup ;
  devDesc dev ;
  char errorMessage[128] , host[128] , deviceType[128] ;
  char deviceString[128] , user[32]  ;
  struct sigaction sa ;
  sigset_t set ;
  
  if( ( args -> argc < 1 ) || ( args -> argc > 2 ) )do_usage(args->pn,NULL) ;
  
  volLabel = args -> argv[0] ;
  devGroup = args -> argc > 1 ? args -> argv[1] : "any" ;
 
  errorMessage[0] = '\0' ;
  
  if( args -> timeoutValue <= 0 )args -> timeoutValue = 60 ;
  
  args -> timeoutValue *= 60 ;
  
  get_user( user ) ;
  
  syslog(LOG_INFO,
         " Allocation Request of %s on %s from %s (parent %d)\n" ,
         volLabel , devGroup , user  , getppid()   ) ;
  
  if( getDeviceSpec2( devGroup , host , deviceType , &flags) ){

     rc = do_the_allocate( NULL ,
                           devGroup ,
                           NULL ,
                           args -> timeoutValue  ,
                           &dev ) ;
  }else{
     rc = do_the_allocate( host ,
                           NULL ,
                           deviceType ,
                           args -> timeoutValue  ,
                           &dev ) ;
  }
  if(rc){
     printf( "%s\n" , dev.message ) ;
     syslog(LOG_INFO, " Allocation Failed : %s\n" , dev.message ) ;
     exit(  rc == MOCS_TIMEOUT ? 3 : 1 ) ;
  }
  dev.flags = flags ;
  syslog(LOG_INFO, " Allocation o.k. %s.%s\n" , dev.host , dev.device ) ;
  /*
   * from now on we are a bit sensitive concerning
   * sudden death.  so lets try  to fetch it.
   */
   if( rc = sigsetjmp(  saveIt , 0 ) ){   
     /*
      * something went wrong. it should be sufficent just to
      * deallocate the drive and make it offline.
      */
     if( rc < 1000 ){
       printf( " impex_allocate : received signal %d\n" , rc ) ;
       syslog(LOG_INFO," Will clean up ( Jump caused by signal %d )\n" , rc ) ;
       (void)do_the_deallocate( &dev ) ;
       (void)do_open_door( &dev ) ;
       exit(M_WHATEVER) ;
     }else{
         if( errorMessage[0] ){
           printf( " impex_allocate : %s\n" , errorMessage ) ;
           syslog(LOG_INFO," Will clean up ( Caused by Problem : %s )\n" ,
                  errorMessage ) ;
         }else{
           printf( " impex_allocate : %d\n" , rc ) ;
           syslog(LOG_INFO," Will clean up ( Caused by Problem : %d )\n" ,
                  rc ) ;
         }
       (void)do_the_deallocate( &dev ) ;
       (void)do_open_door( &dev ) ;
       exit( rc - 1000 ) ;
     }
   }
   doLongJump = 1 ;
  /*
   * lets make sure that the door is open.
   * ( as a first hint, lets ignore the error )
   */
  (void)do_open_door( &dev ) ;
  
  rc = do_the_mount( &dev , volLabel ,
                     args -> writeFlag ? "w" : "r" ,
                     args -> overWriteFlag ? "overwrite" : NULL ,
                     args -> timeoutValue  );
                     
  
  if( rc ){
    /*
     * what a pitty, the mount failed. so I suppose we have to 
     * deallocate the drive.
     */
    sprintf(errorMessage," Mount Failed : %d : %s\n" , rc , dev.message ) ;
    syslog(LOG_INFO," Mount Failed : %d : %s\n" , rc , dev.message ) ;
    siglongjmp( saveIt ,
                1000 + ( rc == MOCS_TIMEOUT ? M_TIMEOUT : M_OPERATOR ) ) ;
  }
  /*
   * we first have to wait that the drive is really
   * ready.
   */
   if( ! ( dev.flags & MF_NO_READY_WAIT ) ){
      syslog(LOG_INFO," Mount O.K. ( Waiting for device ready )\n" ) ;
      rc = do_wait_ready( &dev ) ;
      if( rc == MOCS_TIMEOUT ){
         sprintf(errorMessage," do_wait_ready : READY_TIMEOUT \n" ) ;
         syslog(LOG_INFO," Device Ready Timeout (Failed)\n" ) ;
         siglongjmp( saveIt , 1000 + M_TIMEOUT )  ;
      }else if( rc ){
         sprintf(errorMessage," do_wait_ready : problem %d\n" ,rc ) ;
         syslog(LOG_INFO," do_wait_ready : Failed %d\n" ,rc ) ;
         siglongjmp( saveIt , 1000 + M_WHATEVER )  ;
      }
   }
   syslog(LOG_INFO," Let's go ....\n" ) ;
  /*
   * now lets dive ....
   */   
  if( dev.watchDog = fork() ){
  
     devDesc2string( deviceString , &dev ) ;
     devDesc2log( &dev ) ;
     printf( "%s\n" , deviceString ) ;
     fflush(stdout);
     syslog(LOG_INFO,"DEBUG Exit %d watchdog %d\n",getpid(),dev.watchDog ) ;
     exit(0) ;
  }
  dev.watchDog = getpid() ;
  
  setpgrp() ;
  for( i = 0 ; i < NOFILE ; i++ )close(i) ;
  closelog() ;
  openlog( "impex" , LOG_PID | LOG_NOWAIT , LOG_USER ) ;
    
  syslog(LOG_INFO,"DEBUG Watchdog : %d watching %d\n",dev.watchDog , dev.ppid ) ;
  watch_it( &dev ) ;
  exit(0) ;
}
void get_user( char *user )
{
   uid_t uid ;
   struct passwd *pw ;
   
    pw = getpwuid( getuid() ) ;
    strcpy( user , pw ? pw -> pw_name : "Unknown" ) ;
    
   return ;
}
void impex_signals( int sig )
{
   if( doLongJump ){
      fprintf(stderr," Got signal %d and jump\n" , sig ) ;
      syslog(LOG_INFO," Got signal %d and jump\n" , sig ) ;
      siglongjmp( saveIt , sig ) ;
   }else{
      fprintf(stderr," Got signal %d and exit\n" , sig ) ;
      syslog(LOG_INFO," Got signal %d and exit\n" , sig ) ;
      exit(1);
   }

}
void do_usage( char *name , char *more )
{

   (void)printf(
    "USAGE: %s [-e] [-n timeout] [-x] [-r|w] VolumeLabel DeviceGroup\n",
            "impex_allocate" ) ;         
   (void)printf("\n" );
   (void)printf("   VolumeLabel : Human readable volume label\n" );
   (void)printf("   DeviceGroup : The Tape Device Group Name\n" );
   (void)printf("    -n timeout : Number of seconds before timeout\n" );
   (void)printf("    -r|-w      : Tape read or write\n" );
   (void)printf("    -x         : Overwrite existing tape\n" );
   (void)printf("    -e         : Write status informations to stderr\n" );
   (void)printf("\n" );
   (void)printf(
    "USAGE: %s deviceSpecification deviceHandle\n",
            "impex_device" ) ;         
   (void)printf("  deviceSpec   : Device attributes\n" );
   (void)printf("  deviceHandle : Handle returned by impex_allocate\n" );
   (void)printf("\n" );
   (void)printf(
    "USAGE: %s deviceHandle\n",
            "impex_deallocate" ) ;         
   (void)printf("  deviceHandle : Handle returned by impex_allocate\n" );
   (void)printf("\n" );
   (void)printf(
    "USAGE: %s deviceHandle message\n",
            "impex_notify" ) ;         
   (void)printf("  deviceHandle : Handle returned by impex_allocate\n" );
   (void)printf("  message      : Message to Human operator\n" );
   (void)printf("\n" );

   if(more)printf("%s\n",more);
   exit(1);
}
int do_wait_ready( devDesc *dev ) 
{
 int fd , rc ;
 char errmsg[128] , deviceName[128] ;
 struct  mtget buf ;

  rc = ocs_devfile_set( "8500" , errmsg ) ;

  rc = ocs_devfile_get( dev -> host , dev -> device , deviceName , errmsg ) ;
  if(rc){
  fprintf( stderr," do_wait_ready : Can't determine device name for %s.%s\n" ,
           dev -> host , dev -> device ) ;
  fprintf( stderr," do_wait_ready : %s\n" , errmsg ) ;
  } 
  if( rc )return rc ;
  
  fd = open( deviceName , O_RDONLY ) ;
  if( fd < 0 ){
    fprintf(stderr, " do_wait_ready : open %s failed with %d\n" ,deviceName, errno ) ;
    return -1 ;
  }   

  rc = do_theWait( fd , IMPEX_MAX_READY_WAIT ) ;
  
  close(fd) ;

  return rc ? MOCS_TIMEOUT : 0 ;
}
int do_theWait( int fd , int maxCount )
{
 struct mtget buf ;
 int count ;

  fprintf( stderr," do_theWait (%d) : " , maxCount) ;
  for(   count = maxCount , buf.mt_erreg = 0 ;
       ( count > 0 ) && ! buf.mt_erreg ; count-- ){

    if(ioctl(fd, MTIOCGET, &buf) == -1){
       fprintf( stderr,"  do_wait_ready : ioctl failed : %d\n" , errno ) ;
       return -1 ;
    }
    sleep(1);
    fprintf( stderr,"." ) ; fflush(stderr) ;
  } ;
  if( count ){
      fprintf( stderr,"\n do_theWait : Ready\n" ) ; fflush(stderr) ;
      return 0 ;
  }else{
      fprintf( stderr,"\n do_theWait : Timout\n" ) ; fflush(stderr) ;
      return 1 ;
  }
}
int do_open_door( devDesc *dev ) 
{
 int fd , rc , count ;
 char errmsg[128] , deviceName[128] ;
 struct mtop op;
 struct mtget buf ;

  rc= ocs_devfile_set( "8500" , errmsg ) ;

  rc = ocs_devfile_get( dev -> host , dev -> device , deviceName , errmsg ) ;
  if(rc){
     fprintf( stderr," do_open_door : Can't determine device name for %s.%s\n" ,
           dev -> host , dev -> device ) ;
     fprintf( stderr," do_open_door : %s\n" , errmsg ) ;
  }
   
  if( rc )return rc ;
  fprintf( stderr, " do_open_door : Open for unload %s(%s.%s)\n" , deviceName ,
                   dev -> host , dev -> device ) ;
  fd = open( deviceName , O_RDONLY | O_NDELAY) ;
  if( fd < 0 ){
    fprintf(stderr, " do_open_door : open %s failed with %d\n" ,deviceName, errno ) ;
    return -1 ;
  }   

  do_theWait( fd , 60 ) ;
  fprintf(stderr," do_open_door : Rewinding\n" ) ;
  op.mt_op = MTREW ;
  if(ioctl(fd, MTIOCTOP, &op) == -1){
     fprintf( stderr," do_open_door : ioctl MTREW failed : %d\n" , errno ) ;
/*
     close(fd);
     return -1 ;
*/
  }
  fprintf(stderr," do_open_door : Ready\n" ) ;
  do_theWait( fd , 60 ) ;
  fprintf(stderr," do_open_door : Unloading\n" ) ;
  op.mt_op = MTUNLOAD ;
  if(ioctl(fd, MTIOCTOP, &op) == -1){
     fprintf( stderr,"  do_open_door : ioctl MTUNLOAD failed : %d\n" , errno ) ;
     /*
      * dlt will return an error
      * 
     close(fd);
     return -1 ;
      */
  }
  fprintf(stderr," do_open_door : Ready\n" ) ;
  close(fd) ;

  return 0 ;
}
void clean_it_up( devDesc *dev ) 
{
  char message[128] , errmsg[128] ;
  int rc ;
/*
 * the question is now, that does clean up mean.
 */
 fprintf( stderr , " clean_it_up : Trying to dismount\n" ) ;
 rc = ocs_dismount( dev->host , dev->device , "" , errmsg ) ;
 if( rc ){
   fprintf(stderr, " ocs_dismount : %d : %s\n" , rc , errmsg ) ;
   syslog(LOG_INFO," ocs_dismount : %d : %s\n" , rc , errmsg ) ;
 }else{
   fprintf( stderr," clean_it_up : Dismount successful\n" ) ;
   syslog(LOG_INFO,"DEBUG clean_it_up : Dismount successful\n"  ) ;
 }
  /*
   * and open the door 
   */
 fprintf(stderr, "DEBUG clean_it_up : do_open_door( dev )\n" ) ;
 (void)do_open_door( dev ) ;
 fprintf(stderr, " clean_it_up : do_open_door : Ready\n" ) ;
 syslog(LOG_INFO,"DEBUG clean_it_up : do_open_door : Ready\n"  ) ;
 /*
  *
  */
 rc = do_the_deallocate( dev ) ;
 if(rc){
    fprintf(stderr, " clean_it_up : do_the_deallocate( dev ) %d\n", rc  ) ;
    syslog(LOG_INFO," clean_it_up : do_open_door : Failed %d\n",rc  ) ;
 }
 /*
  * what shalll we do if the deallocation fails ?
  * I would suggest : nothing
  */
  deleteLog( dev ) ;
  fprintf(stderr, " clean_it_up : Sending message to operator\n" ) ;
  sprintf( message , " Please dismount drive %s on %s" , dev->device,dev->host);
  rc = ocs_oper_message("all" , message , errmsg ) ;
  if(rc)syslog(LOG_INFO," ocs_oper_message : %d : %s : %s Failed\n" , rc  , errmsg , message ) ;

  return ;
}
void watch_it( devDesc *dev ) 
{
  char logFile[128] , string[128] , in[128];
  FILE *f ;
  int rc ;
  
  getLogFileName( dev , logFile  ) ;
  devDesc2string( string , dev ) ;
  sleep( SLEEP_TIME ) ;
 
  while(  ! kill( dev -> ppid , 0 ) )sleep( SLEEP_TIME ) ;
  syslog(LOG_INFO," Cleanup deamon : parent %d is gone\n" , dev -> ppid ) ;
  /*
   *  best case : our log file is gone.
   *  everything is pretty o.k.
   */
  if( ! ( f = fopen( logFile , "r" ) ) ){
     syslog(LOG_INFO,"DEBUG Cleanup deamon : parent %d : file %s not present (fine)\n" ,
             dev -> ppid , logFile ) ;
     /*printf( " file %s is gone. \n" , logFile); fflush(stdout);*/
     exit(0);
  }
  fgets( in , 127 , f ) ; 
  fclose( f ) ; 
  /*
   * next case is also not to bad.
   * log file exist, but it is already the next one.
   */
  if( strncmp( string , in , strlen( string ) ) ){
     syslog(LOG_INFO,"DEBUG Cleanup deamon : parent %d : new file found\n" , dev -> ppid ) ;
     /* printf( " is new file \n" ); fflush(stdout); */
     exit(0) ;
  }
  /*
   * this is the part this program was written for.
   * the parent process is gone, but our file is still
   * there. now we have to do the clean ups.
   */
   syslog(LOG_INFO,"DEBUG Doing cleanup for parent %d\n" , dev -> ppid ) ;
   clean_it_up( dev ) ;
   
   exit(0) ;
}
int do_the_deallocate( devDesc *dev ) 
{
 int rc ;
 
   rc = ocs_deallocate( dev -> host , dev -> device , dev -> message ) ;
   if( dbg && rc )
     fprintf(stderr," ocs_deallocate : %d : %s\n" , rc , dev -> message ) ;
     
   return rc ;

}
int do_the_mount( devDesc *dev , char *vsn , char *rw ,
                  char *oprmsg , int timeout            )
{
  int rc , status ;
  char *errmsg ;
  char  hostReply[MOCS_MAX_OBJ_LEN] , deviceReply[MOCS_MAX_OBJ_LEN] ;
  
  errmsg = dev -> message ;
  
  rc = ocs_mount_request( dev->host, dev->device, NULL ,
                          vsn , rw , oprmsg , errmsg );
  if( dbg && rc )
     fprintf(stderr," ocs_mount_request : %d : %s\n" , rc , errmsg ) ;

  rc = ocs_mount_reply( hostReply , deviceReply, &status , errmsg , timeout ) ;
  if( dbg && ( rc < 0 ) )
      fprintf(stderr, " ocs_mount_reply : %d : %s\n" , rc , errmsg ) ;
  if( rc < 0 )return rc ;
  
  if( rc == OCS_TIMEOUT ){
    rc = ocs_mount_cancel( dev->host, dev->device, errmsg ) ;
    if( dbg && rc )
      fprintf(stderr," ocs_mount_cancel : %d : %s\n" , rc , errmsg ) ;
    if( rc )return rc ;
    /*
     * the manual says, that we have to catch the mount cancel event
     * with ocs_mount_reply. 
     */
    rc = ocs_mount_reply( hostReply , deviceReply  , &status , errmsg , 20 ) ;
    if( dbg && rc )
      fprintf(stderr, " ocs_mount_reply : %d : %s\n" , rc , errmsg ) ;
    /*
     * if it fails I don't know what to do anyway, so why don't
     * we just ignore it.
     */   
     if( rc == OCS_TIMEOUT ){
        fprintf(stderr," WARNING : No ocs_mount_reply from ocs_mount_cancel\n");
     }
     return MOCS_TIMEOUT ;
  }
  if( status ){    /* tape mount failed */
     if( dbg && status )
         fprintf(stderr," ocs_mount_reply status : %d : %s\n" , status , errmsg ) ;
     dev -> status = status ;
     return MOCS_MOUNT_FAILED ;    
  }
     
  /*
   * did we really get the mount we inistallized ?
   */
   if( strcmp( dev -> host , hostReply ) ||
       strcmp( dev -> device , deviceReply )  ){
     /*
      * I don't know why we are here now. 
      * and because we are fair : we dismount the tape
      * which is  obviously not ours.
      */
      (void)ocs_dismount( hostReply , deviceReply , "NoOptions" , errmsg ) ;
      
      return MOCS_MISMATCH ;
   }
   /*
    * I think now we got it.
    */
   return MOCS_OK ;
}
int do_the_allocate( char *hostg , char *devg , char *devt ,
                     int timeout ,  devDesc *dev             )
{

  int rc , bid , bid_reply ;
  char *errmsg ;
    
  if( ! dev ) return MOCS_ARGERR ;
  errmsg = dev -> message ;
  
  bid = ocs_queue_alloc( hostg ? hostg : "any" ,
                         devg  ? devg  : "any" ,
                         devt  ? devt  : "any" , errmsg ) ;
                         
  if( dbg && ( bid < 0 ) )
     fprintf(stderr," ocs_queue_alloc : %d %s\n" , bid , errmsg ) ;
                         
  if( bid < 0 )return bid ;
  
  rc = ocs_alloc_reply( &bid_reply , dev -> host , dev -> device ,
                        errmsg , timeout  ) ;
  if( dbg && rc )
     fprintf(stderr," ocs_alloc_reply : %d %s\n" , rc , errmsg ) ;

  if( rc == OCS_TIMEOUT ){
     /*  remark :
      *  there is a small time window between 
      *  a timeout from ocs_alloc_reply and ocs_alloc_cancel
      *  where the request could have been handled.
      *  if this happens the manual says, that the 
      *  ocs_alloc_cancel would not report an error.
      *  to be sure not to run into that trap,
      *  we have to check up with ocs_alloc_reply
      *  after ocs_alloc_cancel if this small timeslot
      *  has been hit.
      */
     rc = ocs_alloc_cancel( bid , errmsg ) ;
     if( dbg && rc )
        fprintf(stderr," ocs_alloc_cancel : %d %s\n" , rc , errmsg ) ;
        
     if(rc)return rc ;
     
     rc = ocs_alloc_reply( &bid_reply , dev -> host ,
                           dev -> device , errmsg , 0  ) ;
                   
     if( dbg && rc )
        fprintf(stderr," ocs_alloc_reply : %d %s\n" , rc , errmsg ) ;
        
     if( rc == OCS_NO_ALLOCS_PENDING ){
        strcpy( errmsg , "Timeout" ) ;
        return MOCS_TIMEOUT ;
     }
     if( rc )return rc ; /* ... any type of error */
     /*
      * uuups we got it.
      */
  }
  if( bid_reply != bid ){
     /* this is a really nasty situation.
      * as a matter of fact, I don't have the slightest idea
      * what to do know.
      */
      return MOCS_MISMATCH ;
  }
  /*
   *  whatever path we were running along, theHost and theDevice
   *  now should contain the host and device.
   */
   dev -> ppid = getppid() ;
      
   return MOCS_OK ;
}
void deleteLog( devDesc *dev  )
{
  char  logFile[128] ;

  getLogFileName( dev , logFile );

  (void)unlink( logFile ) ;
  return ;
}
void devDesc2string( char *string , devDesc *dev )
{
   sprintf( string , "%s.%s.%d.%d" , 
            dev -> host ,
            dev -> device ,
            dev -> ppid  ,
            dev -> watchDog   ) ;
            
  return ;

}
void getLogFileName( devDesc *dev , char *logFile )
{
 sprintf( logFile , "%s/%s.%s" , LOG_FILE_DIR , dev -> host , dev -> device ) ;
 return ;
}
void devDesc2log( devDesc *dev )
{
  char  logFile[128] , string[128] ;
  FILE *f ;
  
   devDesc2string( string , dev  ) ;
            
   getLogFileName( dev , logFile );
   
   if( ! ( f = fopen( logFile , "w" ) ) ){   
      fprintf(stderr, "Problem writeDevLog : Can't open log file %s\n" , logFile );
      return ;
   } 
   fprintf(f,"%s\n" , string ) ;
   fclose(f);
   return ;

}
int string2devDesc( devDesc *dev , char *string )
{
  char *ptr , *x;
  
   dev -> host[0]     = '\0' ;
   dev -> device[0]   = '\0' ;
   dev -> ppid        = 0 ;
   dev -> watchDog    = 0 ;
            
   ptr = string ;        
   if( ! ( x = strchr( ptr , '.' ) ) ){
      strcpy( dev -> host , ptr ) ;
      return -1 ;
   }
   
   *x = '\0' ;
   strcpy( dev -> host , ptr ) ;
   *x = '.' ;
   
   ptr = x + 1 ;
   if( ! ( x = strchr( ptr , '.' ) ) ){
      strcpy( dev -> device , ptr ) ;
      return -1 ;
   }
   *x = '\0' ;
   strcpy( dev -> device , ptr ) ;
   *x = '.' ;
   
   ptr = x + 1 ;
   if( ! ( x = strchr( ptr , '.' ) ) ){
      sscanf( ptr , "%d" , &(dev -> ppid ) ) ;
      return -1 ;
   }
   *x = '\0' ;
   sscanf( ptr , "%d" , &(dev -> ppid ) ) ;
   *x = '.' ;
   
   sscanf( ptr , "%d" , &(dev -> watchDog ) ) ;
           
  return 0 ;
}
/*
 *  ocs devicegroup workaround.
 */
int getDeviceSpec( char *group , char *host , char *device )
{
   char grp[128] , hst[128] , dvc[128] , string[128] ;
   FILE *f ;
   
   *hst = *dvc = *grp =  '\0' ;
   
   if( ! ( f = fopen( IMPEX_CONFIG , "r" ) ) ){
      return -1 ;
   }
   while( fgets( string , 128 , f ) ){
      
      if( *string == '#' )continue ;
      sscanf( string , "%s %s %s" , grp , hst , dvc ) ;
      if( ! strcmp( grp , group ) ){
          strcpy( host , hst ) ;
          strcpy( device , dvc ) ;
          fclose( f ) ;
          return 0 ;
      }
   }
   fclose( f ) ;
   return -1 ;
}
int getDeviceSpec2( char *group , char *host , char *device ,int *flags )
{
   char grp[128] , hst[128] , dvc[128] , string[128] , fgs[128] ;
   FILE *f ;
      
   if( ! ( f = fopen( IMPEX_CONFIG , "r" ) ) ){
      return -1 ;
   }
   while( fgets( string , 128 , f ) ){
      
      if( *string == '#' )continue ;
      *hst = *dvc = *grp =  *fgs = '\0' ;
      sscanf( string , "%s %s %s %s" , grp , hst , dvc , fgs ) ;
      if( ! strcmp( grp , group ) ){
          if(host)strcpy( host , hst ) ;
          if(device)strcpy( device , dvc ) ;
          if(flags){
              *flags = 0 ;
              if( strstr( fgs , "notready" ) )*flags |= MF_NO_READY_WAIT ;
          
          }
          fclose( f ) ;
          return 0 ;
      }
   }
   fclose( f ) ;
   return -1 ;
}

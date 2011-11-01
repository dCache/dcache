// $Id: CleanerV2.java,v 1.23 2007-05-24 13:51:12 tigran Exp $
//
package diskCacheV111.cells ;

import  diskCacheV111.vehicles.*;
import  diskCacheV111.util.*;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;

import  java.io.* ;
import  java.util.*;
import  java.text.*;
import  java.util.zip.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  */


public class CleanerV2 extends CellAdapter implements Runnable {

    private final static Logger _log =
        LoggerFactory.getLogger(CleanerV2.class);

    private final String _cellName     ;
    private final CellNucleus _nucleus ;
    private final Args   _args         ;
    private final String _dirName       ;
    private final File   _trashLocation ;
    private final File   _dbDirectory   ;
    private final File   _archive       ;
    private final File   _currentDir    ;
    private final File   _globalLock    ;
    private final File   _currentRemoveList  ;
    private boolean _nullBug      = false ;

    private Thread _cleaningThread    = null;
    private long   _refreshInterval   = 5*60*1000 ;
    private long   _replyTimeout      = 10000 ;
    private int    _removeCounter     = 0 ;
    private int    _unremoveCounter   = 0 ;
    private int    _lastTimeRemoveCounter   = 0 ;
    private int    _lastTimeUnremoveCounter = 0 ;
    private long   _recoverTimer       = 0L ; // 2 hours
    private int    _exceptionCounter   = 0 ;
    private int    _loopCounter        = 0 ;
    private final Date   _started            = new Date() ;
    private final Object _sleepLock          = new Object() ;
    private long   _previousRecoverRun = System.currentTimeMillis() ;
    private String _broadcastCellName  = null ;
    private File   _moveAwayLocation   = null ;

    private boolean _usePnfsManager     = true ;
    private boolean _cleanUpAfterwards  = false ;
    private String  _pnfsManagerName    = "PnfsManager" ;
    private long    _pnfsManagerTimeout = 60000L ;

    private int     _processInOnce      = 100 ;
    private Thread  _workerThread       = null ;

    private boolean _useZip = false ;
    private boolean _useLog = false ;

    //
    // error counters
    //
    private int _summaryErrorCounters = 0 ;

    private final static SimpleDateFormat _formatter = new SimpleDateFormat ("yyyy.MM.dd");

    private static class DuplicatedOutputStream extends OutputStream {
       private OutputStream _out1 , _out2 ;
       private DuplicatedOutputStream( OutputStream out1 , OutputStream out2 ){
          _out1 = out1 ;
          _out2 = out2 ;
       }
       @Override
    public void close() {
          try{ if( _out2 != null )_out2.close() ; }catch(IOException ee){}
          try{ if( _out1 != null )_out1.close() ; }catch(IOException ee){}
       }

       @Override
    public void flush() throws IOException {
          if( _out2 != null )_out2.flush() ;
          if( _out1 != null )_out1.flush() ;
       }
       @Override
    public void write( int n ) throws IOException {
          if( _out1 != null )_out1.write(n) ;
          if( _out2 != null )_out2.write(n) ;
       }
       @Override
    public void write( byte [] n ) throws IOException {
          if( _out1 != null )_out1.write(n) ;
          if( _out2 != null )_out2.write(n) ;
       }
       @Override
    public void write( byte [] n , int offset , int len ) throws IOException {
         if( _out1 != null )_out1.write(n,offset,len) ;
         if( _out2 != null )_out2.write(n,offset,len) ;
       }
    }

    public CleanerV2( String cellName , String args ) throws Exception {

	super( cellName , CleanerV2.class.getName(), args , false ) ;

	_cellName = cellName ;
	_args     = getArgs() ;
	_nucleus  = getNucleus() ;

	useInterpreter( true ) ;

	try {

            //  Usage : ... [-trash=<trashLocationDirectory>]
            //              [-refresh=<refreshInterval_in_sec.>]
            //              [-poolTimeout=<poolTimeout_in_sec.>]
            //              [-processFilesPerRun=<filesPerRun]  default : 100
            //              [-db=<database directory.>]
            //              [-recover=<timer/Min>]
            //              [-reportRemove=<cellName>]
            //              [-usePnfsManager=[<pnfsManagerName>] | off | on ] default : PnfsManager
            //              [-archive=log|zip|none  default : none
            //              [-moveAwayLocation=<moveAwayLocation>] default : remove
            //              [-nullBug] # debug only
            //
            // find the trash. If not specified do some filesystem
            // inspections.
            //
            String refreshString = _args.getOpt("refresh") ;
            if( refreshString != null ){
               try{
                   _refreshInterval = Long.parseLong( refreshString )*1000L ;
               }catch(NumberFormatException ee ){
                   // bad values ignored
               }
            }
            _log.info("Refresh Interval set to "+_refreshInterval+" milli seconds");

            refreshString = _args.getOpt("recover") ;
            if( refreshString != null ){
               try{
                   _recoverTimer = Long.parseLong( refreshString )*60000L ;
               }catch(NumberFormatException ee ){
                   // bad values ignored
               }
            }
            _log.info("Recover Interval set to "+(_recoverTimer/60000L)+" minutes");

            refreshString = _args.getOpt("poolTimeout") ;
            if( refreshString != null ){
               try{
                   _replyTimeout = Long.parseLong( refreshString )*1000L ;
               }catch(NumberFormatException ee ){
                   // bad values ignored
               }
            }
            _log.info("Pool Timeout set to "+(_replyTimeout/1000L)+" seconds");

            String dirName = null;
            if( ( ( dirName = _args.getOpt("trash") ) == null ) ||
                ( dirName.equals("")                          )    ){
               _log.info("'trash' not defined. Starting autodetect" ) ;
               dirName = autodetectTrash() ;
            }
            _dirName = dirName;
            _log.info("'trash' set to "+_dirName) ;
	    _trashLocation = new File(_dirName);

	    if( ! _trashLocation.isDirectory() ){
                _log.info("'trash' not a directory : "+_dirName ) ;
		throw new
                IllegalArgumentException("'trash' not a directory : "+_dirName ) ;
            }

            String db = _args.getOpt("db") ;
            if( db == null )
              throw new
              IllegalArgumentException("Database Directory (db) not specified");

            _dbDirectory = new File(db) ;
            if( ! _dbDirectory.isDirectory() )
               throw new
                IllegalArgumentException("Not a directory : "+db ) ;

            _log.info("Database directory : "+_dbDirectory);

            String moveAway = _args.getOpt("moveAwayLocation") ;
            if( ( moveAway != null ) &&  ! moveAway.equals("") ){
               _moveAwayLocation = new File( moveAway ) ;
               if( ! _moveAwayLocation.isDirectory()  ){
                   _log.warn("moveAwayLocation is not a directory, switching to remove mode");
                   _moveAwayLocation = null ;
               }
            }
            _log.info("moveAwayLocation : "+( _moveAwayLocation == null  ? "VOID" : _moveAwayLocation.toString() ) ) ;

            _broadcastCellName = ( ( _broadcastCellName = _args.getOpt("reportRemove" ) ) == null ) ||
                                 _broadcastCellName.equals("") ||
                                 _broadcastCellName.equals("none") ?
                                 null : _broadcastCellName ;

            _log.info(_broadcastCellName == null ?
                "Remove report disabled" :
                "Remove report sent to : "+ _broadcastCellName ) ;

            _currentDir = new File( _dbDirectory , "current") ;
            _currentDir.mkdir() ;
            if( ! _currentDir.isDirectory() )
              throw new
              IOException("Can't create current : "+_currentDir ) ;

            _archive = new File( _dbDirectory , "archive" ) ;
            _archive.mkdir() ;
            if( ! _archive.isDirectory() )
               throw new
               IOException("Can't create archive : "+_archive ) ;

            _currentRemoveList = new File( _currentDir , "removeList" ) ;
	    _globalLock = new File( _currentDir , "GlobalLock" ) ;
            _globalLock.deleteOnExit() ;

            _nullBug = _args.hasOption("nullBug") ;

            String tmp = _args.getOpt("usePnfsManager") ;
            if( ( tmp != null ) && ( tmp.length() > 0 ) ){
               if( tmp.equalsIgnoreCase("off")|| tmp.equalsIgnoreCase("no") ){
                  _usePnfsManager  = false ;
               }else if( !tmp.equalsIgnoreCase("on") && !tmp.equalsIgnoreCase("yes") ){
                  _pnfsManagerName = tmp ;
               }
            }
            tmp = _args.getOpt("pnfsManagerTimeout") ;
            if( tmp != null ){
               try{
                  _pnfsManagerTimeout = Long.parseLong(tmp) * 1000L ;
               }catch(NumberFormatException ee ){
                  _log.warn("Problem with argument of pnfsManagerTimeout : "+tmp );
               }
            }
            _log.info("Using PnfsManager for cacheInfo : "+_usePnfsManager ) ;
            if( _usePnfsManager ){
               _log.info( "Using PnfsManager Name    : "+_pnfsManagerName ) ;
               _log.info( "Using PnfsManager Timeout : "+_pnfsManagerTimeout);
            }
	    tmp = _args.getOpt("processFilesPerRun");
            if( tmp != null )try{ _processInOnce = Integer.parseInt(tmp) ; }catch(NumberFormatException ee ){}

            _log.info("Processing "+_processInOnce+" files per run");

            if( ( tmp = _args.getOpt("archive") ) != null ){

                if( tmp.equals("zip") ){ _useZip = true ; _useLog = false ; }
                else if( tmp.equals("log") ){ _useLog = true ; _useZip = false ; }
                else { _useLog = false ; _useZip = false ; }

            }
            _log.info("Archive method : "+( _useZip ? "zip" : _useLog ? "log" : "none" ) );

            ( _workerThread = _nucleus.newThread( this , "Worker" ) ).start() ;

	} catch (Exception e){
	    _log.warn ("Exception occurred while running cleaner constructor : "+e, e);
	    start();
	    kill();
	    throw e;
	}

	start() ;
    }

    @Override
    public void getInfo( PrintWriter pw ){
	pw.println("Cleaner (V2)");
	pw.println("  Version           : $Id: CleanerV2.java,v 1.23 2007-05-24 13:51:12 tigran Exp $" ) ;
        pw.println("  Db Directory      : " + _dbDirectory ) ;
	pw.println("  Trash Location    : " + _dirName ) ;
	pw.println("  Chain Location    : " + (_moveAwayLocation==null?"VOID":_moveAwayLocation.toString()) ) ;
        pw.println("  Archive method    : " + ( _useZip ? "zip" : _useLog ? "log" : "none" ) );
	pw.println("  Refresh interval  : " + _refreshInterval);
	pw.println("  ReplyTimeout (ms) : " + _replyTimeout);
        pw.println("  Report Remove to  : " + (_broadcastCellName==null?"none":_broadcastCellName));
	pw.println("  Started           : " + _started);
	pw.println("  Loops             : " + _loopCounter);
	pw.println("  Files removed     : " + _removeCounter+" / "+_unremoveCounter);
        pw.println("  Process per r-run : " + _processInOnce+" / "+_summaryErrorCounters ) ;
        pw.println("  Global Lock       : " + ( _globalLock.exists() ? "set" : "released") ) ;
        pw.println("  Recover Timer     : " + ( _recoverTimer == 0L  ? "DISABLED" : ( _recoverTimer/1000L/60L) + " Min" )  ) ;
        if( _recoverTimer > 0L ){
           pw.println("  Next Recover Run  : "+(( _recoverTimer - (System.currentTimeMillis() - _previousRecoverRun ))/60000L)+" Minutes");
        }
        pw.println("  Use Pnfs Manager  : " + _usePnfsManager ) ;
        if( _usePnfsManager ){
            pw.println("  PnfsManager Name     : " + _pnfsManagerName ) ;
            pw.println("  PnfsManager Timeout  : " + _pnfsManagerTimeout ) ;
        }
//	pw.println("  Files removed (l) : " + _lastTimeRemoveCounter+
//                   " / "+_lastTimeUnremoveCounter);
//	pw.println("  Exception         : " + _exceptionCounter);
    }
    @Override
    public String toString(){
       return "Removed="+_removeCounter+";X="+_exceptionCounter ;
    }
    private String autodetectTrash() throws Exception {
       File pnfsSetup = new File( "/usr/etc/pnfsSetup" ) ;
       if( ! pnfsSetup.exists() )
          throw new Exception( "Not a pnfsServer" ) ;
       if( ! pnfsSetup.canRead() )
          throw new Exception( "Can't read pnfsSetup file" ) ;

       BufferedReader br = new BufferedReader(
                              new FileReader( pnfsSetup ) ) ;
       String line = null ;
       try{
           while( ( line = br.readLine() ) != null ){
              StringTokenizer st = new StringTokenizer(line,"=") ;
              try{
                 String key   = st.nextToken() ;
                 String value = st.nextToken() ;
                 if( key.equals("trash") && ( value.length() > 0 ) )
                    return value+"/2" ;
              }catch(Exception ee ){
              }
           }
       }catch(EOFException eof ){

       }finally{
           try{ br.close() ; }catch(Exception eeee ){}
       }
       throw new
       Exception( "'trash' not found in pnfsSetup" ) ;
    }
    //
    //  clearTrash :
    //      removes all entries of currentRemoveList from
    //      trash/2
    //
    private void clearTrash() throws IOException {

       BufferedReader br = new BufferedReader( new FileReader( _currentRemoveList ) ) ;

       String line = null ;
       try{
          while( true ){
             try{
                 if( ( line = br.readLine() ) == null )break ;
             }catch( IOException ioe ){
                 _log.warn("Io Exception at : "+line+" : "+ioe);
                 break ;
             }
             try{
                StringTokenizer st = new StringTokenizer( line ) ;
                String pnfsId = st.nextToken() ;

                if( _moveAwayLocation != null ){
                    boolean success = new File( _trashLocation , pnfsId ).renameTo( new File( _moveAwayLocation , pnfsId ) ) ;
                    _log.info("Moving : "+pnfsId +" "+ success );
                }else{
                    boolean success = new File( _trashLocation , pnfsId ).delete() ;
                    _log.info("Removing : "+pnfsId +" "+ success );
                }

                //
                // we don't necessarily need to clear all cache locations. Pools will do that.
                //
                if( _usePnfsManager && _cleanUpAfterwards)clearAllCacheLocations( new PnfsId(pnfsId) ) ;
             }catch(Exception ee){
                _log.warn("clearTrash : Problem "+line+" : "+ee );
             }
          }
       }finally{
          try{ br.close() ; }catch(Exception eee ){}
       }
    }
    private void informBroadcaster(){

       String broadcast = _broadcastCellName  ;
       if( broadcast == null )return ;

       List<String> list = new ArrayList<String>() ;

       BufferedReader br = null;
       try{
           br = new BufferedReader( new FileReader( _currentRemoveList ) ) ;

           String    line = null ;

           try{
              while( true ){
                 try{
                     if( ( line = br.readLine() ) == null )break ;
                 }catch( IOException ioe ){
                     _log.warn("Io Exception at : "+line+" : "+ioe);
                     break ;
                 }
                 try{
                    StringTokenizer st = new StringTokenizer( line ) ;
                    list.add( st.nextToken() ) ;
                 }catch(Exception ee){
                    _log.warn("clearTrash : Problem "+line+" : "+ee );
                 }
              }
           }finally{
              if ( br != null ) try{ br.close() ; }catch(IOException eee ){}
           }

       }catch(IOException ioe ){
           _log.warn("I/O problems with "+_currentRemoveList+" : "+ioe ) ;
           return ;
       }
       if( list.size() == 0 )return ;

       String [] fileList = list.toArray( new String[list.size()] ) ;

       PoolRemoveFilesMessage msg = new PoolRemoveFilesMessage(broadcast) ;
       msg.setFiles( fileList ) ;

       /*
        * no rely required
        */
       msg.setReplyRequired(false);

       try{
           sendMessage( new CellMessage( new CellPath(broadcast) , msg )  ) ;
       }catch(Exception ee ){
           _log.warn("Problems sending 'remove files' message to "+broadcast+" : "+ee.getMessage(), ee);
       }
    }
    public String hh_set_moveawaylocation = "<directory to move processed pnfsid files> | void " ;
    public String ac_set_moveawaylocation_$_1( Args args ){

       String loc =  args.argv(0) ;
       if( loc.equals("") || loc.equals("void") ){
          _moveAwayLocation = null ;
          return "Move Away feature disabled" ;
       }else{
          File f = new File( loc ) ;
          if( ! f.isDirectory() )
             throw new
             IllegalArgumentException("Not a directory : "+f);
          _moveAwayLocation = f;
          return "New move away location : "+f ;
       }

    }
    public String ac_scantrash( Args args )throws Exception {

        createSummary() ;
        return "" ;
    }
    public String ac_cleartrash( Args args )throws Exception {

        clearTrash() ;
        return "" ;
    }
    public String ac_split( Args args )throws Exception {

        split() ;
        return "" ;
    }
    public String ac_rundelete( Args args )throws Exception {

        runDelete(getPoolFiles()) ;
        return "" ;
    }
    public String hh_lock = "get|check|release" ;
    public String ac_lock_$_1( Args args )throws Exception {
       String command = args.argv(0) ;
       if( command.equals( "get") ){

          getGlobalLock() ;

          return "Lock granted" ;

       }else if( command.equals( "release") ){
          releaseGlobalLock() ;
          return "Lock released";
       }else if( command.equals("check") ){
          return "Lock "+( _globalLock.exists() ? "set" : "not set");
       }else{
          throw new
          CommandSyntaxException("Syntax Error");
       }
    }
    public String hh_set_refresh = "<refreshTimeInSeconds> # > 5 [-wakeup]" ;
    public String ac_set_refresh_$_0_1( Args args ){

       if( args.argc() > 0 ){
          long newRefresh = Long.parseLong(args.argv(0)) * 1000L;
          if( newRefresh < 5000L )
             throw new
             IllegalArgumentException("Time must be greater than 5");

          _refreshInterval = newRefresh ;
       }
       if( args.hasOption("wakeup") ){
          synchronized( _sleepLock ){
              _sleepLock.notifyAll() ;
          }
       }
       return "Refresh set to "+(_refreshInterval/1000L)+" seconds" ;
    }
    public String hh_set_recover = "<refreshTimeInMINUTES>" ;
    public String ac_set_recover_$_1( Args args ){

      long newRefresh = Long.parseLong(args.argv(0)) * 1000L * 60L ;
          if( newRefresh < 0L )
             throw new
             IllegalArgumentException("Time must be greater >= 0");

      _recoverTimer = newRefresh ;

      if( _recoverTimer > 0 )
         return "Recover Timer set to "+(_recoverTimer/60000L)+" minutes" ;
      else
         return "Recover disabled" ;
    }
    public String hh_reactivate = "<poolName> | -a" ;
    public String ac_reactivate_$_0_1( Args args ) throws Exception {
        boolean all = args.hasOption("a") ;
        if( ( ( ! all ) && ( args.argc() < 1 ) ) ||
            ( ( all )   && ( args.argc() > 0 ) )    )
             throw new
             CommandSyntaxException("Not enough or inconsistent arguments" ) ;

        if( all ) {


             getGlobalLock() ;
            try{

               reactivate( ) ;

            }finally{
               releaseGlobalLock() ;
            }

        }else{
            String poolName = args.argv(0);
            File failedFile = new File( _currentDir , "failed."+poolName ) ;
            if( ! failedFile.exists()  )
                 throw new
                 IllegalArgumentException("Failed pool file not found for "+poolName);

            getGlobalLock() ;
            try{

               reactivate( failedFile ) ;

            }finally{
               releaseGlobalLock() ;
            }
        }
        return "" ;
    }
    public String hh_scan = "[-init] [-force]" ;
    public String ac_scan( Args args )throws Exception {

        if( !args.hasOption("force") )getGlobalLock() ;

        try{

           runNextCheck( args.hasOption("init") ) ;

        }finally{
           releaseGlobalLock() ;
        }
        return "" ;
    }
    @Override
    public void cleanUp(){

        _log.warn("Clean up called") ;
        if( _workerThread != null )_workerThread.interrupt() ;

    }
    public void run(){

       boolean moreFilesToProcess = false ; // DON'T start immediately

       for( int i = 0 ; ! Thread.interrupted() ; i++ ){

          _loopCounter ++ ;

          if( ! moreFilesToProcess ){
              try{

                 synchronized( _sleepLock ){
                     _sleepLock.wait( _refreshInterval ) ;
                 }

              }catch( InterruptedException ie ){
                  _log.warn("Worker thread interrupted");
                  break ;
              }
          }
          try{
             getGlobalLock() ;
          }catch(Exception ee ){
             _log.warn("Problem in getting global lock : "+ee.getMessage() ) ;
             continue ;
          }
          try{

             if( ( _recoverTimer > 0 ) &&
                 ( ( System.currentTimeMillis() - _previousRecoverRun ) > _recoverTimer ) ){

                 reactivate() ;

                 _previousRecoverRun = System.currentTimeMillis() ;

             }

             moreFilesToProcess = runNextCheck( true ) ;

          }catch(Exception ee ){
             _log.warn("runNextCheck : "+ee ) ;
             ee.printStackTrace();
          }finally{
             releaseGlobalLock() ;
          }
       }

    }
    //
    //   runDelete :
    //     scans current directory for pool.<poolName> files
    //     and processes them one by one.
    //
    //   per pool file :
    //       1) reads file and sends remove of all
    //          pnfsid to pool.
    //       2) If pool is available, it returns
    //          ok or a list of pnfsids which couldn't
    //          be removed (locked, or [lsf=none and precious])
    //       3) 'non removed' files are added to
    //          current/failed.<poolName> list.
    //       4) current/pool.<poolName> is removed.
    //
    private void runDelete( String [] poolFileList ){

        if( poolFileList == null ) return;

       for( int i = 0 ; ( i < poolFileList.length ) &&
                        ! Thread.interrupted() ; i++ ){

          String thisFile = poolFileList[i] ;

          _log.info("runDelete : processing : "+thisFile);

          String thisPool = getPoolName( thisFile ) ;
          if( thisPool == null )continue ;
          BufferedReader br = null ;
          File poolFile     = new File(_currentDir,thisFile) ;
          List<String>    list = new ArrayList<String>() ;
          String       line = null ;
          try{
             br = new BufferedReader(new FileReader(poolFile));
          }catch(FileNotFoundException ee ){
             _log.warn("Can't open : "+thisFile ) ;
             continue ;
          }
          try{
             while( true ){
                try{
                    if( ( line = br.readLine() ) == null )break ;
                }catch(Exception ee ){
                    _log.warn("Problem in reading : "+thisFile+" : "+ee ) ;
                    break ;
                }
                list.add(line.trim());
             }
          }finally{
             if (br != null) try{ br.close() ; }catch(IOException eee ){}
          }
          _log.info("runDelete : list for "+thisPool+" : "+list );
          _removeCounter += list.size() ;
          String [] notRemoved = sendRemoveToPool( thisPool , list ) ;
          if( ( notRemoved != null ) && ( notRemoved.length > 0 ) ){
             _unremoveCounter += notRemoved.length ;
             _log.info("runDelete : sendRemoveToPool returned "+thisPool+" : "+notRemoved.length );
             File removeFile = new File( _currentDir , "failed."+thisPool);
             PrintWriter rpw = null ;
             try{
                rpw = new PrintWriter(
                         new FileWriter( removeFile.getAbsolutePath() , true ) ) ;
                try{
                   for( int j = 0 ; j < notRemoved.length ; j++ ){
                      rpw.println(notRemoved[j].trim());
                   }
                   rpw.flush() ;
                }finally{
                   if(rpw != null) rpw.close() ;
                }
             }catch(Exception eee ){
                _log.warn("Can't create removefile : "+removeFile+" : "+eee);
             }
          }
          poolFile.delete();

       }

    }
    private static final int CREATE_SUMMARY = 0 ;
    private static final int CLEAR_TRASH    = 1 ;
    private static final int SPLIT          = 2 ;
    private static final int RUN_DELETE     = 3 ;

    private boolean runNextCheck( boolean init ) throws Exception {

       // _log.info("runNextCheck with "+init);

       int state = CREATE_SUMMARY ;

       String [] poolFileList = null ;

       if( init ){
          poolFileList = getPoolFiles() ;

          if( ( poolFileList != null ) && ( poolFileList.length > 0 ) ){
             //
             // check if there are remainding pool files.
             //
             state = RUN_DELETE ;
             _log.info("runNextCheck : found pool files (->RUN_DELETE)") ;

          }else if( _currentRemoveList.exists() ){
             //
             // check if the removeList is still there
             //
             state = CLEAR_TRASH ;
             _log.info("runNextCheck : found remove list (->CLEAR_TRASH)") ;
          }else{
             state = CREATE_SUMMARY ;
             //_log.info("runNextCheck : clean system found (->CREATE_SUMMARY)") ;
          }
       }
       switch( state ){
          case CREATE_SUMMARY :

             int count = createSummary() ;
             if( count > 0 ){
                 _log.info("runNextCheck: createSummary collected "+count+" files");
             }else{
                 return false ;
             }

             _log.info("runNextCheck: informing Broadcaster");
             informBroadcaster() ;

          case CLEAR_TRASH :

             _log.info("runNextCheck: clearing Trash");
             clearTrash() ;

          case SPLIT :

             _log.info("runNextCheck: splitting summary");
             split() ;
             poolFileList = getPoolFiles() ;
             _log.info("runNextCheck: got poollist : "+(poolFileList==null?"NULL":(""+poolFileList.length ) ) );
          case RUN_DELETE :

             _log.info("runNextCheck: runningDelete");
             runDelete( poolFileList ) ;

       }
       return true ;
    }
    private String []  getPoolFiles(){
        return  _currentDir.list(
          new FilenameFilter(){
            public boolean accept( File dir , String name ){
               return name.startsWith("pool.") ;
            }
          }
        ) ;
    }
    private String []  getFailedFiles(){
        return  _currentDir.list(
          new FilenameFilter(){
            public boolean accept( File dir , String name ){
               return name.startsWith("failed.") ;
            }
          }
        ) ;
    }
    private String getPoolName( String filename ) {
          int pos = filename.indexOf('.');
          if( ( pos < 0 ) || ( ( pos + 1 ) == filename.length() ) )
             return null ;

          return filename.substring(pos+1);

    }
    private void reactivate() throws Exception {
        String [] failedFiles = getFailedFiles() ;
        for( int i = 0 ; i < failedFiles.length ; i++ ){
            try{
                _log.info("Reactivating : "+failedFiles[i]);
                reactivate( new File( _currentDir , failedFiles[i] ) ) ;
            }catch(Exception e ){
                _log.warn("Problem reactivating : "+failedFiles[i]+" : "+e) ;
            }
        }

    }
    private void reactivate( File failedFile )throws Exception {

        File parentFile = failedFile.getParentFile() ;
        String poolName = getPoolName( failedFile.getName());
        File tmpFailed  = new File( parentFile , "$failed."+poolName ) ;
        File tmpRetry   = new File( parentFile , "$retry."+poolName) ;

        PrintWriter failed = null ;
        PrintWriter retry  = null ;

        BufferedReader br  = new BufferedReader( new FileReader( failedFile )  ) ;

        int remainingFiles = 0 ;

        try{
            //
            // copy the first 100 into $retry.<pool>
            //
            retry = new PrintWriter( new FileWriter( tmpRetry ) ) ;
            try{
                 String line = null ;
                 Set<String> set = new HashSet<String>(100) ;
                 while( ( ( line = br.readLine() ) != null ) && ( set.size() < 100 ) ){
                     String pnfsid = line.trim() ;
                     new PnfsId( pnfsid ); // check format
                     set.add(pnfsid);
                 }
                 for( Iterator i = set.iterator() ; i.hasNext() ; ){
                     retry.println( i.next().toString() ) ;
                 }
            }finally{
                try{ retry.close()  ; }catch(Exception d4 ){}
            }
            //
            // copy the rest to $failed.<poolname>
            //
            failed = new PrintWriter( new FileWriter( tmpFailed ) ) ;

            try{
               String line = null ;
               while( ( line = br.readLine() ) != null ){
                   failed.println(line);
                   remainingFiles++ ;
               }
            }finally{
                try{ failed.close()  ; }catch(Exception d4 ){}
            }
        }catch(Exception rm ){
            tmpRetry.delete() ;
            tmpFailed.delete() ;
        }finally{
            try{ br.close()  ; }catch(Exception d4 ){}
        }
        failedFile.delete() ;

        if( remainingFiles > 0 )tmpFailed.renameTo( failedFile ) ;
        else tmpFailed.delete() ;

        tmpRetry.renameTo( new File( parentFile , "pool."+poolName) ) ;

    }
    private String [] sendRemoveToPool( String poolName , List<String> removeList ){

       PoolRemoveFilesMessage msg = new PoolRemoveFilesMessage(poolName);
       String [] pnfsList = removeList.toArray(new String[removeList.size()]);
       if( _nullBug ){
           for( Iterator i = removeList.iterator()  ; i.hasNext() ; )
               if( i.next() == null )_log.warn("sendRemoveToPool : nullBug : null in original list" ) ;
           for( int j = 0 , n = pnfsList.length ; j < n ; j++ )
               if( pnfsList[j] == null )_log.warn("sendRemoveToPool : nullBug : null in copied list" ) ;

       }
       msg.setFiles( pnfsList );

       CellMessage cellMessage = new CellMessage( new CellPath( poolName ) , msg ) ;

       try{
          cellMessage = sendAndWait( cellMessage , _replyTimeout ) ;
       }catch(Exception ee ){
          return pnfsList ;
       }
       if(  cellMessage == null ){
           _log.warn("sendRemoveToPool : remove message to "+poolName+" timed out");
           return pnfsList ;
       }

       Object reply = cellMessage.getMessageObject() ;

       if(  reply == null ){
           _log.warn("sendRemoveToPool : reply message from "+poolName+" didn't contain messageObject");
           return pnfsList ;
       }

       if( ! ( reply instanceof PoolRemoveFilesMessage ) ){
          _log.warn("sendRemoveToPool : got unexpected reply class : "+reply.getClass().getName());
          return pnfsList ;
       }
       //
       // if return code is ok. we assume that all files have been
       // deleted from the pool.
       //
       PoolRemoveFilesMessage prfm = (PoolRemoveFilesMessage)reply ;
       if( prfm.getReturnCode() == 0 ){
           _log.info("sendRemoveToPool : all files removed from "+poolName);
           return null ;
       }
       Object o = prfm.getErrorObject() ;
       //
       // if return code is not ok, the error object should
       // either contain some exception, or it should contain
       // a list of files which coudn't be removed, so that  we
       // try again later.
       //
       if( o instanceof String [] ){
          String [] notRemoved = (String [])o;
          _log.warn("sendRemoveToPool : "+notRemoved.length+" files couldn't be removed from "+poolName);
          return notRemoved ;
       }else if( o == null ){
          _log.warn("sendRemoveToPool : reply from "+poolName+" [null]");
          return pnfsList ;
       }else{
          _log.warn("sendRemoveToPool : reply from "+poolName+" ["+o.getClass().getName()+"]="+o.toString());
          return pnfsList ;
       }

    }
    //
    //     split :
    //          current/removeList
    //                ->     pool.<poolName>
    //                ......
    //   Format :
    //          <pnfsId1>
    //          <pnfsid2>
    //             ...
    //
    private void split() throws IOException {
       BufferedReader br = new BufferedReader( new FileReader( _currentRemoveList ) ) ;
       Map<String, File> hash = new HashMap<String, File>() ;

       try{
          while( true ){
             String        line = br.readLine() ;
             if( line == null )break ;
             StringTokenizer st = new StringTokenizer( line ) ;
             String      pnfsId = st.nextToken() ;
             while( st.hasMoreTokens() ){
                String pool = st.nextToken() ;
                File   f    = hash.get(pool) ;

                if( f == null ){
                   hash.put( pool , f = new File( _currentDir , "pool."+pool ) ) ;
                   f.delete() ;
                }
                PrintWriter pw = null ;
                try{
                   pw = new PrintWriter( new FileWriter(f.getAbsolutePath(),true) ) ;
                }catch(IOException ioe ){
                   _log.warn("Couldn't write to : "+f.getAbsolutePath()+" : "+ioe);
                   continue ;
                }
                pw.println(pnfsId);
                pw.flush() ;
                pw.close();
             }

          }
       }finally{
          try{ br.close() ; }catch(Exception ee){}
          _currentRemoveList.delete() ;
       }
    }
    //
    //   createSummary :
    //
    //      <trashDir>/2
    //          ->          archive/$remove.<time>.gz
    //          ->          current/$removeList
    //
    //    Format :
    //               <pnfsId>  pool1 [pool2 [pool3 ...]]
    //
    //   i) does NOT remove trash files
    //  ii) if createSummary failes : output files removed
    //      if not : output files renamed to
    //               archive/$removeList -> archive/removeList
    //               current/$removes.<time>.gz current/remove.<time>.gz
    //
    private int createSummary() throws Exception {
       OutputStream out1 = null , out2 = null ;

       String zipFileName = "removes."+System.currentTimeMillis()+".gz" ;
       File zipfile    = new File( _archive    , "$"+zipFileName ) ;
       File removeList = new File( _currentDir , "$removeList" ) ;
       File logFile    = new File( _archive , _formatter.format( new Date() ) ) ;
       int     count   = 0 ;
       try{

          out1 = new FileOutputStream(removeList) ;

          try{
             if( _useZip )out2 = new GZIPOutputStream( new FileOutputStream( zipfile ) ) ;
             else if( _useLog )out2 = new FileOutputStream( logFile , true ) ;
          }catch(Exception iozip ){
             try{ out1.close() ; }catch(Exception ee){}
             throw iozip ;
          }
          PrintWriter pw = null ;
          try{
             pw = new PrintWriter(
                     new OutputStreamWriter(
                        new DuplicatedOutputStream( out1 , out2 ) ) ) ;

             count = createSummary( pw ) ;

          }catch(Exception ioe ){
             throw ioe ;
          }finally{
             try{ pw.close() ; }catch(Exception ee ){}
          }

       }catch( Exception fex ){
          if( _useZip )zipfile.delete() ;
          removeList.delete() ;
          throw fex ;
       }
       if( count == 0 ){
          if( _useZip )zipfile.delete() ;
          removeList.delete() ;
       }else{
          if( _useZip )zipfile.renameTo( new File( _archive    , zipFileName ) ) ;
          removeList.renameTo( _currentRemoveList ) ;
       }

       return count ;
    }
    //
    private int createSummary( PrintWriter pw  ){
       //
       // get some files from the trash directory and
       // build the linked lists.
       //
       String [] fileList = _trashLocation.list(
          //
          // should prevent running out of memory (100 files max)
          // we check syntax by using new PnfsId.
          //
          new FilenameFilter(){
            int counter = 0 ;
            public boolean accept( File dir , String name ){
               if( counter > _processInOnce  )return false ;
               try{ new PnfsId(name) ; }catch(Exception e){return false ;}
               counter ++ ;
               return true ;
            }
          }
       ) ;
       //
       // make sure the file had some time to be closed.
       //
       try{ Thread.sleep(2000L) ; }catch(InterruptedException ee ){}
       //
       int validCounter = 0 ;
       int errorCounter = 0 ;
       for( int i = 0; i <  fileList.length; i++) {


           _log.info( "Processing "+fileList[i]);

           PnfsId pnfsId     = null ;
           try{
              pnfsId = new PnfsId(fileList[i]) ;
           }catch(Exception pnfse){
               _log.warn( "Error in syntax of pnfsId name "+fileList[i], pnfse) ;
              continue ;
           }
           try{
              Set<String> list = new HashSet<String>() ;


              if( _usePnfsManager )getCacheInfoFromPnfsManager( pnfsId , list ) ;

              pw.print(pnfsId.toString()) ;
              for( String location: list ){
                pw.print(" ");
                pw.print(location);
              }
              pw.println("");

              validCounter ++ ;

           }catch(Exception ee ){
              _log.warn("Not removed : "+pnfsId+" couldn't get cacheinfo due to "+ee.getMessage());
              errorCounter ++ ;
           }

        }
        _summaryErrorCounters = errorCounter ;
        return validCounter ;
   }
   private void clearAllCacheLocations( PnfsId pnfsId ){

       PnfsClearCacheLocationMessage pnfs = new PnfsClearCacheLocationMessage( pnfsId , "*" ) ;

       CellMessage message = new CellMessage( new CellPath( _pnfsManagerName ) , pnfs ) ;

       try{
           sendMessage( message ) ;
       }catch(Exception ee ){
           _log.warn( "Ignoring error in sending clearCacheLocation : "+ee ) ;
       }

   }
   private void getCacheInfoFromPnfsManager( PnfsId pnfsId , Set<String> externalList ) throws Exception {

       PnfsGetCacheLocationsMessage pnfs = new PnfsGetCacheLocationsMessage( pnfsId ) ;

       CellMessage message = new CellMessage( new CellPath( _pnfsManagerName ) , pnfs ) ;

       message = sendAndWait( message , _pnfsManagerTimeout ) ;

       if( message == null )
         throw new
         Exception("PnfsManager request timed out for "+pnfsId);

       Object obj = message.getMessageObject() ;
       if( ! ( obj instanceof PnfsGetCacheLocationsMessage ) ){
          //
          // might be a NoRouteToCellException
          //
          if( obj instanceof Exception )throw (Exception)obj ;

          throw new
          Exception("Got unexpected reply from PnfsManager "+
                    obj.getClass().getName()+
                    " instead of PnfsGetCacheLocationsMessage");
       }
       pnfs = (PnfsGetCacheLocationsMessage)obj ;
       if( pnfs == null )
          throw new
          Exception("No reply from PnfsManager for "+pnfsId);


        if( pnfs.getReturnCode() != 0 ){
          Object error = pnfs.getErrorObject() ;
          _log.warn("Got error from PnfsManager for "+pnfsId+" ["+pnfs.getReturnCode()+"] "+
               (error==null?"":error.toString()));
          //
          // this could be 'file no longer in pnfs from pnfsManagerV2 which
          // is of course not an error.
          //
          return ;
       }
       List<String> locations = pnfs.getCacheLocations() ;
       if( locations == null ){
           _log.warn("getCacheInfoFromPnfsManager : PnfsManager replied with 'null' getCacheInfo answer for "+pnfsId);
           return ;
       }
       _log.info("getCacheInfoFromPnfsManager : adding cacheinfo for "+pnfsId+" "+locations);

       externalList.addAll( locations ) ;

       return ;

   }
   public String hh_report_remove = "<reportRemoveReceiver>|off" ;
   public String ac_report_remove_$_1( Args args ){
       String broadcaster = args.argv(0);
       if( broadcaster.equals("off") )_broadcastCellName = null ;
       else _broadcastCellName = broadcaster ;

       return _broadcastCellName == null ?
              "Remove report disabled" :
              ("Sending removeReport to "+_broadcastCellName) ;
   }
   public String hh_ls_failed = "[-a] [-l]" ;
   public String ac_ls_failed( Args args )throws Exception {
      StringBuffer sb = new StringBuffer() ;
      boolean detail  = args.hasOption("l") ;
      boolean more    = args.hasOption("a") ;
      detail = more ? true : detail ;
      String [] poolFileList = _currentDir.list(
         //
         //
         new FilenameFilter(){
           public boolean accept( File dir , String name ){
              return name.startsWith("failed.") ;
           }
         }
      ) ;
      for( int i = 0 ; i < poolFileList.length ; i++ ){
         Set<String> hash  = new HashSet<String>() ;
         int     count = 0 ;
         String  line  = null ;
         sb.append(Formats.field(poolFileList[i],18,Formats.LEFT));
         if( detail ){
            File f = new File( _currentDir , poolFileList[i] ) ;
            BufferedReader br = null ;
            try{
               br = new BufferedReader( new FileReader( f ) ) ;
            }catch(Exception ee ){
               sb.append(" Can't open : ").append(f).append("\n") ;
               continue ;
            }
            while( ( line = br.readLine() ) != null ){
               count ++ ;
               if( more )hash.add(line.trim());
            }
            try{ br.close() ; }catch(Exception ee ){}

            sb.append(" ").
               append(Formats.field(""+count,5,Formats.RIGHT));
            if( more )
               sb.append(" ").
                  append(Formats.field(""+hash.size(),5,Formats.RIGHT));
         }
         sb.append("\n");

      }
      return sb.toString() ;
   }
   private synchronized void releaseGlobalLock(){ _globalLock.delete() ; }
   private synchronized void getGlobalLock() throws Exception {
       boolean result = _globalLock.createNewFile() ;
       if( ! result )
         throw new IllegalStateException("File exists: " + _globalLock.getAbsoluteFile());
  }
   @Override
public void messageArrived( CellMessage message ){
      Object obj = message.getMessageObject() ;
      _log.warn("Unexpected message arrived from : "+message.getSourcePath()+" "+obj.getClass().getName()+" "+obj.toString());
   }

}

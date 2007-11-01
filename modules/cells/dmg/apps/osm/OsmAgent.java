package dmg.apps.osm ;

import java.util.* ;
import java.io.* ;
import java.text.* ;

import dmg.util.* ;

public class OsmAgent implements Logable  {

   private Runtime     _runtime = Runtime.getRuntime() ;
   private LibraryInfo _dummyLibrary = null ;
   private int         _id = 100 ;
   private int         _simDate = 0;
   private int         _histogramBinWidth = 60 * 15 ;
   
   private synchronized int nextId(){ return _id ++ ; }
   private class EatIt implements Runnable {
      //
      // we have to make sure that nobody
      // calls toString, before the run routine
      // has finished ( AND started ) .
      //
      private InputStream _in = null ;
      private int _myId = nextId() ;
      private StringBuffer _buffer     = new StringBuffer() ;
      private Object       _runLock    = new Object() ;
      private boolean      _runStarted = false ;
      private EatIt( InputStream in ){
         _in = in ;
         say( "Starting io "+_myId) ;
         synchronized( _runLock ){
            new Thread(this).start() ;
            try{ 
               while(!_runStarted)_runLock.wait() ;
            }catch(InterruptedException ie ){
               esay( "runState lock interrupted" ) ;
            }
         }
      }
      public synchronized void run(){
         synchronized( _runLock ){
            _runStarted = true ;
            _runLock.notifyAll() ;  // release the constructor
         }
         BufferedReader br = null ;
         String line = null ;
         say( "IO thread started " + _myId ) ;
         try{
            br = new BufferedReader( 
                    new InputStreamReader( _in ) ) ;
       
            while( ( line = br.readLine() ) != null ){
               _buffer.append(line).append("\n");
            }
         }catch( Exception ee ){
            esay( "XXException : "+ee ) ;
         }finally{
            try{ br.close() ; }catch(Exception ee){}
         }
         say( "IO Thread done "+_myId  ) ;
      }
      public synchronized String toString(){ return _buffer.toString() ; }
   }
   private String _osmDir    = null ;
   private String _osmdError = null ;
   private String _osmqError = null ;
   private String _library   = null ;
   private Logable _log      = null ;
   
   public void log(String str){ }
   public void elog(String str){ }
   public void plog(String str){  }
   public OsmAgent( String library , Logable log ) throws MissingResourceException {
      //
      // try to find osmq, osmd
      //
      _log     = log == null ? this : log ;
      _library = library ;
      _osmDir  = findOsmDir() ;
   }
   public OsmAgent( String library ) throws MissingResourceException {
      //
      // try to find osmq, osmd
      //
      _log     = this ;
      _library = library ;
      _osmDir  = findOsmDir() ;
      say( "osmDir : "+_osmDir ) ;
   }
   public OsmAgent( String osmd , String osmq ) throws Exception {
      //
      // try to find osmq, osmd
      //
      int now = 0 ;
      DriveInfo [] drives = scanOsmd( loadFile(osmd) )  ;
      QueueInfo queueInfo = scanOsmq( loadFile(osmq) )  ;
   
      _dummyLibrary = new LibraryInfo( "dummy" , now , drives , queueInfo ) ;
   }
   public synchronized void say( String str ){
      _log.log(str) ;
   }
   public synchronized void esay( String str ){
      _log.elog(str);
   }
   private static String [] __choises = {
      "/usr/bin" ,
       "/usr/sbin" ,
       "./"
   } ;
   private String loadFile( String filename )throws IOException {
      File x = new File( filename ) ;
      BufferedReader br = new BufferedReader(
                            new FileReader( x ) ) ;
      StringBuffer sb = new StringBuffer() ;
      String line = null ;
      try{
         while( ( line = br.readLine() ) != null )
            sb.append(line).append("\n") ;
      }finally{
         try{ br.close() ; }catch(Exception xx ) {}
      }
      return sb.toString() ;
   }
   private void loadSimDate( File simDateFile ){
      _simDate = 0 ;
      try{
      
          BufferedReader br = new BufferedReader(
                                new FileReader( simDateFile ) ) ;
          try{
             String line = br.readLine() ;
             _simDate = parseTime(line) ;
          }finally{
             try{ br.close() ; }catch(Exception eee ){}
          }  
      }catch(Exception e){
      }
   }
   private String findOsmDir() throws MissingResourceException {
      
      File simDate = new File( "." , "date" ) ;
      if( simDate.exists() )loadSimDate(simDate) ;
      for( int i = 0 ; i < __choises.length ; i++ ){
         if( new File( __choises[i] , "osmd" ).exists() &&
             new File( __choises[i] , "osmq" ).exists()    )
             return __choises[i] ;
      }
      throw new 
      MissingResourceException( "Executable not found" , 
                                "Program" , 
                                "osmd/osmq" ) ;
   }
   private String runProcess( String name ) throws Exception {
       Process process = null ;
       try{
          say( "Running process "+name ) ;
          process = _runtime.exec( name ) ;
          say( "Process "+name+" started" ) ;
          EatIt error =  new EatIt( process.getErrorStream() ) ;
          EatIt output = new EatIt( process.getInputStream() ) ;
          say( "Waiting for process to finish" ) ;
          int rc = process.waitFor() ;
          if( rc != 0 ){
              esay( "Result of "+name+" : "+rc ) ;
              throw new
              Exception( "Result("+rc+") "+error.toString() ) ;
          }
          say( "Process "+name+" ok" ) ;
          return output.toString() ;
       }catch( Exception ee ){
          esay( "Process : "+name+" : "+ee ) ;
          throw ee ;
       }
   
   }
   private Object _varLock = new Object() ;
   private DriveInfo [] _driveEntries = null ;
   private QueueInfo     _queueInfo    = null ;
   private int           _now          = 0 ;
   private DriveInfo [] scanOsmd( String output ) throws Exception {
      BufferedReader br = new BufferedReader(
                           new StringReader( output ) ) ;
      String line = null ;
      StringTokenizer st = null ;
      String drive = null , status = null , owner = null , tape = null ;
      Vector v = new Vector() ;
      for( int i = 0 ; ( line = br.readLine() ) != null ; i++ ){
      
         if( i < 2 )continue ;
         st = new StringTokenizer(line) ;
         st.nextToken() ;
         drive = st.nextToken() ;
         st.nextToken() ;
         st.nextToken() ;
         st.nextToken() ;
         status = st.nextToken() ;
         tape   = st.nextToken() ;
         st.nextToken() ;
         owner = st.nextToken() ;
         v.addElement( new DriveInfo( drive , status , tape , owner ) ) ;        
      }
      DriveInfo [] entries = new DriveInfo[v.size()] ;
      v.copyInto(entries);
      synchronized( _varLock ){ _driveEntries = entries ; }
      return entries ;
   }
   private int parseTime( String timeString ) throws Exception {
   
     try{
        StringTokenizer st = new StringTokenizer( timeString , ":" ) ;
        int t = ( ( Integer.parseInt(st.nextToken()) * 60 ) + 
                    Integer.parseInt(st.nextToken())         ) * 60  +
                    Integer.parseInt(st.nextToken()) ;
        return t ;
     }catch(Exception ee){
        throw new 
        NumberFormatException( "Not a valid date : "+timeString);
     }
   
   
   }
   private int getTimeRelative( String timeString ) throws Exception {
      int now = _simDate > 0 ? _simDate : _now ;
      int x = (int)(now - parseTime(timeString)) ;
      //
      // looks a bit complicated, but we have to cover the jitter.
      //
      return x > 0 ? x : x < -120 ? ( 24*3600  + x ) :  0 ;
   }
   private int scanNow( String output ) throws Exception {
      BufferedReader br = new BufferedReader(
                           new StringReader( output ) ) ;
                           
       int t = parseTime( br.readLine() ) ;
       synchronized( _varLock ){ _now = t ; }
       
       return t ;
//       System.out.println( "Time : "+time ) ;  
   }
   private class HistValues {
     private int    _binWidth = 1 ;
     private int [] _values = new int[1] ;
     private int    _xMax   = 0 ;
     private HistValues( int binWidth ){
        _binWidth = binWidth ; 
        
     }
     private synchronized void add( int value ){
        if( value < 0 )return ;
        int pos = value / _binWidth ;
        if( pos >= _values.length ){
           int [] v = new int[pos+10] ;
           System.arraycopy( _values , 0 , v , 0 , _values.length ) ;
           _values = v ;
        }
        _values[pos] ++ ;
        
     }
     private synchronized int [] getHistogramArray(){
        int m = _values.length -1 ;
        for( ; ( m >= 0 ) && ( _values[m] == 0 ) ; m-- ) ;
        int [] x = new int[2 + ( m + 1 ) ] ;
        x[0] = 1 ;  // histogram presentation version 
        x[1] = _binWidth ;
        System.arraycopy( _values , 0 , x , 2 , x.length - 2 ) ;
        return x ;
     }   
   }
   public void setQueueHistogramBinWidth( int binWidth ){
      _histogramBinWidth = binWidth ;
   } 
   public int getQueueHistogramBinWidth(){
      return _histogramBinWidth ;
   } 
   private QueueInfo scanOsmq( String output ) throws Exception {
      if( _now == 0 ){
          esay( "Now not yet known" ) ;
          return new QueueInfo(0,0,0);
      }
      BufferedReader br = new BufferedReader(
                           new StringReader( output ) ) ;
      String line        = null ;
      StringTokenizer st = null ;
      Vector v         = new Vector() ;
      int requestCount = 0 ;
      int startTime = 0 ;
      int timeValue = 0 ;
      int binWidth  = _histogramBinWidth ;
      HistValues histogram = new HistValues(binWidth) ;
      for( int i = 0 ; ( line = br.readLine() ) != null ; i++ ){
      
         if( i < 2 )continue ;
         st = new StringTokenizer(line) ;
         st.nextToken() ;
         st.nextToken() ;
         st.nextToken() ;
         st.nextToken() ;
         st.nextToken() ;
         histogram.add( timeValue = getTimeRelative(st.nextToken()) );       
         if( i == 2 )startTime = timeValue ;
         requestCount ++ ;
      }
      
      QueueInfo info = new QueueInfo( requestCount , 
                                      startTime ,
                                      timeValue ) ;
      synchronized( _varLock ){ 
         _queueInfo = info ; 
         _queueInfo.setHistogram( histogram.getHistogramArray() ) ;
      }
      return info ;
      
   }
   public LibraryInfo getLibraryInfo() throws Exception {
     return getLibraryInfo( _library ) ;
   }
   public LibraryInfo getLibraryInfo( String library ) throws Exception {
   
      if( _dummyLibrary != null )return _dummyLibrary ;
      
      int now = scanNow( runProcess( "/bin/date +%H:%M:%S") ) ;
      DriveInfo [] drives = 
           scanOsmd( runProcess(_osmDir+"/osmd "+library ) )  ;
      QueueInfo queueInfo = 
           scanOsmq( runProcess(_osmDir+"/osmq "+library ) )  ;
      say( "Library info assembled" ) ;
      return new LibraryInfo( library , now , drives , queueInfo ) ;
   }
   public static void main( String [] args ) throws Exception {
      final OsmAgent agent = new OsmAgent( "powder" ) ;
      Thread xxx =
      new Thread( 
             new Runnable(){
                public void run(){
                   try{
                      System.out.println( "Run started" ) ;
//                      Thread.sleep(3000);
                      System.out.println( agent.getLibraryInfo().toString() ) ;
                   }catch(Exception e ){
                       System.err.println("Exception "+e ) ;
                   }
                }
             }
       ) ;
       xxx.start() ;
       Thread.sleep(1000) ;
       System.out.println( "Thread started" ) ;
   }
   
   
   
}

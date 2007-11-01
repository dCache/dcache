package dmg.cells.examples ;
import  dmg.protocols.ssh.* ;
import  java.math.* ;
import  java.net.* ;
import  java.io.* ;
import  java.util.* ;

public class      SshObjectClient 
       implements SshClientAuthentication, Runnable {
       
   private SshStreamEngine    _engine ;
   private Socket             _socket ;
   private String             _user , _passwd ;
   private Thread             _listenThread ;
   private Thread             _sendThread ;
   private final static int ST_NULL     = 0 ;
   private final static int ST_PASSWORD = 1 ;
   private final static int ST_RSA      = 2 ;
   private final static int ST_RHOSTS_RSA      = 3 ;
   private ObjectOutputStream _out  ;
   private ObjectInputStream  _in   ;
   private BufferedReader     _reader ;
   private PrintWriter        _writer ;
   
   public SshObjectClient( String address ,
                           int port ,
                           String user ,
                           String passwd      ) throws Exception {
                         
      _socket    = new Socket( address , port ) ;      
      _user      = user ;
      _passwd    = passwd ;                       
      _engine    = new SshStreamEngine( _socket , this ) ;

//      _out = new ObjectOutputStream(_engine.getOutputStream() ) ;
//      _in  = new ObjectInputStream(_engine.getInputStream() ) ;

      _reader = new BufferedReader( _engine.getReader() ) ;
      _writer = new PrintWriter( _engine.getWriter() ) ;
      
      _listenThread = new Thread( this ) ;
      _listenThread.start() ;
      _sendThread = new Thread( this ) ;
      _sendThread.start() ;
   } 
   public void run(){
      
      if( Thread.currentThread() == _sendThread ){
          try{ 
            for( int n = 0 ; n < 4 ; n ++ ){
               Thread.sleep(1000) ; 
               _writer.println(  "ps -a" ) ;
               _writer.flush() ;
               System.out.println( "Send ready" ) ;
            }
            _writer.println( "switchToObjects" ) ;
            _writer.flush() ;
            System.out.println( "Switching to ObjectOutputStream" ) ;
            _out = new ObjectOutputStream(_engine.getOutputStream() ) ;
            for( int n = 0 ; n < 10 ; n ++ ){
               Thread.sleep(1000) ; 
               _out.writeObject( new SLRequest( "System" , "ps -a" )  ) ;
               _out.flush() ;
            }
            _out.writeObject( "exit" ) ;
            _out.flush() ;
          }catch(Exception ee){
             ee.printStackTrace() ;
          }
      }else if( Thread.currentThread() == _listenThread ){
          int rc ;
          try{
            while(true){
               String line = _reader.readLine() ;
               if( line == null )throw new EOFException("line=null") ;
               System.out.println( "received line "+line ) ;
               if( line.equals( "switchToObjects" ) )break ;
            }
            System.out.println( "Switching to ObjectInputStream" ) ;
            _in  = new ObjectInputStream(_engine.getInputStream() ) ;
            while(true){
               Object obj = _in.readObject() ;
               System.out.println( "received object "+obj.toString() ) ;
            }
          }catch(Exception ee){
             ee.printStackTrace() ;
          }
      }
   }
   public void run2(){
      
      if( Thread.currentThread() == _sendThread ){
          try{ 
            for( int n = 0 ; n < 10 ; n ++ ){
               Thread.sleep(1000) ; 
               _out.writeObject( new SLRequest( "System" , "ps -a" )  ) ;
            }
            _out.writeObject( "exit" ) ;
          }catch(Exception ee){
             ee.printStackTrace() ;
          }
      }else if( Thread.currentThread() == _listenThread ){
          byte [] buffer = new byte[1024] ;
          int rc ;
          try{
            while(true){
               Object obj = _in.readObject() ;
               System.out.println( "received "+obj.toString() ) ;
            }
          }catch(Exception ee){
             ee.printStackTrace() ;
          }
      }
   }
   //
   //   Client Authentication interface 
   //   
   public boolean  isHostKey( InetAddress host , SshRsaKey keyModulus ) {
       return true ;
   }
   public String getUser( ){
      System.out.println( " User requested" ) ;
      return _user ;
   }
   public SshSharedKey  getSharedKey( InetAddress host ){ return null ; }
   private int _state = ST_PASSWORD ;

   public SshAuthMethod getAuthMethod(){
   
      SshAuthMethod rc = null ;
      
      switch( _state ){
         case ST_NULL :
            rc = null ;
         break ;
         case ST_PASSWORD :
            System.out.println( "Trying ST_PASSWORD" ) ;
            rc = new SshAuthPassword( _passwd ) ;
            _state = ST_NULL ;
            break ;
      
      }
      return rc ;
   }
   
   public static void main( String [] args ){
   
      if( args.length < 4 ){
        System.out.println( 
        " USAGE : SshObjectClient <host> <port> <user> <passwd>"  );
        System.exit(4);
      }
      try{
         String address = args[0] ;
         int port  = new Integer( args[1] ).intValue() ;

         String user    = args[2] ;
         String passwd  = args[3] ;
         
         new SshObjectClient( address , port , user , passwd ) ;
         
      }catch( Exception e ){
         System.out.println( "Exception : "+e ) ;
         e.printStackTrace() ;
      }
   }


}

package dmg.cells.services.login ;

import java.io.* ;
import java.util.* ;
import dmg.security.digest.Crypt ;

public class UnixPassword {

   private Hashtable _hash      = null ;
   private Crypt     _crypt     = new dmg.security.digest.Crypt() ;
   private long      _timeStamp = 0 ;
   private File      _file      = null ;
   
   public UnixPassword( String pswFile ) throws IOException {
   
      _file = new File( pswFile ) ;
      if( ! _file.canRead() ) {
          throw new IOException("File Not Found : " + pswFile);
      }
      
      _update() ;
   }
   public synchronized void update(){
       long ts = _file.lastModified() ;
       try{
          if( ts > _timeStamp ) {
              _update();
          }
       }catch(Exception ee ){}
   }
   private synchronized void _update() throws IOException {
      
      BufferedReader  br   = new BufferedReader(
                                 new FileReader( _file ) ) ;
      String          line = null ;
      StringTokenizer st   = null ;
      
      _hash = new Hashtable() ;
      
      try{
         while( ( line = br.readLine() ) != null ){
            try{
                st = new StringTokenizer( line , ":" ) ;
                _hash.put( st.nextToken() , st.nextToken() ) ;
            }catch( Exception ee ){}
         }
      }finally{
         br.close() ;
      }  
   
   }
   public synchronized boolean checkPassword( String user , String password ){
      update() ;
      String cipher = (String)_hash.get( user ) ;
      if( cipher == null ) {
          return false;
      }
      
      String result = _crypt.crypt( cipher , password ) ;
      return _crypt.crypt( cipher , password ).equals(cipher) ;
   }
   public static void main(String [] args ) throws Exception {
      if( args.length < 3 ){
         System.out.println( "Usage : ... <file> <user> <passwd>" ) ;
         System.exit(4);
      }
      UnixPassword p = new UnixPassword( args[0] ) ;
      System.out.println( "Result : "+p.checkPassword(args[1],args[2])) ;
   }
}

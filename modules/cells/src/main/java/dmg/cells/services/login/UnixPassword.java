package dmg.cells.services.login ;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.StringTokenizer;

public class UnixPassword {

   private Hashtable<String, String> _hash;
   private Crypt     _crypt     = new Crypt() ;
   private long      _timeStamp;
   private File      _file;

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

       String          line;
      StringTokenizer st;

      _hash = new Hashtable<>() ;

       try (BufferedReader br = new BufferedReader(
               new FileReader(_file))) {
           while ((line = br.readLine()) != null) {
               try {
                   st = new StringTokenizer(line, ":");
                   _hash.put(st.nextToken(), st.nextToken());
               } catch (Exception ee) {
               }
           }
       }


   }
   public synchronized boolean checkPassword( String user , String password ){
      update() ;
      String cipher = _hash.get( user );
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

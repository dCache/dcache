package dmg.security.cipher ;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Hashtable;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class EncryptionKeyContainer {

    private Hashtable<String, EncryptionKey> _publicList  = new Hashtable<>() ;
    private Hashtable<String, EncryptionKey> _sharedList  = new Hashtable<>() ;
    private Hashtable<String, EncryptionKey> _privateList = new Hashtable<>() ;

    public synchronized void addKey( EncryptionKey key ){

        String [] domains = key.getDomainList() ;
        String    mode = key.getKeyMode() ;
        Hashtable<String, EncryptionKey> hash = mode.equals("public")  ? _publicList  :
                         mode.equals("private") ? _privateList :
                         mode.equals("shared" ) ? _sharedList  : null ;
        if( hash == null ) {
            return;
        }

        for (String domain : domains) {
            hash.put(domain, key);
        }
    }
    public synchronized void readInputStream( EncryptionKeyInputStream stream )
           throws IOException {
       EncryptionKey key ;
       while( ( key = stream.readEncryptionKey() ) != null ) {
           addKey(key);
       }

    }
    public synchronized EncryptionKey get( String mode , String name )
           throws EncryptionKeyNotFoundException   {

        EncryptionKey key;
        Hashtable<String, EncryptionKey> hash = mode.equals("public")  ? _publicList  :
                         mode.equals("private") ? _privateList :
                         mode.equals("shared" ) ? _sharedList  : null ;

        if( hash == null ){
          if((key = _publicList.get(name)) == null ){
            if((key = _privateList.get(name)) == null ){
              if((key = _sharedList.get(name)) != null ) {
                  return key;
              }
            }
          }
        }else{
          key = hash.get( name );
        }
        if( key == null ) {
            throw new EncryptionKeyNotFoundException(name + " : not found");
        }

        return key ;
    }


    public static void main( String [] args ){
      if( args.length != 1 ){
         System.out.println( "USAGE : ... <keyfilename> " ) ;
         System.exit(4) ;
      }
      try{
         MixedKeyInputStream in = new MixedKeyInputStream(
                                  new FileInputStream( args[0] ) ) ;
         EncryptionKeyContainer container =
                    new EncryptionKeyContainer() ;

 //        container.readEncryptionKey( in ) ;


      }catch(IOException e ){
        System.err.println( " Exception : " + e ) ;
        System.exit(4);
      }

   }

}

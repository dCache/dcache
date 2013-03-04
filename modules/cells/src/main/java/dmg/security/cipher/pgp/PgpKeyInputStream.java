package dmg.security.cipher.pgp ;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import dmg.security.cipher.EncryptionKey;
import dmg.security.cipher.EncryptionKeyInputStream;
import dmg.security.cipher.rsa.RsaEncryptionKey;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      PgpKeyInputStream
       extends    PGPInputStream
       implements EncryptionKeyInputStream {


    private final static int IDLE              = 0 ;
    private final static int CERTIFICATE_FOUND = 1 ;
    private final static int PUBLIC_LEFT       = 2 ;

    private int       _state  = IDLE ;
    private PGPPacket _key;
    private String [] _domainList ;

    public PgpKeyInputStream( InputStream in ){
       super( in ) ;

    }
    @Override
    public EncryptionKey readEncryptionKey() throws IOException {
      PGPPacket pgp ;
      PGPKeyCertificate       publicKey ;
      PGPSecretKeyCertificate privateKey ;
      while( true ){
       switch( _state ){

          case IDLE :
             try{ if( ( pgp = readPGPPacket() ) == null ) {
                 return null;
             }
             }catch( EOFException eof ){ return null ; }
//             System.out.println( " reading : "+pgp.toString() ) ;
             if( pgp instanceof PGPKeyCertificate ){
                _state  = CERTIFICATE_FOUND ;
                _key    = pgp ;
             }
          break ;
          case CERTIFICATE_FOUND :
             try{ if( ( pgp = readPGPPacket() ) == null ) {
                 return null;
             }
             }catch( EOFException eof ){ return null ; }
//             System.out.println( " reading : "+pgp.toString() ) ;
             if( pgp instanceof PGPKeyCertificate ){
                _state  = CERTIFICATE_FOUND ;
                _key    = pgp ;
             }else if( pgp instanceof PGPUserIdPacket ){
                _domainList    = new String[1] ;
                _domainList[0] = ((PGPUserIdPacket)pgp).getId() ;
                _state         = PUBLIC_LEFT ;
                if( _key instanceof PGPSecretKeyCertificate ){
                   privateKey  = (PGPSecretKeyCertificate)_key ;
                   return new RsaEncryptionKey( _domainList ,
                                                "private",
                                                privateKey.getD() ,
                                                privateKey.getN()    ) ;
                }
             }
          break ;
          case PUBLIC_LEFT :
             _state     = IDLE ;
             publicKey  = (PGPKeyCertificate)_key ;
             return new RsaEncryptionKey( _domainList ,
                                          "public",
                                          publicKey.getE() ,
                                          publicKey.getN()    ) ;


       }
      }
    }
   public static void main( String [] args ){
     if( args.length < 2 ){
       System.err.println( " USAGE : ... pgp2mixed <secretKeyRing>" ) ;
       System.exit(4) ;
     }
     if( args[0].equals( "pgp2mixed" ) ){

        String filename = args[1] ;
	String [] domainList ;
        try{
            PgpKeyInputStream  pgpInput =
                new PgpKeyInputStream(  new FileInputStream( filename ) ) ;
            RsaEncryptionKey key ;
            while( ( key = (RsaEncryptionKey)pgpInput.readEncryptionKey() ) != null ){
	       domainList = key.getDomainList() ;
	       for( int i = 0 ; i <domainList.length ; i++ ){
	          System.out.print( domainList[i] ) ;
		  if( i < domainList.length-1) {
                      break;
                  }
	             System.out.print( "," ) ;
	       }
	       System.out.print( " "+key.getKeyType()+","+key.getKeyMode()+ " " ) ;
	       System.out.print( key.getExponent().toString(16)+" "+
	                         key.getModulus().toString(16)+"\n"       ) ;
            }
        }catch( IOException ioe ){
          System.err.println( " Exception : "+ioe );
          System.exit(1) ;
        }
     }else{
       System.err.println( " USAGE : ... pgp2mixed <secretKeyRing>" ) ;
       System.exit(4) ;
     }

   }

}

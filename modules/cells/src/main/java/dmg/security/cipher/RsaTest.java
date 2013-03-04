package dmg.security.cipher ;

import java.io.FileInputStream;
import java.util.Date;

import dmg.security.cipher.rsa.RsaEncryption;
import dmg.security.cipher.rsa.RsaEncryptionKey;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class RsaTest {

   public static void main( String [] args ){
     if( args.length < 1 ){
       System.err.println( " USAGE : ... <secretKeyRing> " ) ;
       System.exit(4) ;
     }
     String filename = args[0] ;
     try{
         EncryptionKeyInputStream  input =
             new MixedKeyInputStream(  new FileInputStream( filename ) ) ;

	 EncryptionKeyContainer container =
	     new EncryptionKeyContainer() ;

	 container.readInputStream( input ) ;

	 RsaEncryptionKey pub1 = (RsaEncryptionKey)container.get( "public" , "user1" ) ;
	 RsaEncryptionKey pub2 = (RsaEncryptionKey)container.get( "public" , "user2" ) ;
	 RsaEncryptionKey pri1 = (RsaEncryptionKey)container.get( "private" , "user1" ) ;
	 RsaEncryptionKey pri2 = (RsaEncryptionKey)container.get( "private" , "user2" ) ;

	 RsaEncryption encr = new RsaEncryption( pub1 , pri2 ) ;
	 RsaEncryption decr = new RsaEncryption( pub2 , pri1 ) ;

	 byte [] in = "hallo du da".getBytes() ;

	 long start = new Date().getTime() ;
	 byte [] cipher = encr.encrypt( in , 0 , in.length ) ;
	 byte [] result = decr.decrypt( cipher , 0 , cipher.length ) ;
	 long finish = new Date().getTime() ;


	 System.out.println( " Result : >"+new String( result )+"<" ) ;
	 System.out.println( " Time : "+(finish-start)+" ms" ) ;
     }catch( Exception e ){
        System.out.println( " Exception : "+e ) ;
	System.exit(4) ;
     }
     System.exit(0);

   }


}

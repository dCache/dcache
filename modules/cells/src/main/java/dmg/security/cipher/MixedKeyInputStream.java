package dmg.security.cipher ;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import dmg.security.cipher.idea.IdeaEncryptionKey;
import dmg.security.cipher.rsa.RsaEncryptionKey;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      MixedKeyInputStream
       extends    FilterInputStream
       implements EncryptionKeyInputStream {

   private BufferedReader _in ;
   public MixedKeyInputStream( InputStream in ){
      super( in ) ;
      _in = new BufferedReader(
            new InputStreamReader( in ) ) ;
   }
   @Override
   public EncryptionKey readEncryptionKey()
          throws IOException {

      String line = _in.readLine() ;
      if( line == null ) {
          return null;
      }

      StringTokenizer domains;
      StringTokenizer options;

      String cipher;
      int tokens ;
      //
      //   <domain>,<domain>,...    idea        <key>
      //   <domain>,<domain>,...    rsa,public  <e>     <n>
      //   <domain>,<domain>,...    rsa,private <e>     <n>
      //
      StringTokenizer st = new StringTokenizer( line ) ;
      if( st.hasMoreTokens() ){
         domains = new StringTokenizer( st.nextToken() ,"," ) ;
         tokens  = domains.countTokens() ;
         String [] domainList = new String[ tokens ] ;
         for( int i= 0 ; i < tokens ; i++ ) {
             domainList[i] = domains.nextToken();
         }

         if( st.hasMoreTokens() ){

            options = new StringTokenizer( st.nextToken() , "," ) ;
            tokens  = options.countTokens() ;
            cipher  = options.nextToken() ;

            if( cipher.equals( "idea" ) && st.hasMoreTokens() ){
               try{
                  return new IdeaEncryptionKey( domainList , st.nextToken() ) ;
               }catch( Exception e ){
                  throw new IOException( "IllegalNumberFormat : " + e ) ;
               }
            }else if( cipher.equals( "rsa" ) && options.hasMoreTokens() ){
               String mode = options.nextToken() ;
               tokens = st.countTokens() ;
               if( tokens > 1  ){
                  String e = st.nextToken() ;
                  String n = st.nextToken() ;
                  try{
                    return new RsaEncryptionKey( domainList , mode , e , n ) ;
                  }catch( Exception ee ){
                     throw new IOException( "IllegalNumberFormat: " + e ) ;
                  }
               }
            }else{
               if( st.countTokens() < 1 ) {
                   return null;
               }
               String n = st.nextToken() ;
               try{
                  return new RsaEncryptionKey( domainList ,
                                               cipher ,
                                               n ) ;
               }catch( Exception iooi ){
                  return null ;
               }
            }

         }
      }
      return null ;

   }
   public static void main( String [] args ){
      if( args.length != 1 ){
         System.out.println( "USAGE : ... <keyfilename> " ) ;
         System.exit(4) ;
      }
      try{
         MixedKeyInputStream in = new MixedKeyInputStream(
                                  new FileInputStream( args[0] ) ) ;
         EncryptionKey key ;
         while( ( key = in.readEncryptionKey() ) != null ){

            System.out.println( ""+key ) ;
         }

      }catch(IOException e ){
        System.err.println( " Exception : " + e ) ;
        System.exit(4);
      }

   }

}

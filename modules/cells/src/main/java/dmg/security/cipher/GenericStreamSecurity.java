package dmg.security.cipher ;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.StringTokenizer;

import dmg.security.cipher.idea.IdeaEncryptionKey;
import dmg.security.cipher.idea.IdeaStreamEncryption;
import dmg.security.cipher.rsa.RsaEncryptionKey;
import dmg.security.cipher.rsa.RsaStreamEncryption;
/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class GenericStreamSecurity implements StreamSecurity {

   private EncryptionKeyContainer _keys = new EncryptionKeyContainer() ;

   public GenericStreamSecurity( String keyFile )
          throws IOException {

       _keys.readInputStream( new MixedKeyInputStream(
                              new FileInputStream( keyFile ) ) ) ;

   }
   @Override
   public StreamEncryption getEncryption( String domain )
          throws EncryptionKeyNotFoundException {
      //
      // scan the domain string.
      //
      StringTokenizer st = new StringTokenizer( domain , ":" ) ;
      int tokens = st.countTokens() ;
      if( tokens < 2 ) {
          throw
                  new EncryptionKeyNotFoundException("Invalid domain desc: " + domain);
      }

      String cipher = st.nextToken() ;
       switch (cipher) {
       case "idea":
           String name = st.nextToken();
           EncryptionKey key = _keys.get("shared", name);
           try {
               return new IdeaStreamEncryption((IdeaEncryptionKey) key);
           } catch (Exception e) {
               throw
                       new EncryptionKeyNotFoundException("not shared : " + name);
           }
       case "rsa":
           if (tokens < 3) {
               throw
                       new EncryptionKeyNotFoundException("Invalid domain desc: " + domain);
           }
           String pubName = st.nextToken();
           String priName = st.nextToken();
           EncryptionKey pub = _keys.get("public", pubName);
           EncryptionKey pri = _keys.get("private", priName);
           try {
               return new RsaStreamEncryption((RsaEncryptionKey) pub,
                       (RsaEncryptionKey) pri);
           } catch (Exception e) {
               throw
                       new EncryptionKeyNotFoundException("not rsa : " + domain);
           }
       default:
           throw
                   new EncryptionKeyNotFoundException("Unknown cipher type : " + cipher);
       }
   }
   @Override
   public StreamEncryption getSessionEncryption(){
      return new IdeaStreamEncryption() ;
   }
   @Override
   public StreamEncryption getSessionEncryption( byte [] keyDescriptor )
          throws IllegalEncryptionException {
      return new IdeaStreamEncryption( keyDescriptor ) ;
   }
}


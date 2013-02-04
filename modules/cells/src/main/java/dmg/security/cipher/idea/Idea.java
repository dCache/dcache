 package dmg.security.cipher.idea  ;


 /**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
 public class Idea extends Jdea {

     public Idea( byte [] k ) throws IllegalArgumentException {
        super( k ) ;
     }
     public Idea(){ super() ; } 
     /**
      * Encrypts a byte array[8] in Electronic Code Book mode.
      *
      * The call returns a byte array of length 8 containing the
      * encrypted message. The <code>in</code> array remains unchanged.
      *
      * @param in must be at least 8 bytes long. More bytes are silently 
      *           ignored.
      * @exception IllegalArgumentException if in.length is less then 8.
      */
     public byte [] encryptECB( byte [] in ){
        byte [] out = new byte[8] ;
        encryptECB( in , 0 , out , 0 ) ;
        return out ;
     }
     /**
      * Decrypts a byte array[8] in Electronic Code Book mode.
      *
      * The call returns a byte array of length 8 containing the
      * decrypted message. The <code>in</code> array remains unchanged.
      *
      * @param in must be at least 8 bytes long. More bytes are silently 
      *           ignored.
      * @exception IllegalArgumentException if in.length is less then 8.
      */
      public byte [] decryptECB( byte [] in ){
        if( in.length < 8 ) {
            throw new IllegalArgumentException();
        }
        byte [] out = new byte[8] ;
        decryptECB( in , 0 , out , 0 ) ;
        return out ;
      }
     /**
      * Encrypts a byte array using the Feedback Block Cipher mode.
      *
      * The call returns a byte array containing the encrypted message.
      * The length of the output array is identical to the length of
      * the input array.
      * The <code>in</code> array remains unchanged.
      *
      * @param in is the array which is going to be encrypted.
      */
      public byte [] encryptCFB64( byte [] in ){
        byte [] out = new byte[in.length] ;
        encryptCFB64( in , 0 , out , 0 , in.length  ) ; 
        return out ;
      }
     /**
      * Decrypts a byte array using the Feedback Block Cipher mode.
      *
      * The call returns a byte array containing the decrypted message.
      * The length of the output array is identical to the length of
      * the input array.
      * The <code>in</code> array remains unchanged.
      * The starting vector must be identical to the starting
      * vector of the encryption process.
      *
      * @param in is the array which is going to be decrypted.
      */
      public byte [] decryptCFB64( byte [] in ){
        byte [] out = new byte[in.length] ;
        decryptCFB64( in , 0 , out , 0 , in.length  ) ; 
        return out ;
      }
    static public String byteToHexString( byte b ) {
       return Integer.toHexString( ( b < 0 ) ? ( 256 + (int)b ) : (int)b  ) ;
    
    }
    static public String byteToHexString( byte [] bytes ) {
        
  	  StringBuilder sb = new StringBuilder(bytes.length +1);

        for (byte aByte : bytes) {
            sb.append(byteToHexString(aByte)).append(" ");
        }
         return sb.toString() ;    
    }
 }

package dmg.security.digest ;

import java.util.zip.CRC32;

public class ZipCrc32 extends CRC32 implements MsgDigest {

   public ZipCrc32(){ super() ; }
//   public void reset(){ super.reset() ; }
//   public void update( byte [] data ){ super.update( data ) ; }
//   public void update( byte [] data , int off , int size ){
//     super.update( data , off , size ) ;
//   }
   @Override
   public byte [] digest(){
     byte [] r = new byte[4] ;
     long crc  = super.getValue() ;

       r[0] = (byte)(( crc >>> 24 ) & 0xff ) ;
       r[1] = (byte)(( crc >>> 16 ) & 0xff ) ;
       r[2] = (byte)(( crc >>>  8 ) & 0xff ) ;
       r[3] = (byte)(( crc >>>  0 ) & 0xff ) ;

       return r ;
   }

}

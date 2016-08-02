/*
 * @(#)Checksum.java	1.2 03/11/10
 *
 * Copyright 1996-2003 dcache.org All Rights Reserved.
 *
 * This software is the proprietary information of dCache.org
 * Use is subject to license terms.
 */

package diskCacheV111.util;


import java.io.FileInputStream;
import java.security.MessageDigest;

import org.dcache.util.Checksum;

import static org.dcache.util.ByteUnit.KiB;

/**
 *
 * @author  Patrick Fuhrmann
 * @version 1.2, 03/11/10
 * @see     Preferences
 * @since   1.4
 */

public class Adler32 extends MessageDigest
{

   private static final int BASE = 65521 ; /* largest prime smaller than 65536 */
   private final java.util.zip.Adler32 _zipAdler;
   private long _adler = 1L ;
   public Adler32(){

       super("ADLER32") ;
      _zipAdler = new java.util.zip.Adler32() ;
   }
   public void resetAdler32(){
       _zipAdler.reset() ;
       _adler = 1L ;
   }
//   public long updateAdler32(long adler, byte [] buf, int len){
//      return updateAdler32( adler , buf , 0 , len ) ;
//   }
   public long updateAdler32(long adler, byte [] buf, int off , int len){

	if( buf == null ) {
            return 1L;
        }

	int s1 = (int) ( adler & 0xffffL );
	int s2 = (int) ( ( adler >> 16) & 0xffffL );


	for( int n = 0 ; n < len; n++ ){

           s1 = (s1 + ( buf[off+n] & 0xFF ) ) % BASE;
           s2 = (s2 + s1)     % BASE;

	}
	return (s2 << 16) + s1;
   }
   public long getAdler32(){
       return _adler ;
   }

   @Override
   public byte [] engineDigest(){
//      return digestAdler32() ;
      return digestAdlerZip() ;
   }

   @Override
   public void engineReset(){
//      resetAdler32() ;
      _zipAdler.reset() ;
   }
   @Override
   public void engineUpdate( byte input ){
      byte [] x = { input } ;
//      _adler = updateAdler32( _adler , x , 0 , 1 ) ;
      _zipAdler.update(  x , 0 , 1 ) ;
   }
   @Override
   public void engineUpdate( byte [] data , int offset , int size ){
//       _adler = updateAdler32( _adler , data , offset , size ) ;
       _zipAdler.update( data, offset , size ) ;
   }
   @Override
   public int engineGetDigestLength(){ return 4 ; }

   public byte [] digestAdler32(){
      byte [] _value = new byte[4] ;
      _value[0] = (byte) ((_adler>>24)&0xff) ;
      _value[1] = (byte) ((_adler>>16)&0xff) ;
      _value[2] = (byte) ((_adler>>8)&0xff) ;
      _value[3] = (byte) ((_adler)&0xff) ;
      return _value ;
   }
   public byte [] digestAdlerZip(){
      _adler = _zipAdler.getValue() ;
      byte [] _value = new byte[4] ;
      _value[0] = (byte) ((_adler>>24)&0xff) ;
      _value[1] = (byte) ((_adler>>16)&0xff) ;
      _value[2] = (byte) ((_adler>>8)&0xff) ;
      _value[3] = (byte) ((_adler)&0xff) ;
      return _value ;
   }
   public static void main( String [] args )throws Exception {

       if( args.length < 1 ){
           System.err.println("Usage : ... <filename>") ;
           System.exit(4);
       }
       MessageDigest adler = new Adler32() ;

       FileInputStream in = new FileInputStream( args[0] ) ;
       byte [] buffer = new byte[KiB.toBytes(1)] ;
       long sum = 0L ;
       long started = System.currentTimeMillis() ;
       while(true){

           int rc = in.read( buffer , 0 , buffer.length ) ;
           if( rc <=0 ) {
               break;
           }
           sum += rc ;
           adler.update( buffer , 0 , rc ) ;
       }
       started = System.currentTimeMillis() - started ;
       in.close() ;
       byte [] digest = adler.digest() ;
       System.out.println("Adler : ("+sum+") "+Checksum.bytesToHexString(digest) ) ;
       System.out.println("Done in "+started+" milli seconds" ) ;
       System.exit(0);
   }
}


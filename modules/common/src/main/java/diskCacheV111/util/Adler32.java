/*
 * @(#)Checksum.java	1.2 03/11/10
 *
 * Copyright 1996-2018 dcache.org All Rights Reserved.
 *
 * This software is the proprietary information of dCache.org
 * Use is subject to license terms.
 */

package diskCacheV111.util;

import java.security.MessageDigest;

/**
 * A {@link MessageDigest} implementation that uses <b>adler32</b> algorithm.
 *
 * @author  Patrick Fuhrmann
 * @version 1.2, 03/11/10
 * @see     MessageDigest
 * @see     https://en.wikipedia.org/wiki/Adler-32
 * @since   1.4
 */

public class Adler32 extends MessageDigest
{

   private final java.util.zip.Adler32 _zipAdler;
   private long _adler = 1L ;

   public Adler32(){
       super("ADLER32") ;
      _zipAdler = new java.util.zip.Adler32() ;
   }

   @Override
   public byte [] engineDigest(){
      return digestAdlerZip() ;
   }

   @Override
   public void engineReset(){
      _zipAdler.reset() ;
   }
   @Override
   public void engineUpdate( byte input ){
      byte [] x = { input } ;
      _zipAdler.update(  x , 0 , 1 ) ;
   }
   @Override
   public void engineUpdate( byte [] data , int offset , int size ){
       _zipAdler.update( data, offset , size ) ;
   }
   @Override
   public int engineGetDigestLength(){ return 4 ; }

   private byte [] digestAdlerZip(){
      _adler = _zipAdler.getValue() ;
      byte [] _value = new byte[4] ;
      _value[0] = (byte) ((_adler>>24)&0xff) ;
      _value[1] = (byte) ((_adler>>16)&0xff) ;
      _value[2] = (byte) ((_adler>>8)&0xff) ;
      _value[3] = (byte) ((_adler)&0xff) ;
      return _value ;
   }
}


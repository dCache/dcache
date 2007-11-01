 /*
 * @(#)Checksum.java	1.2 03/11/10
 *
 * Copyright 1996-2003 dcache.org All Rights Reserved.
 * 
 * This software is the proprietary information of dCache.org  
 * Use is subject to license terms.
 */

package diskCacheV111.movers;


import diskCacheV111.util.Checksum ;
/**
 *
 * @author  Patrick Fuhrmann
 * @version 1.2, 03/11/10
 * @see     Preferences
 * @since   1.4
 */


public interface ChecksumMover {

   public void     setDigest( Checksum checksum ) ;
   public Checksum getClientChecksum() ;
   public Checksum getTransferChecksum() ;

}

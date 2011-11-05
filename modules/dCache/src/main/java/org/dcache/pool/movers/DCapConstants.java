// $Id: DCapConstants.java,v 1.3 2007-07-08 21:39:11 tigran Exp $
package org.dcache.pool.movers;


/**
 * DCAP data channel IO constants
 */
public interface DCapConstants
{
    public static final int IOCMD_WRITE     = 1;
    public static final int IOCMD_READ      = 2;
    public static final int IOCMD_SEEK      = 3;
    public static final int IOCMD_CLOSE     = 4;
    public static final int IOCMD_INTERRUPT = 5;
    public static final int IOCMD_ACK       = 6;
    public static final int IOCMD_FIN       = 7;
    public static final int IOCMD_DATA      = 8;
    public static final int IOCMD_LOCATE    = 9;
    public static final int IOCMD_STATUS         = 10;
    public static final int IOCMD_SEEK_AND_READ  = 11;
    public static final int IOCMD_SEEK_AND_WRITE = 12;
    public static final int IOCMD_READV = 13;
    public static final int IOCMD_SEEK_SET      = 0;
    public static final int IOCMD_SEEK_CURRENT  = 1;
    public static final int IOCMD_SEEK_END      = 2;
}

// $Id: DCapConstants.java,v 1.3 2007-07-08 21:39:11 tigran Exp $
package org.dcache.pool.movers;


/**
 * DCAP data channel IO constants
 */
public interface DCapConstants
{
    int IOCMD_WRITE     = 1;
    int IOCMD_READ      = 2;
    int IOCMD_SEEK      = 3;
    int IOCMD_CLOSE     = 4;
    int IOCMD_INTERRUPT = 5;
    int IOCMD_ACK       = 6;
    int IOCMD_FIN       = 7;
    int IOCMD_DATA      = 8;
    int IOCMD_LOCATE    = 9;
    int IOCMD_STATUS         = 10;
    int IOCMD_SEEK_AND_READ  = 11;
    int IOCMD_SEEK_AND_WRITE = 12;
    int IOCMD_READV = 13;
    int IOCMD_SEEK_SET      = 0;
    int IOCMD_SEEK_CURRENT  = 1;
    int IOCMD_SEEK_END      = 2;
}

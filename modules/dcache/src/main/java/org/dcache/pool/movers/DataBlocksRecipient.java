/*
 * ChecksumUpdateRecepient.java
 *
 * Created on March 29, 2005, 11:33 AM
 */

package org.dcache.pool.movers;

import java.io.IOException;

/**
 *
 * @author  timur
 */
public interface DataBlocksRecipient
{
    void receiveEBlock(byte[] array, int offset, int length,
                       long offsetOfArrayInFile)
        throws IOException;
}

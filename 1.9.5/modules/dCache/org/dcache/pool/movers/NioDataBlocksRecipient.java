/*
 * ChecksumUpdateRecepient.java
 *
 * Created on March 29, 2005, 11:33 AM
 */

package org.dcache.pool.movers;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author  timur
 */
public interface NioDataBlocksRecipient 
{
    public  void receiveEBlock(ByteBuffer buffer, int offset, int length,
                               long offsetOfArrayInFile) throws IOException;
}

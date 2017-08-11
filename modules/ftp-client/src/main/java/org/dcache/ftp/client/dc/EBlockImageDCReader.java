/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dcache.ftp.client.dc;

import java.io.InputStream;
import java.io.IOException;
import java.io.DataInputStream;

import org.dcache.ftp.client.Buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EBlockImageDCReader
        extends EBlockAware
        implements DataChannelReader
{

    boolean eodReceived = false;
    boolean willCloseReceived = false;

    private static final Logger logger =
            LoggerFactory.getLogger(EBlockImageDCReader.class);

    protected DataInputStream input;

    @Override
    public void setDataStream(InputStream in)
    {
        input = new DataInputStream(in);
    }

    /**
     * @return true if at least once received
     * the "server will close the connection" signal
     */
    public boolean willCloseReceived()
    {
        return willCloseReceived;
    }

    @Override
    public Buffer read() throws IOException
    {

        //EOD received in previous read
        if (eodReceived) {
            return null;
        }

        // WILL_CLOSE received in previous read
        if (willCloseReceived) {
            return null;
        }

        byte desc = input.readByte();
        long size = input.readLong();
        long offset = input.readLong();

        boolean eof = (desc & EOF) != 0;
        boolean eod = (desc & EOD) != 0;

        if (logger.isDebugEnabled()) {
            logger.debug(desc + " " + size + " " + offset);
        }

        // if closing flag not yet received,
        // check this buffer for closing flag
        willCloseReceived = (desc & WILL_CLOSE) != 0;
        if (willCloseReceived) {
            logger.debug("Received the CLOSE flag");
        }

        if (eod) {
            this.eodReceived = true;
            context.eodTransferred();
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Received EOD. Still expecting: {}",
                        ((context.getEodsTotal() == EBlockParallelTransferContext.UNDEFINED)
                           ? "?"
                           : Integer.toString(
                                context.eodsTotal - context.eodsTransferred)));
            }
        }

        if (eof) {
            context.setEodsTotal((int) offset);
            if (logger.isDebugEnabled()) {
                logger.debug("Received EODC. Expecting total EODs: {}",
                        context.getEodsTotal());
            }
            return null;

        } else {
            byte[] bt = new byte[(int) size];
            input.readFully(bt);
            return new Buffer(bt, (int) size, offset);
        }
    }

    @Override
    public void close() throws IOException
    {
        // we want to reuse the socket
        input.close();
    }
}

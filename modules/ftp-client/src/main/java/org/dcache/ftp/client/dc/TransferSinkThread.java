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

import org.dcache.ftp.client.Buffer;
import org.dcache.ftp.client.DataSink;
import org.dcache.ftp.client.vanilla.FTPServerFacade;
import org.dcache.ftp.client.vanilla.BasicServerControlChannel;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements incoming transfer.
 * While the transfer is in progress, replies are sent to the
 * local control channel. Also any failure messages go there
 * in the form of a negative reply.
 **/
public class TransferSinkThread extends TransferThread
{

    protected static final Logger logger =
            LoggerFactory.getLogger(TransferSinkThread.class);

    protected DataChannelReader reader;
    protected final DataSink sink;
    protected final BasicServerControlChannel localControlChannel;
    protected final TransferContext context;
    protected final SocketBox socketBox;

    public TransferSinkThread(AbstractDataChannel dataChannel,
                              SocketBox socketBox,
                              DataSink sink,
                              BasicServerControlChannel localControlChannel,
                              TransferContext context)
            throws Exception
    {
        this.socketBox = socketBox;
        this.sink = sink;
        this.localControlChannel = localControlChannel;
        this.context = context;
        this.reader = dataChannel.getDataChannelSource(context);
        reader.setDataStream(socketBox.getSocket().getInputStream());
    }

    @Override
    public void run()
    {
        boolean error = false;
        Object quitToken = null;
        logger.debug("TransferSinkThread executing");

        try {
            startup();

            try {
                copy();
            } catch (Exception e) {
                error = true;
                FTPServerFacade.exceptionToControlChannel(
                        e,
                        "exception during TransferSinkThread",
                        localControlChannel);
            } finally {
                // attempt to obtain permission to close resources
                quitToken = context.getQuitToken();
                shutdown(quitToken);
            }

            if (!error) {
                // local control channel is shared by all data channels
                // so only the last one exiting may send "226 transfer complete"
                if (quitToken != null) {
                    localControlChannel.write(new LocalReply(226));
                }
            }

        } catch (Exception e) {
            // exception occurred when trying to write to local
            // control channel. So there is no way to inform
            // the user.
            FTPServerFacade.cannotPropagateError(e);
        }
    }

    protected void startup() throws Exception
    {
        //send initial reply only if nothing has yet been sent
        synchronized (localControlChannel) {
            if (localControlChannel.getReplyCount() == 0) {
                // 125 Data connection already open; transfer starting
                localControlChannel.write(new LocalReply(125));
            }
        }
    }

    protected void copy() throws Exception
    {
        Buffer buf;
        long transferred = 0;

        while ((buf = reader.read()) != null) {
            transferred += buf.getLength();
            sink.write(buf);
        }
        logger.debug("finished receiving data; received {} bytes",
                     transferred);
    }

    protected void shutdown(Object quitToken) throws IOException
    {
        logger.debug("shutdown");

        reader.close();

        // garbage collect the socket
        socketBox.setSocket(null);

        // data sink is shared by all data channels,
        // so should be closed by the last one exiting
        if (quitToken != null) {
            sink.close();
        }
    }

}

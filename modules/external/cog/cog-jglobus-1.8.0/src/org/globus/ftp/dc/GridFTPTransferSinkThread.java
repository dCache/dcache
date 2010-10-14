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
package org.globus.ftp.dc;

import org.globus.ftp.DataSink;
import org.globus.ftp.vanilla.BasicServerControlChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GridFTPTransferSinkThread extends TransferSinkThread {

    protected static Log logger =
        LogFactory.getLog(GridFTPTransferSinkThread.class.getName());

    // utility alias to context
    protected EBlockParallelTransferContext eContext;

    public GridFTPTransferSinkThread(
                          AbstractDataChannel dataChannel,
                          SocketBox socketBox,
                          DataSink sink,
                          BasicServerControlChannel localControlChannel,
                          EBlockParallelTransferContext context)
        throws Exception {
        super(dataChannel, socketBox, sink, localControlChannel, context);
        this.eContext = context;
    }

    protected void startup() {
        
        //update manager's thread count
        TransferThreadManager threadManager =
            eContext.getTransferThreadManager();
        threadManager.transferThreadStarting();

        //send initial reply only if nothing has yet been sent
        synchronized(localControlChannel) {
            if (localControlChannel.getReplyCount() == 0) {
                // 125 Data connection already open; transfer starting
                localControlChannel.write(new LocalReply(125));
            }
        }
    }

    protected void shutdown(Object quitToken) throws java.io.IOException {

        SocketPool pool =
            ((EBlockParallelTransferContext) context).getSocketPool();
        
        if (((ManagedSocketBox) socketBox).isReusable()) {
            // we're in EBLOCK mode, so reader is EBlockImageDCReader
            if (! ((EBlockImageDCReader)reader).willCloseReceived()) {
                // we're in EBLOCK mode. Store the socket for later reuse
                logger.debug("shutdown; leaving the socket open");
                pool.checkIn(socketBox);
            } else {
                //the server indicated closing the connection.
                //remove the useless socket.
                logger.debug("shutdown; closing the socket");
                reader.close();

                // do not reuse the socket
                pool.remove(socketBox);
                socketBox.setSocket(null);
            }
        } else {
            // we're in stream mode or other non-eblock,
            // close the socket to indicate end of read.
            logger.debug("shutdown; closing the socket");
            reader.close();

            // do not reuse the socket
            pool.remove(socketBox);
            socketBox.setSocket(null);
        }

        // data sink is shared by all data channels,
        // so should be closed by the last one exiting
        if (quitToken != null) {
            sink.close();
        }

        //update manager's thread count
        TransferThreadManager threadManager =
            eContext.getTransferThreadManager();
        threadManager.transferThreadTerminating();
        
    }
}

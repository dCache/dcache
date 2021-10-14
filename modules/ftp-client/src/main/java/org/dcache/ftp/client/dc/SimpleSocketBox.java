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

import java.io.IOException;
import java.net.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSocketBox implements SocketBox {

    private static final Logger logger =
          LoggerFactory.getLogger(SimpleSocketBox.class);

    protected Socket socket;

    /**
     * @see SocketBox#setSocket(Socket)
     */
    @Override
    public void setSocket(Socket newSocket) {
        if (newSocket == null) {
            logger.debug("Setting socket to null");
            closeSocket();
        } else {
            logger.debug("Setting socket");
        }
        this.socket = newSocket;
    }

    @Override
    public Socket getSocket() {
        return this.socket;
    }

    private void closeSocket() {
        if (this.socket != null) {
            try {
                this.socket.close();
            } catch (IOException e) {
            }
        }
    }

}

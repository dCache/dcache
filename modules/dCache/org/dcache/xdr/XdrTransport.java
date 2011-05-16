/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package org.dcache.xdr;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 *
 * Abstraction for sending reply to clients
 *
 */
public interface XdrTransport {

    /**
     * Send data to remote end point.
     *
     * @param data
     * @throws IOException
     */
    public void send(ByteBuffer data) throws IOException;

    public ReplyQueue<Integer, RpcReply> getReplyQueue();

    /**
     * Get local end point.
     *
     * @return InetSocketAddress of local socket end point
     */
    public InetSocketAddress getLocalSocketAddress();

    /**
     * Get remote end point.
     *
     * @return InetSocketAddress of remote socket end point.
     */
    public InetSocketAddress getRemoteSocketAddress();

}

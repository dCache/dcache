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

import com.sun.grizzly.Context;
import com.sun.grizzly.Controller;
import com.sun.grizzly.ProtocolFilter;
import com.sun.grizzly.util.WorkerThread;
import java.io.IOException;

/**
 * This is a helper {@link ProtocolFilter} to store
 * used protocol (TCP/UDP/SSL) in thread context.
 *
 * As a workaround of Grizzly limitation.
 */
public class ProtocolKeeperFilter implements ProtocolFilter {

    public static final String CONNECTION_PROTOCOL = "ConnectionProtocol";

    public boolean execute(Context cntxt) throws IOException {
        Controller.Protocol protocol = cntxt.getProtocol();
        ((WorkerThread)Thread.currentThread()).getAttachment().setAttribute(CONNECTION_PROTOCOL, protocol);
        return true;
    }

    public boolean postExecute(Context cntxt) throws IOException {
        ((WorkerThread)Thread.currentThread()).getAttachment().removeAttribute(CONNECTION_PROTOCOL);
        return true;
    }

}

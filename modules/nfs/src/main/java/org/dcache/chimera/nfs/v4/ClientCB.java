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

package org.dcache.chimera.nfs.v4;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.dcache.utils.net.InetSocketAddresses;

class ClientCB {

    private final InetSocketAddress _socketAddress;
    private String _type;
    private int _program;
    private int _version;

    // TODO: make it nicer and faster
    ClientCB(String address, String type, int program)
    {
        _socketAddress = InetSocketAddresses.forUaddrString(address);
        _type = type;
        _program = program;
    }

    public int port() {
        return _socketAddress.getPort();
    }

    public int program() {
        return _program;
    }

    public int version() {
        return _version;
    }

    public InetAddress address() {
        return _socketAddress.getAddress();
    }

    public int protocol() {
        // TODO: parse type
        return 6; //TCP
    }

    @Override
    public String toString() {
        return _type + "://" + _socketAddress.getHostName()+ ":" + _socketAddress.getPort() + "/" + _program;
    }
}

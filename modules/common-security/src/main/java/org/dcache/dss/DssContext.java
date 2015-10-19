/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.dss;

import javax.security.auth.Subject;

import java.io.IOException;

/**
 * The dCache Security Services API.
 *
 * A simplified version of GSSAPI suitable for the use in various dCache doors.
 */
public interface DssContext
{
    /**
     * Initiates a handshake with the peer.
     */
    byte[] init(byte[] token) throws IOException;

    /**
     * Accepts a handshake from the peer.
     */
    byte[] accept(byte[] token) throws IOException;

    /**
     * Wraps application data to be sent as a token to the peer.
     */
    byte[] wrap(byte[] data, int offset, int len) throws IOException;

    /**
     * Unwraps a token received from a peer.
     */
    byte[] unwrap(byte[] token) throws IOException;

    /**
     * Returns the Subject established during the handshake.
     */
    Subject getSubject();

    /**
     * Returns true if the handshake has been completed and a security
     * context has been established.
     */
    boolean isEstablished();

    /**
     * Returns the name of the peer.
     */
    String getPeerName();
}

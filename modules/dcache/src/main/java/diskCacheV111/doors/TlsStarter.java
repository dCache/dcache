/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package diskCacheV111.doors;

import javax.net.ssl.SSLEngine;

import java.util.function.Consumer;

/**
 * A class that implements TlsStarter is able to trigger a TLS handshake with
 * subsequent messages being encrypted.  This approach to encrypting the
 * channel is often called StartTLS.  Usually the client requests (in
 * plain-text) that the channel switches to an encrypted mode.  The server
 * responds that this is OK.  When the client receives the OK message, it
 * initiates the TLS handshake by sending the TLS ClientHello message.
 */
public interface TlsStarter
{
    /**
     * This method provides the class with a way of triggering a TLS handshake
     * for subsequent traffic.  It is expected that the TlsStarter class will
     * send a reply to the client <em>after</em> calling the {startTls#accept}
     * method.  This reply will be sent in plain-text.  The client, on receiving
     * the reply, should initiate the TLS handshake by sending the TLS
     * ClientHello message.
     * <p>
     * The supplied SSLEngine instance should be configured but unused instance.
     * The framework will call {@code SSLEngine.setUseClientMode(false)} to
     * ensure the engine will respond as a server to the handshake.
     * @param startTls indicate that the client will initiate TLS handshake.
     */
    void setTlsStarter(Consumer<SSLEngine> startTls);
}

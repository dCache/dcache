/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2025 Deutsches Elektronen-Synchrotron
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
package org.dcache.webdav.transfer;

import com.google.common.net.InetAddresses;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chooses the local address for the connection a door makes to a remote server when it proxies a
 * third-party transfer.  Using the address on which the client connected to the door keeps the
 * transfer on the network (or network interface) that the client uses.
 */
final class ProxiedTransferSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxiedTransferSource.class);

    private ProxiedTransferSource() {
        // Utility class
    }

    /**
     * Select the local address to use.
     *
     * @param requestLocalAddress the address the client connected to, as a literal IP address
     * @param remoteHost          the name or address of the remote server
     * @return the address to bind to, or empty if the operating system should choose.
     */
    static Optional<InetAddress> choose(String requestLocalAddress, String remoteHost) {
        InetAddress local;
        try {
            local = InetAddresses.forString(requestLocalAddress);
        } catch (IllegalArgumentException e) {
            LOGGER.debug("Not binding: request local address \"{}\" is not an IP address",
                  requestLocalAddress);
            return Optional.empty();
        }

        if (local.isAnyLocalAddress()) {
            return Optional.empty();
        }

        try {
            if (NetworkInterface.getByInetAddress(local) == null) {
                // For example, a virtual address of a load balancer.
                LOGGER.debug("Not binding: {} is not an address of this host", local);
                return Optional.empty();
            }
        } catch (SocketException e) {
            LOGGER.debug("Not binding: cannot check whether {} is local: {}", local,
                  e.toString());
            return Optional.empty();
        }

        try {
            for (InetAddress remote : InetAddress.getAllByName(remoteHost)) {
                if (remote.getClass() == local.getClass()) {
                    return Optional.of(local);
                }
            }
        } catch (UnknownHostException e) {
            return Optional.empty();
        }

        LOGGER.debug("Not binding: {} has no address in the same IP family as {}", remoteHost,
              local);
        return Optional.empty();
    }
}

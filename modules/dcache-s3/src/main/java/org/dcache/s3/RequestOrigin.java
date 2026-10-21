/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.s3;

import static java.util.Objects.requireNonNull;

import java.net.InetAddress;
import java.util.Optional;

/**
 * The address of the client whose request the current thread serves.
 * <p>
 * The s3proxy {@link org.gaul.s3proxy.BlobStoreLocator} is not given the request, so the door
 * binds the client address to the thread for the duration of the request.
 */
public final class RequestOrigin {

    private static final ThreadLocal<InetAddress> CLIENT = new ThreadLocal<>();

    private RequestOrigin() {
    }

    /**
     * Binds the client address to the current thread until the returned scope is closed.
     */
    public static Scope bind(InetAddress address) {
        CLIENT.set(requireNonNull(address));
        return CLIENT::remove;
    }

    /**
     * Returns the client address bound to the current thread, if any.
     */
    public static Optional<InetAddress> current() {
        return Optional.ofNullable(CLIENT.get());
    }

    /**
     * Ends the binding of a client address.
     */
    @FunctionalInterface
    public interface Scope extends AutoCloseable {

        @Override
        void close();
    }
}

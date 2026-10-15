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

import java.util.function.Supplier;
import org.gaul.s3proxy.blobstore.BlobStore;

/**
 * Creates the blob store that serves one S3 request.
 */
@FunctionalInterface
public interface S3BlobStoreFactory {

    /**
     * Returns a blob store acting as the user that {@code user} logs in.
     * <p>
     * The blob store must not call {@code user} before its first operation: the S3 handler asks
     * for the blob store before it has verified the request signature.  {@code user} throws an
     * {@link org.gaul.s3proxy.S3ProxyException} if the login fails.
     *
     * @param user logs in the client on first call; later calls return the same user
     */
    BlobStore create(Supplier<S3User> user);
}

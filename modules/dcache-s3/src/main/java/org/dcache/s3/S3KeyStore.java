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

import java.util.Optional;

/**
 * Source of S3 secret keys.  The S3 door needs the plain-text secret of an access key to verify a
 * request signature.  Which dCache user an access key belongs to is decided by gPlazma, not by the
 * key store.
 */
public interface S3KeyStore {

    /**
     * Returns the secret key for an access key ID.
     *
     * @param accessKey the access key ID from the request
     * @return the secret, or empty if the access key is unknown or the key store is unusable
     */
    Optional<String> secretFor(String accessKey);
}

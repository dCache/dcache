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

import diskCacheV111.util.FsPath;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;

/**
 * A logged-in S3 client: the dCache identity its requests run as.
 *
 * @param accessKey   the access key ID the client signed the request with
 * @param subject     the identity gPlazma mapped the access key to
 * @param restriction the restrictions gPlazma placed on the identity
 * @param root        the directory whose subdirectories are the client's buckets
 */
public record S3User(String accessKey, Subject subject, Restriction restriction, FsPath root) {

    public S3User {
        requireNonNull(accessKey);
        requireNonNull(subject);
        requireNonNull(restriction);
        requireNonNull(root);
    }
}

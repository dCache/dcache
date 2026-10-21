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

import com.google.common.base.Splitter;
import diskCacheV111.util.FsPath;
import java.util.Optional;
import org.gaul.s3proxy.blobstore.S3Exceptions;

/**
 * Maps S3 buckets and keys to the dCache namespace.
 * <p>
 * A bucket is a directory below the user's bucket root; a key is a path below the bucket, with
 * {@code /} separating directories.  Keys that have no such path -- empty segments, {@code .} or
 * {@code ..} segments, a trailing {@code /} -- are refused, as are names the door reserves for
 * itself; listings hide the reserved names.
 */
final class S3Paths {

    /**
     * The prefix of files an upload writes to before they are renamed to their key.
     */
    static final String UPLOAD_PREFIX = ".s3-tmp-";

    /**
     * The directory in a bucket that holds multipart uploads in progress.
     */
    static final String MULTIPART_DIRECTORY = ".s3-multipart";

    private static final Splitter SEGMENTS = Splitter.on('/');

    private S3Paths() {
    }

    /**
     * Whether a directory below {@code root} is shown as a bucket.  S3 bucket names never start
     * with a dot, so hidden directories are not buckets.
     */
    static boolean isBucketName(String name) {
        return !name.isEmpty() && !name.startsWith(".") && name.indexOf('/') == -1;
    }

    /**
     * Whether a file or directory name is reserved by the door and hidden from clients.
     */
    static boolean isReserved(String name) {
        return name.startsWith(UPLOAD_PREFIX) || name.equals(MULTIPART_DIRECTORY);
    }

    /**
     * Returns the directory of a bucket.
     *
     * @throws software.amazon.awssdk.services.s3.model.NoSuchBucketException if the name is not a
     *                                                                         bucket name
     */
    static FsPath bucket(FsPath root, String bucket) {
        if (!isBucketName(bucket)) {
            throw S3Exceptions.noSuchBucket(bucket, "not a bucket name");
        }
        return root.child(bucket);
    }

    /**
     * Returns the file of an object.
     *
     * @throws software.amazon.awssdk.services.s3.model.S3Exception InvalidArgument if the key has
     *                                                              no path in dCache
     */
    static FsPath object(FsPath bucket, String key) {
        FsPath path = bucket;
        for (String segment : SEGMENTS.split(key)) {
            if (!isValidSegment(segment)) {
                throw S3Exceptions.invalidArgument(
                      "Object keys must be paths without empty, '.' or '..' segments");
            }
            path = path.child(segment);
        }
        return path;
    }

    /**
     * Returns the directory that holds the keys starting with {@code directoryKey}.
     *
     * @param directoryKey empty, or a key prefix ending with {@code /}
     * @return the directory, or empty if no key can start with {@code directoryKey}
     */
    static Optional<FsPath> directory(FsPath bucket, String directoryKey) {
        if (directoryKey.isEmpty()) {
            return Optional.of(bucket);
        }
        FsPath path = bucket;
        String withoutSlash = directoryKey.substring(0, directoryKey.length() - 1);
        for (String segment : SEGMENTS.split(withoutSlash)) {
            if (!isValidSegment(segment)) {
                return Optional.empty();
            }
            path = path.child(segment);
        }
        return Optional.of(path);
    }

    private static boolean isValidSegment(String segment) {
        return !segment.isEmpty() && !segment.equals(".") && !segment.equals("..")
              && !isReserved(segment);
    }
}

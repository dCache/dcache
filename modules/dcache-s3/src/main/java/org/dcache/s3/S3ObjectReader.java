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

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import java.io.InputStream;
import org.dcache.vehicles.FileAttributes;

/**
 * Reads the content of a file from a pool.
 */
@FunctionalInterface
public interface S3ObjectReader {

    /**
     * Starts a transfer of part of a file and returns its data.  Closing the stream ends the
     * transfer, whether or not all data was read.
     *
     * @param user       the user reading the file
     * @param path       the file
     * @param attributes the file's attributes, as the blob store fetched them
     * @param offset     the first byte to read
     * @param length     the number of bytes to read
     */
    InputStream read(S3User user, FsPath path, FileAttributes attributes, long offset,
          long length) throws CacheException, InterruptedException;
}

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
import diskCacheV111.util.PnfsId;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Writes a new file to a pool.
 */
@FunctionalInterface
public interface S3ObjectWriter {

    /**
     * Creates a file, creating missing parent directories, and writes data to it.  Returns once
     * the pool has confirmed the transfer.  If writing fails, the file is removed.
     *
     * @param user        the user writing the file
     * @param path        the new file; it must not exist
     * @param billingPath the path billing records for the transfer, as {@code path} may be a
     *                    temporary name
     * @param xattrs      extended attributes of the new file
     * @param data        the content; exactly {@code length} bytes are read from it
     * @param length      the size of the file
     * @return the PNFS-ID of the new file
     * @throws IOException if reading {@code data} fails
     */
    PnfsId write(S3User user, FsPath path, FsPath billingPath, Map<String, String> xattrs,
          InputStream data, long length)
          throws CacheException, InterruptedException, IOException;
}

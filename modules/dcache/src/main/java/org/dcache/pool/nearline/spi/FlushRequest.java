/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.nearline.spi;

import java.io.File;
import java.net.URI;
import java.util.Set;

import org.dcache.vehicles.FileAttributes;

/**
 * A request to flush a file to nearline storage.
 *
 * The result of a flush request is one or more URIs that identify
 * the flushed file.
 */
public interface FlushRequest extends NearlineRequest<Set<URI>>
{
    /**
     * A local file system path to the replica to flush.
     *
     * @return A file system path
     */
    File getFile();

    /**
     * Attributes of the file to which this request applies.
     *
     * @return Attributes of the file
     */
    FileAttributes getFileAttributes();
}

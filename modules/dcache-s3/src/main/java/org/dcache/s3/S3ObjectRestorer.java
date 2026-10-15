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
import org.dcache.vehicles.FileAttributes;

/**
 * Brings files whose only copy is on tape back to disk, for S3 RestoreObject.
 */
public interface S3ObjectRestorer {

    /**
     * Whether a file is being brought back to disk.
     */
    boolean isRestoring(S3User user, FileAttributes attributes)
          throws CacheException, InterruptedException;

    /**
     * Starts bringing a file back to disk and keeps it there for a number of days.  Returns once
     * the restore has started, not when it has finished.
     *
     * @param days how long the file should stay on disk; implementations may shorten it
     */
    void restore(S3User user, FsPath path, FileAttributes attributes, int days)
          throws CacheException, InterruptedException;
}

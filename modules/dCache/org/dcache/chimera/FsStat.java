/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera;

/**
 * File system stat information.
 *
 */

/* Immutable */
public class FsStat {

    private final long _totalSpace;
    private final long _totalFiles;
    private final long _usedSpace;
    private final long _usedFiles;

    public FsStat(long totalSpace, long totalFiles, long usedSpace, long usedFiles) {
        _totalSpace = totalSpace;
        _totalFiles = totalFiles;
        _usedSpace = usedSpace;
        _usedFiles = usedFiles;
    }

    public long getTotalFiles() {
        return _totalFiles;
    }

    public long getTotalSpace() {
        return _totalSpace;
    }

    /**
     *
     * @return total number of files. If a file has multiple replicas only one
     * replica is counted.
     */
    public long getUsedFiles() {
        return _usedFiles;
    }

    /**
     *
     * @return total number of bytes off all files. If a file has a multiple
     * replicas only one replica is counted.
     */
    public long getUsedSpace() {
        return _usedSpace;
    }
}

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryStreamHelper {

    private static final Logger _log = LoggerFactory.getLogger(DirectoryStreamHelper.class);

    /**
     * Convert directory stream into a {@link List}.
     * @param inode of a directory to be listed
     * @return a list of {@link HimeraDirectoryEntry}
     * @throws IOException
     */
    public static List<HimeraDirectoryEntry> listOf(FsInode inode) throws IOException, IOHimeraFsException {

        List<HimeraDirectoryEntry> directoryList;

        int estimatedListSize = inode.statCache().getNlink();
        if (estimatedListSize < 0) {
            _log.error("Invalid nlink count for directory {}", inode);
            directoryList = new ArrayList<>();
        } else {
            directoryList = new ArrayList<>(estimatedListSize);
        }

        try (DirectoryStreamB<HimeraDirectoryEntry> dirStream =
                inode.newDirectoryStream()) {
            for (HimeraDirectoryEntry e : dirStream) {
                directoryList.add(e);
            }
        }

        return directoryList;
    }
}

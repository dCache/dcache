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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryStreamHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryStreamHelper.class);

    /**
     * Returns a {@link Stream} of {@link HimeraDirectoryEntry} in the directory {@code inode}.
     * <p>
     * After this method returns, then any subsequent I/O exception that occurs while listing the
     * directory is wrapped in an UncheckedIOException.
     * <p>
     * The returned stream keeps a db Connection. The try-with-resources construct should be used to
     * ensure that the stream's close method is invoked after the stream operations are completed.
     *
     * @param inode of a directory to be listed
     * @return a stream of {@link HimeraDirectoryEntry}
     * @throws IOException
     */
    public static Stream<HimeraDirectoryEntry> streamOf(FsInode inode)
          throws IOException, IOHimeraFsException {
        DirectoryStreamB<HimeraDirectoryEntry> listStream = inode.newDirectoryStream();
        return listStream.stream().onClose(uncheckedRunnable(listStream));
    }

    private static Runnable uncheckedRunnable(Closeable closeable) {
        return () -> {
            try {
                closeable.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}

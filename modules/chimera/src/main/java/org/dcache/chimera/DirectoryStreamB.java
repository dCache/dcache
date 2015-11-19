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
import java.util.Iterator;

/**
 * An object to iterate over the entries in a directory. A directory stream
 * allows for convenient use of the for-each construct:
 * <pre>
 *   Path dir = ...
 *   DirectoryStream&lt;Path&gt; stream = dir.newDirectoryStream();
 *   try {
 *       for (Path entry: stream) {
 *         ..
 *       }
 *   } finally {
 *       stream.close();
 *   }
 * </pre>
 *
 * This is a backport of the JDK 7 interface.
 */
public interface DirectoryStreamB<T> extends Closeable, Iterable<T> {

    interface Filter<T> {

        /**
         * Decides if the given directory entry should be accepted or filtered.
         *
         * @param   entry
         *          the directory entry to be tested
         *
         * @return  {@code true} if the directory entry should be accepted
         */
        boolean accept(T entry);
    }

    @Override
    Iterator<T> iterator();
}

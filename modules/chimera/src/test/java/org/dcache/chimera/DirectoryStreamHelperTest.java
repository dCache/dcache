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
import java.util.Collections;
import java.util.Iterator;
import org.dcache.chimera.posix.Stat;
import org.junit.Test;
import static org.mockito.Mockito.*;
import static org.mockito.BDDMockito.given;

public class DirectoryStreamHelperTest {

    @Test
    public void testNegativeNlink() throws ChimeraFsException, IOException {

        FsInode inode = mock(FsInode.class);
        Stat stat = mock(Stat.class);

        given(stat.getNlink()).willReturn(-7);
        given(inode.stat()).willReturn(stat);
        given(inode.statCache()).willReturn(stat);
        given(inode.newDirectoryStream()).willReturn( new DirectoryStreamB<HimeraDirectoryEntry>() {

            @Override
            public Iterator<HimeraDirectoryEntry> iterator() {
                return Collections.emptyIterator();
            }

            @Override
            public void close() throws IOException {}
        });
        DirectoryStreamHelper.listOf(inode);
    }

}

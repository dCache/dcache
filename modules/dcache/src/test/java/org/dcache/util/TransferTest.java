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
package org.dcache.util;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.GenericStorageInfo;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileType;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.junit.Test;

public class TransferTest {

    @Test
    public void shouldReadNamespaceEntryWithoutCellAddress() throws Exception {
        CellStub stub = mock(CellStub.class);
        when(stub.send(any(PnfsGetFileAttributes.class), anyLong())).thenAnswer(invocation -> {
            PnfsGetFileAttributes request = invocation.getArgument(0);
            PnfsGetFileAttributes reply =
                  new PnfsGetFileAttributes(request.getPnfsPath(),
                        request.getRequestedAttributes());
            FileAttributes attributes = FileAttributes.of()
                  .fileType(FileType.REGULAR)
                  .size(1L)
                  .storageInfo(new GenericStorageInfo())
                  .build();
            reply.setFileAttributes(attributes);
            return immediateFuture(reply);
        });

        Transfer transfer = new Transfer(new PnfsHandler(stub), new Subject(),
              Restrictions.none(), FsPath.create("/test/path"));

        transfer.readNameSpaceEntry(false);

        verify(stub).send(any(PnfsGetFileAttributes.class), anyLong());
    }
}

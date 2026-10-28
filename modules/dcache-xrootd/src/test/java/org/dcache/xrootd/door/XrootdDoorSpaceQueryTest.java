package org.dcache.xrootd.door;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.GenericStorageInfo;
import java.net.InetSocketAddress;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.space.Capacity;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.junit.Test;

public class XrootdDoorSpaceQueryTest {

    @Test
    public void shouldPassStorageInfoOfQueriedPathAndClient() throws Exception {
        CellStub pnfs = mock(CellStub.class);
        when(pnfs.send(any(PnfsGetFileAttributes.class), anyLong())).thenAnswer(invocation -> {
            PnfsGetFileAttributes request = invocation.getArgument(0);
            GenericStorageInfo storageInfo = new GenericStorageInfo();
            storageInfo.setKey("writeToken", "42");
            FileAttributes attributes = new FileAttributes();
            attributes.setStorageInfo(storageInfo);
            request.setFileAttributes(attributes);
            return Futures.immediateFuture(request);
        });
        Capacity.Provider provider = mock(Capacity.Provider.class);
        XrootdDoor door = new XrootdDoor();
        door.setPnfsHandler(new PnfsHandler(pnfs));
        door.setCapacity(provider);

        door.getCapacity(FsPath.create("/data/cms"), Subjects.ROOT, Restrictions.none(),
              new InetSocketAddress("192.0.2.1", 1094));

        verify(provider).get(
              argThat(a -> "42".equals(a.getStorageInfo().getMap().get("writeToken"))),
              eq("192.0.2.1"), eq("Xrootd/" + XrootdProtocol.PROTOCOL_VERSION_MAJOR));
    }

    @Test
    public void shouldFormatSpaceInfoLikeXrootdOss() {
        long size = 1L << 60;
        assertThat(XrootdRedirectHandler.toSpaceInfo(Capacity.UNKNOWN),
              is("oss.cgroup=public&oss.space=" + size + "&oss.free=" + size
                    + "&oss.maxf=" + size + "&oss.used=0&oss.quota=" + size));
    }
}

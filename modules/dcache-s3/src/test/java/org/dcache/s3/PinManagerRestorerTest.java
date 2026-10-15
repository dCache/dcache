package org.dcache.s3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.HttpProtocolInfo;
import dmg.cells.nucleus.CellAddressCore;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.pinmanager.PinManagerListPinsMessage;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class PinManagerRestorerTest {

    private static final PnfsId PNFSID = new PnfsId("0000A1B2C3D4E5F60718293A4B5C6D7E8F90");
    private static final FsPath PATH = FsPath.create("/data/s3/bucket/file");

    private CellStub pinManager;
    private FileAttributes namespaceAttributes;
    private S3User user;
    private PinManagerRestorer restorer;

    @Before
    public void setup() {
        Subject subject = new Subject();
        subject.getPrincipals().add(new UidPrincipal(1000));
        user = new S3User("AKIAEXAMPLE", subject, Restrictions.readOnly(),
              FsPath.create("/data/s3"));
        namespaceAttributes = FileAttributes.of().pnfsId(PNFSID).size(5).build();

        CellStub pnfsStub = mock(CellStub.class);
        when(pnfsStub.send(any(PnfsGetFileAttributes.class), anyLong())).thenAnswer(i -> {
            PnfsGetFileAttributes msg = i.getArgument(0);
            msg.setFileAttributes(namespaceAttributes);
            return Futures.immediateFuture(msg);
        });

        pinManager = mock(CellStub.class);

        restorer = new PinManagerRestorer();
        restorer.setPnfsHandler(new PnfsHandler(pnfsStub));
        restorer.setPinManagerStub(pinManager);
        restorer.setCellAddress(new CellAddressCore("S3-door", "s3Domain"));
        restorer.setMaxDays(30);
    }

    @Test
    public void shouldReportPinBeingEstablishedAsRestoring() throws Exception {
        givenPins(PinManagerListPinsMessage.State.PINNED, PinManagerListPinsMessage.State.PINNING);

        assertThat(restorer.isRestoring(user, namespaceAttributes), is(true));
    }

    @Test
    public void shouldNotReportEstablishedPinAsRestoring() throws Exception {
        givenPins(PinManagerListPinsMessage.State.PINNED);

        assertThat(restorer.isRestoring(user, namespaceAttributes), is(false));
    }

    @Test
    public void shouldPinForRequestedDays() throws Exception {
        PinManagerPinMessage pin = restore(7);

        assertThat(pin.getLifetime(), is(TimeUnit.DAYS.toMillis(7)));
        assertThat(pin.isReplyWhenStarted(), is(true));
        assertThat(pin.getRequestId(), is("s3:AKIAEXAMPLE"));
        assertThat(pin.getPnfsId(), is(PNFSID));
        assertThat(pin.getSubject(), is(equalTo(user.subject())));
        assertThat(pin.getRestriction(), is(Restrictions.readOnly()));
        var protocolInfo = (HttpProtocolInfo) pin.getProtocolInfo();
        assertThat(protocolInfo.getSocketAddress().getAddress(),
              is(InetAddress.getByName("192.0.2.1")));
    }

    @Test
    public void shouldCapPinLifetime() throws Exception {
        PinManagerPinMessage pin = restore(365);

        assertThat(pin.getLifetime(), is(TimeUnit.DAYS.toMillis(30)));
    }

    @Test
    public void shouldNotPinReplacedFile() throws Exception {
        FileAttributes requested = FileAttributes.of()
              .pnfsId(new PnfsId("0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")).build();

        CacheException e = assertThrows(CacheException.class,
              () -> restorer.restore(user, PATH, requested, 1));

        assertThat(e.getRc(), is(CacheException.FILE_NOT_FOUND));
        verify(pinManager, never()).sendAndWait(any(PinManagerPinMessage.class));
    }

    private PinManagerPinMessage restore(int days) throws Exception {
        when(pinManager.sendAndWait(any(PinManagerPinMessage.class)))
              .thenAnswer(i -> i.getArgument(0));
        try (var ignored = RequestOrigin.bind(InetAddress.getByName("192.0.2.1"))) {
            restorer.restore(user, PATH, namespaceAttributes, days);
        }
        ArgumentCaptor<PinManagerPinMessage> pin =
              ArgumentCaptor.forClass(PinManagerPinMessage.class);
        verify(pinManager).sendAndWait(pin.capture());
        return pin.getValue();
    }

    private void givenPins(PinManagerListPinsMessage.State... states) throws Exception {
        List<PinManagerListPinsMessage.Info> pins = new ArrayList<>();
        for (var state : states) {
            var pin = mock(PinManagerListPinsMessage.Info.class);
            when(pin.getState()).thenReturn(state);
            pins.add(pin);
        }
        when(pinManager.sendAndWait(any(PinManagerListPinsMessage.class))).thenAnswer(i -> {
            PinManagerListPinsMessage msg = i.getArgument(0);
            msg.setInfo(pins);
            return msg;
        });
    }
}

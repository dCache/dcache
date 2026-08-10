package org.dcache.pool.classic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;
import diskCacheV111.util.PnfsId;

import java.lang.reflect.Array;
import java.net.InetAddress;
import java.util.Set;
import javax.security.auth.Subject;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;
import org.dcache.auth.UidPrincipal;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.json.MoverData;
import org.dcache.util.IoPriority;
import org.dcache.vehicles.FileAttributes;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.junit.Before;
import org.junit.Test;
import org.python.antlr.op.Or;

public class MoverRequestSchedulerTest {

    private MoverRequestScheduler scheduler;
    private PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");

    @Before
    public void setUp() {
        scheduler = new MoverRequestScheduler("test-queue", 0, MoverRequestScheduler.Order.FIFO);
    }

    @Test
    public void shouldCountNonP2PRequests() throws Exception {
        addMover(pnfsId, false);
        addMover(pnfsId, false);

        assertEquals(2, scheduler.numberOfRequestsFor(pnfsId));
    }

    @Test
    public void shouldNotCountP2PRequests() throws Exception {
        addMover(pnfsId, true);
        addMover(pnfsId, false);

        assertEquals(1, scheduler.numberOfRequestsFor(pnfsId));
    }

    @Test
    public void shouldReturnZeroWhenNoRequests() {
        assertEquals(0, scheduler.numberOfRequestsFor(pnfsId));
    }

    @Test
    public void shouldNotCountRequestsForDifferentPnfsId() throws Exception {
        PnfsId otherPnfsId = new PnfsId("000000000000000000000000000000000002");
        addMover(otherPnfsId, false);

        assertEquals(0, scheduler.numberOfRequestsFor(pnfsId));
    }

    @Test
    public void shouldIncludeDnInUserDataForX509Subject() throws Exception {
        String dn = "dnstring";
        Subject subject = new Subject();
        subject.getPrincipals().add(new GlobusPrincipal(dn));
        subject.getPrincipals().add(new UidPrincipal(1000));
        subject.getPrincipals().add(new GidPrincipal(1000, true));
        subject.getPrincipals().add(new GidPrincipal(2000, true));
        subject.getPrincipals().add(new Origin(InetAddress.getByName("192.168.1.1")));

        addMover(pnfsId, false, subject);

        long[] gids = new long[1];
        gids[0] = 1000;

        MoverData data = scheduler.getMoverData(x -> true, MoverData::compareTo).get(0);
        assertEquals(dn, data.getUserData().getDn());
        assertEquals(Long.valueOf(1000), data.getUserData().getUserId());
        assertEquals(gids[0], data.getUserData().getGroupIds()[0]);
    }

    @Test
    public void shouldHaveNullDnInUserDataForBasicAuthSubject() throws Exception {
        Subject subject = new Subject();
        subject.getPrincipals().add(new UidPrincipal(1000));
        subject.getPrincipals().add(new GidPrincipal(1000, true));
        subject.getPrincipals().add(new GidPrincipal(2000, true));
        subject.getPrincipals().add(new Origin(InetAddress.getByName("192.168.1.1")));

        addMover(pnfsId, false, subject);

        long[] gids = new long[1];
        gids[0] = 1000;

        MoverData data = scheduler.getMoverData(x -> true, MoverData::compareTo).get(0);
        assertNull(data.getUserData().getDn());
        assertEquals(Long.valueOf(1000), data.getUserData().getUserId());
        assertEquals(gids[0], data.getUserData().getGroupIds()[0]);
    }

    @Test
    public void shouldHaveNullUserDataFieldsForAnonymousSubject() throws Exception {
        Subject subject = new Subject();
        subject.getPrincipals().add(new Origin(InetAddress.getByName("192.168.1.1")));
        addMover(pnfsId, false, subject);

        MoverData data = scheduler.getMoverData(x -> true, MoverData::compareTo).get(0);
        assertNull(data.getUserData().getUserId());
        assertNull(data.getUserData().getGroupIds());
    }

    private void addMover(PnfsId pnfsId, boolean isP2P) throws Exception {
        addMover(pnfsId, isP2P, new Subject());
    }

    private void addMover(PnfsId pnfsId, boolean isP2P, Subject subject) throws Exception {
        Mover mover = mock(Mover.class);
        FileAttributes attributes = new FileAttributes();
        attributes.setPnfsId(pnfsId);
        attributes.setStorageClass("teststore");
        when(mover.getFileAttributes()).thenReturn(attributes);
        when(mover.isPoolToPoolTransfer()).thenReturn(isP2P);
        when(mover.getSubject()).thenReturn(subject);
        when(mover.getQueueName()).thenReturn("regular");
        when(mover.getIoMode()).thenReturn(Set.of());
        when(mover.getPathToDoor()).thenReturn(new CellPath(new CellAddressCore("door", "domain")));

        MoverSupplier supplier = mock(MoverSupplier.class);
        when(supplier.createMover()).thenReturn(mover);

        scheduler.getOrCreateMover(supplier, "door-" + System.nanoTime(), IoPriority.REGULAR);
    }
}

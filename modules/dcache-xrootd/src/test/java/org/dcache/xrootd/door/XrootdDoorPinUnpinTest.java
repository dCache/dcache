/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
*/
package org.dcache.xrootd.door;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.ServiceUnavailableException;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.pinmanager.PinManagerUnpinMessage;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link XrootdDoor#pin} and {@link XrootdDoor#unpin}, the methods backing the
 * xrootd "prepare" (stage/pin, evict/cancel-unpin) support.
 */
public class XrootdDoorPinUnpinTest {

    private static final PnfsId PNFS_ID = new PnfsId("000000000000000000000000000000000001");
    private static final FsPath PATH = FsPath.create("/data/file");
    private static final InetSocketAddress CLIENT = new InetSocketAddress("127.0.0.1", 1094);

    private CellStub pnfsCellStub;
    private CellStub pinManagerStub;
    private XrootdDoor door;
    private Subject subject;

    @Before
    public void setUp() {
        pnfsCellStub = mock(CellStub.class);
        pinManagerStub = mock(CellStub.class);

        door = new XrootdDoor();
        door.setCellAddress(new CellAddressCore("xrootd", "local"));
        door.setPnfsHandler(new PnfsHandler(pnfsCellStub));
        door.setPinManagerStub(pinManagerStub);

        subject = Subjects.of(1000, 1000, new int[]{1000});
    }

    /**
     * Stubs the mocked PNFS CellStub so that any {@link PnfsGetFileAttributes} request is
     * answered with a PNFSID plus, if RETENTION_POLICY was requested, the given retention policy
     * (mimicking what {@link XrootdDoor#pin} needs to make its CUSTODIAL check).
     */
    private void stubFileAttributes(RetentionPolicy retentionPolicy) {
        when(pnfsCellStub.send(any(PnfsGetFileAttributes.class), anyLong()))
              .thenAnswer(invocation -> {
                  PnfsGetFileAttributes request = invocation.getArgument(0);
                  FileAttributes attributes = new FileAttributes();
                  attributes.setPnfsId(PNFS_ID);
                  if (request.getRequestedAttributes().contains(FileAttribute.RETENTION_POLICY)) {
                      attributes.setRetentionPolicy(retentionPolicy);
                      attributes.setAccessLatency(AccessLatency.NEARLINE);
                      attributes.setOwner(0);
                      attributes.setGroup(0);
                      attributes.setFileType(FileType.REGULAR);
                  }
                  request.setFileAttributes(attributes);
                  return Futures.immediateFuture(request);
              });
    }

    @Test
    public void pinSendsPinMessageForCustodialFile() throws Exception {
        stubFileAttributes(RetentionPolicy.CUSTODIAL);

        door.pin(new FsPath[]{PATH}, CLIENT, subject, Restrictions.none());

        ArgumentCaptor<PinManagerPinMessage> captor =
              ArgumentCaptor.forClass(PinManagerPinMessage.class);
        verify(pinManagerStub).sendAndWait(captor.capture());

        PinManagerPinMessage message = captor.getValue();
        assertThat(message.getPnfsId(), is(PNFS_ID));
        assertThat(message.getRequestId(), is("1000"));
        assertThat(message.getLifetime(), is(TimeUnit.HOURS.toMillis(12)));
        assertThat(message.isReplyWhenStarted(), is(true));
    }

    @Test
    public void pinSkipsNonCustodialFile() throws Exception {
        stubFileAttributes(RetentionPolicy.REPLICA);

        door.pin(new FsPath[]{PATH}, CLIENT, subject, Restrictions.none());

        verify(pinManagerStub, never()).sendAndWait(any(PinManagerPinMessage.class));
    }

    @Test(expected = ServiceUnavailableException.class)
    public void pinWrapsNoRouteToCellExceptionAsServiceUnavailable() throws Exception {
        stubFileAttributes(RetentionPolicy.CUSTODIAL);
        when(pinManagerStub.sendAndWait(any(PinManagerPinMessage.class)))
              .thenThrow(noRouteToCellException());

        door.pin(new FsPath[]{PATH}, CLIENT, subject, Restrictions.none());
    }

    @Test(expected = PermissionDeniedCacheException.class)
    public void pinRejectsAnonymousSubject() throws Exception {
        stubFileAttributes(RetentionPolicy.CUSTODIAL);

        door.pin(new FsPath[]{PATH}, CLIENT, Subjects.NOBODY, Restrictions.none());
    }

    @Test
    public void unpinSendsUnpinMessageWithRequestId() throws Exception {
        stubFileAttributes(RetentionPolicy.CUSTODIAL);

        door.unpin(new FsPath[]{PATH}, subject, Restrictions.none());

        ArgumentCaptor<PinManagerUnpinMessage> captor =
              ArgumentCaptor.forClass(PinManagerUnpinMessage.class);
        verify(pinManagerStub).sendAndWait(captor.capture());

        PinManagerUnpinMessage message = captor.getValue();
        assertThat(message.getPnfsId(), is(PNFS_ID));
        assertThat(message.getRequestId(), is("1000"));
    }

    @Test(expected = ServiceUnavailableException.class)
    public void unpinWrapsNoRouteToCellExceptionAsServiceUnavailable() throws Exception {
        stubFileAttributes(RetentionPolicy.CUSTODIAL);
        when(pinManagerStub.sendAndWait(any(PinManagerUnpinMessage.class)))
              .thenThrow(noRouteToCellException());

        door.unpin(new FsPath[]{PATH}, subject, Restrictions.none());
    }

    @Test(expected = PermissionDeniedCacheException.class)
    public void unpinRejectsAnonymousSubject() throws Exception {
        stubFileAttributes(RetentionPolicy.CUSTODIAL);

        door.unpin(new FsPath[]{PATH}, Subjects.NOBODY, Restrictions.none());
    }

    /**
     * Builds a {@link NoRouteToCellException}, which requires a real {@link CellMessage}
     * envelope rather than a mock.
     */
    private static NoRouteToCellException noRouteToCellException() {
        CellMessage envelope = new CellMessage(new CellPath("pinmanager"), "noop");
        return new NoRouteToCellException(envelope, "no route");
    }
}

package org.dcache.restful.resources.namespace;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.Futures;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import dmg.cells.nucleus.CellAddressCore;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.NotFoundException;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.LoginAttributes;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.auth.attributes.Role;
import org.dcache.cells.CellStub;
import org.dcache.http.AuthenticationHandler;
import org.dcache.http.PathMapper;
import org.dcache.namespace.FileType;
import org.dcache.restful.util.RequestUser;
import org.dcache.vehicles.FileAttributes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Tests that deleting through the REST API sends a billing {@code remove} record for files and
 * links, but not for directories or failed deletes.
 */
@RunWith(MockitoJUnitRunner.class)
public class FileResourcesTest {

    private static final FsPath PATH = FsPath.create("/data/file");
    private static final PnfsId PNFSID = new PnfsId("000000000000000000000000000000000001");
    private static final CellAddressCore CELL_ADDRESS =
          new CellAddressCore("Frontend-test", "frontendDomain");

    @Mock
    private HttpServletRequest request;

    @Mock
    private PathMapper pathMapper;

    @Mock
    private CellStub pnfsmanager;

    @Mock
    private CellStub billingTopic;

    @InjectMocks
    private FileResources resources;

    @InjectMocks
    private RequestUser requestUser;

    private final AtomicReference<PnfsDeleteEntryMessage> sentDelete = new AtomicReference<>();

    private Subject user;

    @Before
    public void setUp() throws Exception {
        user = new Subject();
        user.getPrincipals().add(new UidPrincipal(1000));
        user.getPrincipals().add(new Origin(ImmutableList.of(
              InetAddresses.forString("192.0.2.10"), InetAddresses.forString("198.51.100.20"))));

        resources.setCellAddress(CELL_ADDRESS);
        when(pathMapper.asDcachePath(eq(request), anyString(), any(), any())).thenReturn(PATH);
        when(request.getAttribute(AuthenticationHandler.DCACHE_RESTRICTION_ATTRIBUTE))
              .thenReturn(Restrictions.none());
        lenient().when(request.getAttribute(AuthenticationHandler.DCACHE_LOGIN_ATTRIBUTES))
              .thenReturn(Collections.emptySet());
    }

    @After
    public void tearDown() throws Exception {
        requestUser.filter(null, null);
    }

    @Test
    public void shouldSendRemoveRecordWhenFileIsDeleted() throws Exception {
        givenDeleteSucceedsWith(FileType.REGULAR, 1024L);

        deleteAs(user);

        DoorRequestInfoMessage record = sentBillingRecord();
        assertThat(record.getMessageType(), equalTo("remove"));
        assertThat(record.getCellAddress(), equalTo(CELL_ADDRESS));
        assertThat(record.getBillingPath(), equalTo(PATH.toString()));
        assertThat(record.getPnfsId(), equalTo(PNFSID));
        assertThat(record.getFileSize(), equalTo(1024L));
        assertThat(Subjects.getUid(record.getSubject()), equalTo(1000L));
        assertThat(record.getClient(), equalTo("192.0.2.10"));
        assertThat(record.getClientChain(), equalTo("192.0.2.10,198.51.100.20"));
    }

    @Test
    public void shouldSendRemoveRecordWhenLinkIsDeleted() throws Exception {
        givenDeleteSucceedsWith(FileType.LINK, 12L);

        deleteAs(user);

        assertThat(sentBillingRecord().getMessageType(), equalTo("remove"));
    }

    @Test
    public void shouldNotSendRecordWhenDirectoryIsDeleted() throws Exception {
        givenDeleteSucceedsWith(FileType.DIR, 512L);

        deleteAs(user);

        verifyNoInteractions(billingTopic);
    }

    @Test
    public void shouldNotSendRecordWhenDeleteFails() throws Exception {
        when(pnfsmanager.send(any(PnfsDeleteEntryMessage.class), anyLong())).thenAnswer(i -> {
            PnfsDeleteEntryMessage msg = i.getArgument(0);
            msg.setFailed(CacheException.FILE_NOT_FOUND, "No such file or directory");
            return Futures.immediateFuture(msg);
        });

        try {
            deleteAs(user);
            fail("Expected NotFoundException");
        } catch (NotFoundException expected) {
        }

        verifyNoInteractions(billingTopic);
    }

    @Test
    public void shouldNameRequestingUserWhenAdminDeletesAsRoot() throws Exception {
        when(request.getAttribute(AuthenticationHandler.DCACHE_LOGIN_ATTRIBUTES))
              .thenReturn(Set.<LoginAttribute>of(new Role(LoginAttributes.ADMIN_ROLE_NAME)));
        givenDeleteSucceedsWith(FileType.REGULAR, 1024L);

        deleteAs(user);

        assertThat(Subjects.isRoot(sentDelete.get().getSubject()), is(true));
        assertThat(Subjects.getUid(sentBillingRecord().getSubject()), equalTo(1000L));
    }

    private void givenDeleteSucceedsWith(FileType type, long size) {
        when(pnfsmanager.send(any(PnfsDeleteEntryMessage.class), anyLong())).thenAnswer(i -> {
            PnfsDeleteEntryMessage msg = i.getArgument(0);
            sentDelete.set(msg);
            FileAttributes attributes = FileAttributes.of()
                  .pnfsId(PNFSID)
                  .fileType(type)
                  .size(size)
                  .build();
            msg.setFileAttributes(attributes);
            msg.setSucceeded();
            return Futures.immediateFuture(msg);
        });
    }

    /**
     * Calls the REST delete as {@code subject}, with the request state that the
     * {@link RequestUser} filter sets up for a real request.
     */
    private void deleteAs(Subject subject) throws Exception {
        try {
            Subject.callAs(subject, (Callable<Void>) () -> {
                requestUser.filter(null);
                resources.deleteFileEntry("data/file");
                return null;
            });
        } catch (CompletionException e) {
            Throwables.throwIfInstanceOf(e.getCause(), Exception.class);
            throw e;
        }
    }

    private DoorRequestInfoMessage sentBillingRecord() {
        ArgumentCaptor<Serializable> captor = ArgumentCaptor.forClass(Serializable.class);
        verify(billingTopic).notify(captor.capture());
        assertThat(captor.getValue(), instanceOf(DoorRequestInfoMessage.class));
        return (DoorRequestInfoMessage) captor.getValue();
    }
}

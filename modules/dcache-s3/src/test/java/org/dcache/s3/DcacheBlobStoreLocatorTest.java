package org.dcache.s3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.TimeoutCacheException;
import java.net.InetAddress;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import javax.security.auth.Subject;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.S3AccessKeyPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.auth.attributes.RootDirectory;
import org.gaul.s3proxy.AccessGrant;
import org.gaul.s3proxy.S3ErrorCode;
import org.gaul.s3proxy.S3ProxyException;
import org.gaul.s3proxy.blobstore.BlobStore;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class DcacheBlobStoreLocatorTest {

    private static final String ACCESS_KEY = "AKIAEXAMPLE";
    private static final FsPath DOOR_ROOT = FsPath.create("/data/s3");

    private S3KeyStore keyStore;
    private LoginStrategy loginStrategy;
    private BlobStore blobStore;
    private Supplier<S3User> user;
    private DcacheBlobStoreLocator locator;

    @Before
    public void setup() {
        keyStore = mock(S3KeyStore.class);
        loginStrategy = mock(LoginStrategy.class);
        blobStore = mock(BlobStore.class);
        when(keyStore.secretFor(any())).thenReturn(Optional.empty());
        locator = new DcacheBlobStoreLocator(keyStore, loginStrategy, DOOR_ROOT, u -> {
            user = u;
            return blobStore;
        });
    }

    @Test
    public void shouldNotGrantAccessWithoutIdentity() {
        assertThat(locator.locateBlobStore(null, "bucket", "key"), is(nullValue()));
        verifyNoInteractions(keyStore, loginStrategy);
    }

    @Test
    public void shouldNotGrantAccessForUnknownKey() {
        assertThat(locator.locateBlobStore(ACCESS_KEY, "bucket", "key"), is(nullValue()));
        verifyNoInteractions(loginStrategy);
    }

    @Test
    public void shouldGrantSecretAndBlobStoreForKnownKey() {
        givenKey(ACCESS_KEY, "secret");

        AccessGrant grant = locator.locateBlobStore(ACCESS_KEY, "bucket", "key");

        assertThat(grant.credential(), is(equalTo(Optional.of("secret"))));
        assertThat(grant.blobStore(), is(sameInstance(blobStore)));
    }

    @Test
    public void shouldNotLoginBeforeBlobStoreIsUsed() {
        givenKey(ACCESS_KEY, "secret");

        locator.locateBlobStore(ACCESS_KEY, "bucket", "key");

        verifyNoInteractions(loginStrategy);
    }

    @Test
    public void shouldLoginWithAccessKeyAndOrigin() throws Exception {
        givenKey(ACCESS_KEY, "secret");
        givenLoginReturns();
        InetAddress client = InetAddress.getByName("192.0.2.1");

        try (var ignored = RequestOrigin.bind(client)) {
            locator.locateBlobStore(ACCESS_KEY, "bucket", "key");
        }
        user.get();

        ArgumentCaptor<Subject> subject = ArgumentCaptor.forClass(Subject.class);
        verify(loginStrategy).login(subject.capture());
        assertThat(subject.getValue().getPrincipals(),
              hasItem(new S3AccessKeyPrincipal(ACCESS_KEY)));
        assertThat(subject.getValue().getPrincipals(), hasItem(new Origin(client)));
    }

    @Test
    public void shouldLoginWithoutOriginWhenNoneIsBound() throws Exception {
        givenKey(ACCESS_KEY, "secret");
        givenLoginReturns();

        locator.locateBlobStore(ACCESS_KEY, "bucket", "key");
        user.get();

        ArgumentCaptor<Subject> subject = ArgumentCaptor.forClass(Subject.class);
        verify(loginStrategy).login(subject.capture());
        assertThat(subject.getValue().getPrincipals(),
              contains(new S3AccessKeyPrincipal(ACCESS_KEY)));
    }

    @Test
    public void shouldReturnLoggedInUser() throws Exception {
        givenKey(ACCESS_KEY, "secret");
        Subject subject = new Subject();
        subject.getPrincipals().add(new UidPrincipal(1000));
        when(loginStrategy.login(any())).thenReturn(
              new LoginReply(subject, Set.of(Restrictions.readOnly())));

        locator.locateBlobStore(ACCESS_KEY, "bucket", "key");
        S3User loggedIn = user.get();

        assertThat(loggedIn.accessKey(), is(equalTo(ACCESS_KEY)));
        assertThat(loggedIn.subject(), is(equalTo(subject)));
        assertThat(loggedIn.restriction(), is(equalTo(Restrictions.readOnly())));
        assertThat(loggedIn.root(), is(equalTo(DOOR_ROOT)));
    }

    @Test
    public void shouldLoginOnlyOncePerRequest() throws Exception {
        givenKey(ACCESS_KEY, "secret");
        givenLoginReturns();

        locator.locateBlobStore(ACCESS_KEY, "bucket", "key");
        user.get();
        user.get();

        verify(loginStrategy, times(1)).login(any());
    }

    @Test
    public void shouldUseUserRootBelowDoorRoot() throws Exception {
        givenKey(ACCESS_KEY, "secret");
        givenLoginReturns(new RootDirectory("/data/s3/alice"));

        locator.locateBlobStore(ACCESS_KEY, "bucket", "key");

        assertThat(user.get().root(), is(equalTo(FsPath.create("/data/s3/alice"))));
    }

    @Test
    public void shouldUseDoorRootBelowUserRoot() throws Exception {
        givenKey(ACCESS_KEY, "secret");
        givenLoginReturns(new RootDirectory("/data"));

        locator.locateBlobStore(ACCESS_KEY, "bucket", "key");

        assertThat(user.get().root(), is(equalTo(DOOR_ROOT)));
    }

    @Test
    public void shouldDenyUserWithRootOutsideDoorRoot() throws Exception {
        givenKey(ACCESS_KEY, "secret");
        givenLoginReturns(new RootDirectory("/home/alice"));

        locator.locateBlobStore(ACCESS_KEY, "bucket", "key");

        S3ProxyException e = assertThrows(S3ProxyException.class, user::get);
        assertThat(e.getError(), is(S3ErrorCode.ACCESS_DENIED));
    }

    @Test
    public void shouldDenyAccessWhenLoginIsDenied() throws Exception {
        givenKey(ACCESS_KEY, "secret");
        when(loginStrategy.login(any())).thenThrow(new PermissionDeniedCacheException("no"));

        locator.locateBlobStore(ACCESS_KEY, "bucket", "key");

        S3ProxyException e = assertThrows(S3ProxyException.class, user::get);
        assertThat(e.getError(), is(S3ErrorCode.ACCESS_DENIED));
    }

    @Test
    public void shouldReportInternalErrorWhenLoginFails() throws Exception {
        givenKey(ACCESS_KEY, "secret");
        when(loginStrategy.login(any())).thenThrow(new TimeoutCacheException("timeout"));

        locator.locateBlobStore(ACCESS_KEY, "bucket", "key");

        S3ProxyException e = assertThrows(S3ProxyException.class, user::get);
        assertThat(e.getError(), is(S3ErrorCode.INTERNAL_ERROR));
    }

    @Test
    public void shouldNotLeakOriginAfterScopeIsClosed() throws Exception {
        try (var ignored = RequestOrigin.bind(InetAddress.getByName("192.0.2.1"))) {
            assertThat(RequestOrigin.current(), is(not(Optional.empty())));
        }

        assertFalse(RequestOrigin.current().isPresent());
    }

    @Test
    public void shouldNotLoginForUnknownKeyEvenIfKnownBefore() throws Exception {
        givenKey(ACCESS_KEY, "secret");
        locator.locateBlobStore(ACCESS_KEY, "bucket", "key");
        when(keyStore.secretFor(ACCESS_KEY)).thenReturn(Optional.empty());

        assertThat(locator.locateBlobStore(ACCESS_KEY, "bucket", "key"), is(nullValue()));
        verify(loginStrategy, never()).login(any());
    }

    private void givenKey(String accessKey, String secret) {
        when(keyStore.secretFor(accessKey)).thenReturn(Optional.of(secret));
    }

    private void givenLoginReturns(LoginAttribute... attributes) throws Exception {
        Subject subject = new Subject();
        subject.getPrincipals().add(new UidPrincipal(1000));
        when(loginStrategy.login(any())).thenReturn(new LoginReply(subject, Set.of(attributes)));
    }
}

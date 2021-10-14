package org.dcache.auth;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.TimeoutCacheException;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.HomeDirectory;
import org.junit.Before;
import org.junit.Test;

public class CachingLoginStrategyTests {

    LoginStrategy _backEnd;
    LoginStrategy _cache;
    Subject _subject;
    LoginReply _reply;

    @Before
    public void setUp() {
        _backEnd = mock(LoginStrategy.class);
        _cache = new CachingLoginStrategy(_backEnd, 1, Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        _subject = new Subject();
        _subject.getPrincipals().add(new UserNamePrincipal("andrew"));

        _reply = new LoginReply();
        _reply.getSubject().getPrincipals().add(new UidPrincipal(1000));
        _reply.getLoginAttributes().add(new HomeDirectory("/home/andrew"));
    }

    @Test
    public void testThatCachePreservesResponse() throws CacheException {
        when(_backEnd.login(any(Subject.class))).thenReturn(_reply);

        LoginReply reply = _cache.login(_subject);

        assertThat(reply, is(_reply));
    }

    @Test
    public void testThatCacheActuallyCachesOnTwoCalls() throws CacheException {
        when(_backEnd.login(any(Subject.class))).thenReturn(_reply);

        _cache.login(_subject);
        _cache.login(_subject);

        verify(_backEnd).login(_subject);
    }

    @Test
    public void testThatCacheActuallyCachesOnThreeCalls() throws CacheException {
        when(_backEnd.login(any(Subject.class))).thenReturn(_reply);

        _cache.login(_subject);
        _cache.login(_subject);
        _cache.login(_subject);

        verify(_backEnd).login(_subject);
    }

    @Test
    public void testWithTwoQueriesWithDiffSubjectsBothTriggerQuery() throws CacheException {
        Subject newSubject = new Subject();
        newSubject.getPrincipals().add(new UserNamePrincipal("fred"));

        LoginReply newReply = new LoginReply();
        newReply.getSubject().getPrincipals().add(new UidPrincipal(1010));
        newReply.getLoginAttributes().add(new HomeDirectory("/home/fred"));

        // Prime the cache
        when(_backEnd.login(any(Subject.class))).thenReturn(_reply);
        _cache.login(_subject);

        // Check that a different subject doesn't return the cached reply
        reset(_backEnd);
        when(_backEnd.login(any(Subject.class))).thenReturn(newReply);

        LoginReply reply = _cache.login(newSubject);
        assertThat(reply, is(newReply));
    }

    @Test(expected = TimeoutCacheException.class)
    public void testThatTimeoutCacheExceptionsArePropagated() throws CacheException {
        when(_backEnd.login(any(Subject.class))).thenThrow(TimeoutCacheException.class);
        _cache.login(_subject);
    }

    @Test(expected = PermissionDeniedCacheException.class)
    public void testThatPermissionDeniedCacheExceptionsArePropagated() throws CacheException {
        when(_backEnd.login(any(Subject.class))).thenThrow(PermissionDeniedCacheException.class);
        _cache.login(_subject);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThatIllegalArgumentExceptionsArePropagated() throws CacheException {
        when(_backEnd.login(any(Subject.class))).thenThrow(IllegalArgumentException.class);
        _cache.login(_subject);
    }

    @Test
    public void testThatTimeoutsAreNotCached() throws CacheException {
        when(_backEnd.login(any(Subject.class))).thenThrow(TimeoutCacheException.class);
        try {
            _cache.login(_subject);
        } catch (TimeoutCacheException ignored) {
        }

        reset(_backEnd);
        when(_backEnd.login(any(Subject.class))).thenReturn(_reply);
        LoginReply reply = _cache.login(_subject);
        assertThat(reply, is(_reply));
    }
}

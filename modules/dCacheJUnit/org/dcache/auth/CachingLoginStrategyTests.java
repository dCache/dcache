package org.dcache.auth;

import static org.junit.Assert.assertEquals;

import javax.security.auth.Subject;

import org.dcache.auth.attributes.HomeDirectory;
import org.junit.Before;
import org.junit.Test;

import diskCacheV111.util.CacheException;

public class CachingLoginStrategyTests {

    CountingLoginStrategy _backEnd;
    LoginStrategy _cache;
    Subject _subject;
    LoginReply _reply;

    @Before
    public void setUp() {
        _backEnd = new CountingLoginStrategy();
        _cache = new CachingLoginStrategy( _backEnd);

        _subject = new Subject();
        _subject.getPrincipals().add( new UserNamePrincipal( "andrew"));

        _reply = new LoginReply();
        _reply.getSubject().getPrincipals().add( new UidPrincipal( 1000));
        _reply.getLoginAttributes().add( new HomeDirectory( "/home/andrew"));
        _backEnd.setLoginReply( _reply);
    }

    @Test
    public void testFirstQueryTriggersQuery() {
        doLoginAndAssert( 1);
    }

    @Test
    public void testWithTwoQueriesOnlyFirstTriggersQuery() {
        doLoginAndAssert( 1);
        doLoginAndAssert( 1);
    }

    @Test
    public void testWithThreeQueriesOnlyFirstTriggersQuery() {
        doLoginAndAssert( 1);
        doLoginAndAssert( 1);
        doLoginAndAssert( 1);
    }

    @Test
    public void testWithTwoQueriesWithDiffSubjectsBothTriggerQuery() {
        doLoginAndAssert( 1);

        Subject newSubject = new Subject();
        newSubject.getPrincipals().add( new UserNamePrincipal( "fred"));

        LoginReply newReply = new LoginReply();
        newReply.getSubject().getPrincipals().add( new UidPrincipal( 1010));
        newReply.getLoginAttributes().add( new HomeDirectory( "/home/fred"));

        _backEnd.setLoginReply( newReply);

        doLoginAndAssert( newSubject, newReply, 2);
    }

    private void doLoginAndAssert( int expectedCount) {
        doLoginAndAssert( _subject, _reply, expectedCount);
    }

    private void doLoginAndAssert( Subject subject, LoginReply expectedReply,
                                   int expectedCount) {
        LoginReply reply;

        try {
            reply = _cache.login( subject);
        } catch (CacheException e) {
            throw new RuntimeException( "Didn't expect that!", e);
        }
        assertEquals( "check queries", expectedCount, _backEnd.getCount());

        assertEquals( "login attributes", expectedReply.getLoginAttributes(),
                reply.getLoginAttributes());
        assertEquals( "login subject", expectedReply.getSubject(), reply
                .getSubject());
    }

    /**
     * A simple LoginStrategy that returns a predefined LoginResult and keeps
     * track of how often the login method it is called.
     */
    static class CountingLoginStrategy implements LoginStrategy {
        private int _count;
        private LoginReply _result;

        @Override
        public LoginReply login( Subject subject) throws CacheException {
            _count++;
            return _result;
        }

        public void setLoginReply( LoginReply result) {
            _result = result;
        }

        public int getCount() {
            return _count;
        }
    }
}

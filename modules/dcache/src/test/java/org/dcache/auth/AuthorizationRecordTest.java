package org.dcache.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

import javax.security.auth.Subject;

import org.globus.gsi.jaas.GlobusPrincipal;
import org.junit.Before;
import org.junit.Test;

public class AuthorizationRecordTest
{
    private final static String USERNAME1 = "user1";
    private final static String USERNAME2 = "user2";

    private final static long UID1 = 500;
    private final static long UID2 = 501;
    private final static long GID1 = 1000;
    private final static long GID2 = 1001;

    private final static String DN1 =
        "/O=Grid/O=example/OU=example.org/CN=Tester 1";
    private final static String DN2 =
        "/O=Grid/O=example/OU=example.org/CN=Tester 2";

    private final static String ROLE = "tester";
    private final static String FQAN1 =
        "/example";
    private final static String FQAN2 =
        "/example/group";
    private final static String FQAN3 =
        "/example/group/Role=" + ROLE;

    private Subject _subject1;
    private Subject _subject2;
    private Subject _subject3;
    private Subject _subject4;

    @Before
    public void setUp()
        throws Throwable
    {
        _subject1 = new Subject();
        _subject1.getPrincipals().add(new UidPrincipal(UID1));
        _subject1.getPrincipals().add(new GidPrincipal(GID1, true));
        _subject1.getPrincipals().add(new UserNamePrincipal(USERNAME1));
        _subject1.getPrincipals().add(new GlobusPrincipal(DN1));
        _subject1.getPrincipals().add(new FQANPrincipal(FQAN1, true));

        _subject2 = new Subject();
        _subject2.getPrincipals().add(new UidPrincipal(UID2));
        _subject2.getPrincipals().add(new GidPrincipal(GID2, false));
        _subject2.getPrincipals().add(new UserNamePrincipal(USERNAME2));
        _subject2.getPrincipals().add(new GlobusPrincipal(DN2));
        _subject2.getPrincipals().add(new FQANPrincipal(FQAN2, false));

        _subject3 = new Subject();
        _subject3.getPrincipals().add(new UidPrincipal(UID1));
        _subject3.getPrincipals().add(new UidPrincipal(UID2));
        _subject3.getPrincipals().add(new GidPrincipal(GID1, false));
        _subject3.getPrincipals().add(new GidPrincipal(GID2, true));
        _subject3.getPrincipals().add(new UserNamePrincipal(USERNAME1));
        _subject3.getPrincipals().add(new UserNamePrincipal(USERNAME2));
        _subject3.getPrincipals().add(new GlobusPrincipal(DN1));
        _subject3.getPrincipals().add(new GlobusPrincipal(DN2));
        _subject3.getPrincipals().add(new FQANPrincipal(FQAN1, false));
        _subject3.getPrincipals().add(new FQANPrincipal(FQAN2, true));
        _subject3.getPrincipals().add(new FQANPrincipal(FQAN3, false));

        _subject4 = new Subject();
        _subject4.getPrincipals().add(new UidPrincipal(UID1));
        _subject4.getPrincipals().add(new GidPrincipal(GID1, false));
        _subject4.getPrincipals().add(new GidPrincipal(GID2, true));
        _subject4.getPrincipals().add(new UserNamePrincipal(USERNAME1));
        _subject4.getPrincipals().add(new GlobusPrincipal(DN1));
        _subject4.getPrincipals().add(new FQANPrincipal(FQAN1, false));
        _subject4.getPrincipals().add(new FQANPrincipal(FQAN2, false));
        _subject4.getPrincipals().add(new FQANPrincipal(FQAN3, true));
    }

    @Test
    public void testGetAuthorizationRecord1()
    {
        AuthorizationRecord record = new AuthorizationRecord(_subject1);
        assertEquals("UID must be preserved",
                     UID1, record.getUid());
        assertEquals("GID must be preserved",
                     GID1, record.getGid());
        assertEquals("User name must be preserved",
                     USERNAME1, record.getIdentity());
        assertEquals("DN must be preserved",
                     DN1, record.getName());
        assertEquals("VO group must be FQAN",
                     FQAN1, record.getVoGroup());
        assertNull("Must not have VO role",
                   record.getVoRole());
    }

    @Test
    public void testGetAuthorizationRecord2()
    {
        AuthorizationRecord record = new AuthorizationRecord(_subject2);
        assertEquals("UID must be preserved",
                     UID2, record.getUid());
        assertEquals("GID must be preserved",
                     GID2, record.getGid());
        assertEquals("User name must be preserved",
                     USERNAME2, record.getIdentity());
        assertEquals("Name must be DN",
                     DN2, record.getName());
        assertEquals("VO group must be user name if there is no primary group",
                     USERNAME2, record.getVoGroup());
        assertNull("Must not have VO role",
                   record.getVoRole());
    }

    @Test
    public void testGetAuthorizationRecord4()
    {
        AuthorizationRecord record = new AuthorizationRecord(_subject4);
        assertEquals("UID must be preserved",
                     UID1, record.getUid());
        assertEquals("GID must be preserved",
                     GID2, record.getGid());
        assertEquals("User name must be preserved",
                     USERNAME1, record.getIdentity());
        assertEquals("DN must be preserved",
                     DN1, record.getName());
        assertEquals("VO group must be FQAN",
                     FQAN2, record.getVoGroup());
        assertEquals("VO role must be role of primary FQAN",
                     ROLE, record.getVoRole());
    }

    @Test
    public void testAuthorizationRecordRoundTrip()
    {
        assertEquals(_subject1, new AuthorizationRecord(_subject1).toSubject());

        assertEquals(Subjects.ROOT, new AuthorizationRecord(Subjects.ROOT).toSubject());

        /* We cannot preserve that subject2 does not have a primary GID.
         */
        assertFalse(_subject2.equals(new AuthorizationRecord(_subject2).toSubject()));

    }

    @Test(expected=IllegalArgumentException.class)
    public void testGetAuthorizationRecordWithNoUid()
    {
        new AuthorizationRecord(Subjects.NOBODY);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testGetAuthorizationRecordWithTwoUids()
    {
        new AuthorizationRecord(_subject3);
    }
}
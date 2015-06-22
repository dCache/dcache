package org.dcache.auth;

import com.google.common.collect.ImmutableSet;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.Subject;

import java.util.HashSet;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

public class SubjectsTest
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
    private final static FQAN FQAN1 = new FQAN("/example");
    private final static FQAN FQAN2 = new FQAN("/example/group");
    private final static FQAN FQAN3 = new FQAN("/example/group/Role=" + ROLE);

    private Subject _subject1;
    private Subject _subject2;
    private Subject _subject3;
    private Subject _subject4;

    @Before
    public void setUp()
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
    public void testIsRoot()
    {
        assertTrue("ROOT must be root",
                   Subjects.isRoot(Subjects.ROOT));
        assertFalse("NOBODY must not be root",
                    Subjects.isRoot(Subjects.NOBODY));
    }

    @Test
    public void testHasUid()
    {
        assertTrue("Subject must have its UID",
                   Subjects.hasUid(_subject1, UID1));
        assertFalse("Subject must not have foreign UID",
                    Subjects.hasUid(_subject1, UID2));
        assertTrue("Subject must have its UID",
                   Subjects.hasUid(_subject3, UID1));
        assertTrue("Subject must have its UID",
                   Subjects.hasUid(_subject3, UID2));
    }

    @Test
    public void testGetUid()
    {
        assertEquals("Subject must provide its UID",
                     UID1, Subjects.getUid(_subject1));
        assertEquals("Subject must provide its UID",
                     UID2, Subjects.getUid(_subject2));
    }

    @Test
    public void testGetUids()
    {
        assertTrue("NOBODY must not have UIDs",
                Subjects.getUids(Subjects.NOBODY).length == 0);
        assertArrayEquals("Subject with one UID must have that UID",
                new long[] { UID1 }, Subjects.getUids(_subject1));
        assertArrayEquals("Subject with several UIDs must have those UIDs",
                new long[] {UID1, UID2}, Subjects.getUids(_subject3));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testGetUidWithMultipleUids()
    {
        Subjects.getUid(_subject3);
    }

    @Test(expected=NoSuchElementException.class)
    public void testGetUidWithNoUid()
    {
        Subjects.getUid(Subjects.NOBODY);
    }

    @Test
    public void testHasGid()
    {
        assertTrue("Subject must have its GID",
                   Subjects.hasGid(_subject1, GID1));
        assertFalse("Subject must not have foreign GID",
                    Subjects.hasGid(_subject1, GID2));
        assertFalse("Subject must not have foreign GID",
                    Subjects.hasGid(_subject2, GID1));
        assertTrue("Subject must have its GID",
                   Subjects.hasGid(_subject2, GID2));
        assertTrue("Subject must have its GID",
                   Subjects.hasGid(_subject3, GID1));
        assertTrue("Subject must have its GID",
                   Subjects.hasGid(_subject3, GID2));
    }

    @Test
    public void testGetPrimaryGid()
    {
        assertEquals("Subject must provide its primary GID",
                     GID1, Subjects.getPrimaryGid(_subject1));
        assertEquals("Subject must provide its primary GID",
                     GID2, Subjects.getPrimaryGid(_subject3));
    }

    @Test
    public void testGetGids()
    {
        assertArrayEquals("NOBODY must not have GIDs",
                          new long[] { }, Subjects.getGids(Subjects.NOBODY));
        assertArrayEquals("Subject with primary GID must provide that GID",
                          new long[] { GID1 }, Subjects.getGids(_subject1));
        assertArrayEquals("Subject with non primary GID must provide that GID",
                          new long[] { GID2 }, Subjects.getGids(_subject2));
        assertArrayEquals("Subject with several GIDs must provide all of them",
                          new long[] { GID2, GID1 }, Subjects.getGids(_subject3));
    }

    @Test(expected=NoSuchElementException.class)
    public void testGetPrimaryGidWithNoPrimaryGid()
    {
        Subjects.getPrimaryGid(_subject2);
    }

    @Test
    public void testGetUserNameWithNull()
    {
        assertNull(Subjects.getUserName(null));
    }

    @Test
    public void testGetUserName()
    {
        assertNull("Root must not have user name",
                   Subjects.getUserName(Subjects.ROOT));
        assertEquals("Subject with one user name must provide that name",
                     USERNAME1, Subjects.getUserName(_subject1));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testGetUserNameWithMultipleUserNames()
    {
        Subjects.getUserName(_subject3);
    }

    @Test
    public void testGetDn()
    {
        assertNull("Root must not have DN", Subjects.getDn(Subjects.ROOT));
        assertEquals("Subject with one DN must have correct DN",
                     DN1, Subjects.getDn(_subject1));
        assertEquals("Subject with one DN must have correct DN",
                     DN2, Subjects.getDn(_subject2));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testGetDnWithMultipleDns()
    {
        Subjects.getDn(_subject3);
    }

    @Test
    public void testGetFqans()
    {
        assertTrue("Root must not have FQANs",
                    Subjects.getFqans(Subjects.ROOT).isEmpty());
        assertEquals("Subject with primary FQAN must have correct FQAN",
                     ImmutableSet.of(FQAN1),
                     new HashSet<>(Subjects.getFqans(_subject1)));
        assertEquals("Subject with no primary FQAN must have correct FQAN",
                     ImmutableSet.of(FQAN2),
                     new HashSet<>(Subjects.getFqans(_subject2)));
        assertEquals("Subject with multiple FQANs must have correct FQANs",
                     ImmutableSet.of(FQAN1, FQAN2, FQAN3),
                     new HashSet<>(Subjects.getFqans(_subject3)));
    }

    @Test
    public void testGetPrimaryFqan()
    {
        assertEquals("Subject with one FQAN must have correct primary FQAN",
                     FQAN1, Subjects.getPrimaryFqan(_subject1));
        assertEquals("Subject with multiple FQANs must have correct primary FQAN",
                     FQAN2, Subjects.getPrimaryFqan(_subject3));
    }

    @Test
    public void testGetPrimaryFqanWithNoFqans()
    {
        assertNull("Subject must not have any FQANs",
                   Subjects.getPrimaryFqan(Subjects.ROOT));
    }

    @Test
    public void testGetPrimaryFqanWithNoPrimaryFqan()
    {
        assertNull("Subject must not have a primary FQAN",
                   Subjects.getPrimaryFqan(_subject2));
    }

    @Test
    public void testGetSubjectWithMulitipleGids()
    {
        UserAuthRecord authRecord = new UserAuthRecord();
        authRecord.UID = (int)UID1;
        authRecord.GIDs.add((int)GID1);
        authRecord.GIDs.add((int)GID2);
        Subject subject = Subjects.getSubject(authRecord);
        long[] gids = Subjects.getGids(subject);

        assertEquals(GID1, gids[0]);
        assertEquals(GID2, gids[1]);
    }
}

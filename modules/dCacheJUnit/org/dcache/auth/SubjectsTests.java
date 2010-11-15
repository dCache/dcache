package org.dcache.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import javax.security.auth.Subject;

import org.junit.Before;
import org.junit.Test;

public class SubjectsTests {

    Subject subject;

    @Before
    public void setUp() {
        subject = new Subject();
    }

    @Test
    public void testGetUniquePrincipalWithNull() {
        assertNull(Subjects.getUserName(null));
    }

    @Test
    public void testGetUniquePrincipalWithNoName() {
        assertNull(Subjects.getUserName(subject));
    }

    @Test
    public void testGetUniquePrincipalWithSingleUserName() {
        String userName = "Fred";
        subject.getPrincipals().add( new UserNamePrincipal(userName));
        assertEquals(userName, Subjects.getUserName(subject));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testGetUniquePrincipalWithMultipleUserName() {
        subject.getPrincipals().add( new UserNamePrincipal("Fred"));
        subject.getPrincipals().add( new UserNamePrincipal("Andrew"));
        assertNull(Subjects.getUserName(subject));
    }
}

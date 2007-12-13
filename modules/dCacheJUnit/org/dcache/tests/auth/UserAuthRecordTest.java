package org.dcache.tests.auth;

import java.util.HashSet;

import org.junit.Test;
import static org.junit.Assert.*;

import diskCacheV111.util.UserAuthRecord;


public class UserAuthRecordTest {


    @Test
    public void testEqualsByGids() {

        int uid = 3750;
        boolean readOnly = false;
        String fqan = null;
        String user = "tigran";
        String DN = "/";
        HashSet<String> principals = new HashSet<String>();
        String home = "/";
        String fsroot = "/";
        String root = "/";
        int priority = 1;


        int gids1[] = new int[] {1, 2, 3};
        int gids2[] = new int[] {1, 5, 6};

        UserAuthRecord userRecord1 = new UserAuthRecord(user, DN, fqan, readOnly, priority, uid, gids1, home, root, fsroot, principals);
        UserAuthRecord userRecord2 = new UserAuthRecord(user, DN, fqan, readOnly, priority, uid, gids2, home, root, fsroot, principals);

        assertFalse("User with different groups can't be equals", userRecord1.equals(userRecord2));
    }

    @Test
    public void testEquals() {

        int uid = 3750;
        boolean readOnly = false;
        String fqan = null;
        String user = "tigran";
        String DN = "/";
        HashSet<String> principals = new HashSet<String>();
        String home = "/";
        String fsroot = "/";
        String root = "/";
        int priority = 1;


        int gids1[] = new int[] {1, 2, 3};
        int gids2[] = new int[] {1, 2, 3};

        UserAuthRecord userRecord1 = new UserAuthRecord(user, DN, fqan, readOnly, priority, uid, gids1, home, root, fsroot, principals);
        UserAuthRecord userRecord2 = new UserAuthRecord(user, DN, fqan, readOnly, priority, uid, gids2, home, root, fsroot, principals);

        assertTrue("Users with the same attributes have to be equal", userRecord1.equals(userRecord2));
    }
}

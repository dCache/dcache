package org.dcache.tests.namespace;

import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.auth.Subject;

import java.security.Principal;

import org.dcache.acl.enums.AccessType;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.Origin;
import org.dcache.auth.UidPrincipal;
import org.dcache.namespace.PosixPermissionHandler;
import org.dcache.vehicles.FileAttributes;

import static org.junit.Assert.assertTrue;

public class PosixPermissionHandlerTest {

    private static final int ROOT_UID = 0, OWNER_UID = 3750, GROUP_MEMBER_UID = 3752, OTHER_UID = 3752, ANONYMOUOS_UID = 1111;
    private static final int ROOT_GID = 0, OWNER_GID = 1000, OTHER_GID = 7777, ANONYMOUOS_GID = 2222;
    private static PosixPermissionHandler pdp;
    private static Origin origin;
    private static Subject subject_owner, subject_groupMember, subject_other, subject_anonymouos;

    @BeforeClass
    public static void setUpClass() throws Exception {
        pdp = new PosixPermissionHandler();
        origin = new Origin("127.0.0.1");

        // Initialize owner subject
        subject_owner = new Subject();
        Principal userOwner = new UidPrincipal(OWNER_UID);
        Principal groupOwner = new GidPrincipal(OWNER_GID, true);
        subject_owner.getPrincipals().add(userOwner);
        subject_owner.getPrincipals().add(groupOwner);
        subject_owner.getPrincipals().add(origin);

        // Initialize group member subject
        subject_groupMember = new Subject();
        Principal userMember = new UidPrincipal(GROUP_MEMBER_UID);
        Principal groupMember = new GidPrincipal(OWNER_GID, true);
        subject_groupMember.getPrincipals().add(userMember);
        subject_groupMember.getPrincipals().add(groupMember);
        subject_groupMember.getPrincipals().add(origin);

        // Initialize other subject
        subject_other = new Subject();
        Principal userOther = new UidPrincipal(OTHER_UID);
        Principal groupOther = new GidPrincipal(OTHER_GID, true);
        subject_other.getPrincipals().add(userOther);
        subject_other.getPrincipals().add(groupOther);
        subject_other.getPrincipals().add(origin);

        // Initialize anonymous subject
        subject_anonymouos = new Subject();
        Principal userAnonymouos = new UidPrincipal(ANONYMOUOS_UID);
        Principal groupAnonymouos = new GidPrincipal(ANONYMOUOS_GID, true);
        subject_anonymouos.getPrincipals().add(userAnonymouos);
        subject_anonymouos.getPrincipals().add(groupAnonymouos);
        subject_anonymouos.getPrincipals().add(origin);
    }

    private FileAttributes getAttributes(int uid, int gid, int mode) {
        FileAttributes attr = new FileAttributes();
        attr.setOwner(uid);
        attr.setGroup(gid);
        attr.setMode(mode);
        return attr;
    }

    /***********************************************************************************************************************************************************
     * Tests
     */
    @Test
    public void testCreateFile() {
        FileAttributes attr;

        attr = getAttributes(ROOT_UID, ROOT_GID, 0755);
        assertTrue("Regular user is not allowed to create a file without sufficient permissions",
                pdp.canCreateFile(subject_owner, attr) == AccessType.ACCESS_DENIED);

        attr = getAttributes(OWNER_UID, OWNER_GID, 0755);
        assertTrue("User should be allowed to create a file with sufficient permissions",
                pdp.canCreateFile(subject_owner, attr) == AccessType.ACCESS_ALLOWED);
    }

    @Test
    public void testCreateDir() {
        FileAttributes attr = getAttributes(ROOT_UID, ROOT_GID, 0755);
        assertTrue("Regular user is not allowed to create a directory without sufficient permissions", //
                pdp.canCreateSubDir(subject_owner, attr) == AccessType.ACCESS_DENIED);
    }

    @Test
    public void testWriteFile() {
        FileAttributes attr = getAttributes(OWNER_UID, OWNER_GID, 0600);

        assertTrue("Owner is allowed to write into his file with mode 0600", //
                pdp.canWriteFile(subject_owner, attr) == AccessType.ACCESS_ALLOWED);

        assertTrue("Group member not allowed to write into a file with mode 0600", //
                pdp.canWriteFile(subject_groupMember, attr) == AccessType.ACCESS_DENIED);

        assertTrue("Other not allowed to write into a file with mode 0600", //
                pdp.canWriteFile(subject_other, attr) == AccessType.ACCESS_DENIED);
    }

    @Test
    public void testGroupCreate() {
        FileAttributes attr = getAttributes(OWNER_UID, OWNER_GID, 0770);

        assertTrue("Group member is allowed to create a new directory in a parent with mode 0770", //
                pdp.canCreateSubDir(subject_groupMember, attr) == AccessType.ACCESS_ALLOWED);
    }

    @Test
    public void testNegativeGroup() {
        FileAttributes attr = getAttributes(OWNER_UID, OWNER_GID, 0707);
        assertTrue("Negative group member not allowed to create a new directory in a parent with mode 0707", //
                pdp.canCreateSubDir(subject_groupMember, attr) == AccessType.ACCESS_DENIED);
    }

    @Test
    public void testNegativeOwner() {
        FileAttributes attr = getAttributes(OWNER_UID, OWNER_GID, 0077);
        assertTrue("Negative owner not allowed to create a new directory in a parent with mode 0077", //
                pdp.canCreateSubDir(subject_owner, attr) == AccessType.ACCESS_DENIED);
    }
}

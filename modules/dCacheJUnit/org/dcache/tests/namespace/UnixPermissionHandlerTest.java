package org.dcache.tests.namespace;

import static org.junit.Assert.assertTrue;

import org.dcache.acl.enums.AccessType;
import org.dcache.tests.cells.CellAdapterHelper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import javax.security.auth.Subject;
import java.security.Principal;

import diskCacheV111.services.acl.UnixPermissionHandler;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.Origin;
import org.dcache.auth.UidPrincipal;

/**
 * @author Irina Kozlova, David Melkumyan
 *
 */
public class UnixPermissionHandlerTest {

    /***********************************************************************************************************************************************************
     * Constants
     */
    private static final String CELL_ARGS = //
            " -meta-data-provider=org.dcache.tests.namespace.FileMetaDataProviderHelper";
    private static final boolean DIR = true, FILE = false;
    private static final int ROOT_UID = 0, OWNER_UID = 3750, GROUP_MEMBER_UID = 3752, OTHER_UID = 3752, ANONYMOUOS_UID = 1111;
    private static final int ROOT_GID = 0, OWNER_GID = 1000, OTHER_GID = 7777, ANONYMOUOS_GID = 2222;
    /***********************************************************************************************************************************************************
     * Static member variables
     */
    private static CellAdapterHelper cell;
    private static UnixPermissionHandler permissionHandler;
    private static FileMetaDataProviderHelper metadataSource;
    private static Origin origin;
    private static Subject subject_owner, subject_groupMember, subject_other, subject_anonymouos;

    /***********************************************************************************************************************************************************
     * Static methods
     */
    @BeforeClass
    public static void setUpClass() throws Exception {
        cell = new CellAdapterHelper("TestCell", CELL_ARGS); // Initialize dummy CellAdapter

        permissionHandler = new UnixPermissionHandler(cell); // Initialize Permission Handler
        metadataSource = (FileMetaDataProviderHelper) permissionHandler.getMetadataSource(); // Initialize Metadata Source

        origin = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG, "127.0.0.1"); // Initialize origin

        // Initialize owner subject
        subject_owner = new Subject();
        Principal userOwner = new UidPrincipal(OWNER_UID);
        Principal groupOwner = new GidPrincipal(OWNER_GID, true);
        subject_owner.getPrincipals().add(userOwner);
        subject_owner.getPrincipals().add(groupOwner);

        // Initialize group member subject
        subject_groupMember = new Subject();
        Principal userMember = new UidPrincipal(GROUP_MEMBER_UID);
        Principal groupMember = new GidPrincipal(OWNER_GID, true);
        subject_groupMember.getPrincipals().add(userMember);
        subject_groupMember.getPrincipals().add(groupMember);

        // Initialize other subject
        subject_other = new Subject();
        Principal userOther = new UidPrincipal(OTHER_UID);
        Principal groupOther = new GidPrincipal(OTHER_GID, true);
        subject_other.getPrincipals().add(userOther);
        subject_other.getPrincipals().add(groupOther);

        // Initialize anonymous subject
        subject_anonymouos = new Subject();
        Principal userAnonymouos = new UidPrincipal(ANONYMOUOS_UID);
        Principal groupAnonymouos = new GidPrincipal(ANONYMOUOS_GID, true);
        subject_anonymouos.getPrincipals().add(userAnonymouos);
        subject_anonymouos.getPrincipals().add(groupAnonymouos);
    }

    @Before
    public void setUp() throws Exception {
        metadataSource.cleanAll();
    }

    /***********************************************************************************************************************************************************
     * Tests
     */
    @Test
    public void testCreateFile() throws Exception {
        final PnfsId parentPnfsId = new PnfsId("00006E4FCE51400C4FA38F2E10AAB52E6306");
        metadataSource.setMetaData(parentPnfsId, new FileMetaData(DIR, ROOT_UID, ROOT_GID, 0755));
        assertTrue("Regular user is not allowed to create a file without sufficient permissions", //
                permissionHandler.canCreateFile(parentPnfsId, subject_owner, origin) == AccessType.ACCESS_DENIED);

        final PnfsId dirPnfsId = new PnfsId("00006E4FCE51400C4FA38F2E177777777776");
        metadataSource.setMetaData(dirPnfsId, new FileMetaData(DIR, OWNER_UID, OWNER_GID, 0755));
        assertTrue("User should be allowed to create a file with sufficient permissions", //
                permissionHandler.canCreateFile(dirPnfsId, subject_owner, origin) == AccessType.ACCESS_ALLOWED);
    }

    @Test
    public void testCreateDir() throws Exception {
        final PnfsId pnfsId = new PnfsId("00006E4FCE51400C4FA38F2E10AAB52E6306");
        metadataSource.setMetaData(pnfsId, new FileMetaData(DIR, ROOT_UID, ROOT_GID, 0755));
        assertTrue("Regular user is not allowed to create a directory without sufficient permissions", //
                permissionHandler.canCreateDir(pnfsId, subject_owner, origin) == AccessType.ACCESS_DENIED);
    }
    /*
    @Test
    public void testReadFile() throws Exception {
    final PnfsId parentPnfsId = new PnfsId("00006E4FCE51400C4FA38F2E10AAB52E6306");
    metadataSource.setMetaData(parentPnfsId, new FileMetaData(DIR, ROOT_UID, ROOT_GID, 0755));

    final PnfsId filePnfsId = new PnfsId("00006E4FCE51400C4FA38F2E177777777777");
    metadataSource.setMetaData(filePnfsId, new FileMetaData(FILE, OWNER_UID, OWNER_GID, 0600));

    assertTrue("Owner is allowed to read his file with mode 0600", //
    permissionHandler.canReadFile(filePnfsId, subject_owner, origin) == AccessType.ACCESS_ALLOWED);

    assertTrue("Group member not allowed to read a file with mode 0600", //
    permissionHandler.canReadFile(filePnfsId, subject_groupMember, origin) == AccessType.ACCESS_DENIED);

    assertTrue("Other not allowed to read a file with mode 0600", //
    permissionHandler.canReadFile(filePnfsId, subject_other, origin) == AccessType.ACCESS_DENIED);
    }
     */

    @Test
    public void testWriteFile() throws Exception {
        final PnfsId parentPnfsId = new PnfsId("00006E4FCE51400C4FA38F2E10AAB52E6306");
        metadataSource.setMetaData(parentPnfsId, new FileMetaData(DIR, ROOT_UID, ROOT_GID, 0755));

        final PnfsId filePnfsId = new PnfsId("00006E4FCE51400C4FA38F2E177777777777");
        metadataSource.setMetaData(filePnfsId, new FileMetaData(FILE, OWNER_UID, OWNER_GID, 0600));

        assertTrue("Owner is allowed to write into his file with mode 0600", //
                permissionHandler.canWriteFile(filePnfsId, subject_owner, origin) == AccessType.ACCESS_ALLOWED);

        assertTrue("Group member not allowed to write into a file with mode 0600", //
                permissionHandler.canWriteFile(filePnfsId, subject_groupMember, origin) == AccessType.ACCESS_DENIED);

        assertTrue("Other not allowed to write into a file with mode 0600", //
                permissionHandler.canWriteFile(filePnfsId, subject_other, origin) == AccessType.ACCESS_DENIED);
    }
    /*
    @Test
    public void testGroupRead() throws Exception {
    final PnfsId parentPnfsId = new PnfsId("00006E4FCE51400C4FA38F2E10AAB52E6306");
    metadataSource.setMetaData(parentPnfsId, new FileMetaData(DIR, ROOT_UID, ROOT_GID, 0755));

    final PnfsId filePnfsId = new PnfsId("00006E4FCE51400C4FA38F2E177777777777");
    metadataSource.setMetaData(filePnfsId, new FileMetaData(FILE, OWNER_UID, OWNER_GID, 0640));

    assertTrue("Owner is allowed to read his file with mode 0640", //
    permissionHandler.canReadFile(filePnfsId, subject_owner, origin) == AccessType.ACCESS_ALLOWED);

    assertTrue("Group member is allowed to read a file with mode 0640", //
    permissionHandler.canReadFile(filePnfsId, subject_groupMember, origin) == AccessType.ACCESS_ALLOWED);

    assertTrue("Group member not allowed to write into a file with mode 0640", //
    permissionHandler.canWriteFile(filePnfsId, subject_groupMember, origin) == AccessType.ACCESS_DENIED);
    }

    @Test
    public void testGroupWrite() throws Exception {
    final PnfsId parentPnfsId = new PnfsId("00006E4FCE51400C4FA38F2E10AAB52E6306");
    metadataSource.setMetaData(parentPnfsId, new FileMetaData(DIR, ROOT_UID, ROOT_GID, 0755));

    final PnfsId filePnfsId = new PnfsId("00006E4FCE51400C4FA38F2E177777777777");
    metadataSource.setMetaData(filePnfsId, new FileMetaData(FILE, OWNER_UID, OWNER_GID, 0660));

    assertTrue("Owner is allowed to read his file with mode 0660", //
    permissionHandler.canReadFile(filePnfsId, subject_owner, origin) == AccessType.ACCESS_ALLOWED);

    assertTrue("Group member is allowed to read a file with mode 0660", //
    permissionHandler.canReadFile(filePnfsId, subject_groupMember, origin) == AccessType.ACCESS_ALLOWED);

    assertTrue("Group member is allowed to write into a file with mode 0660", //
    permissionHandler.canWriteFile(filePnfsId, subject_groupMember, origin) == AccessType.ACCESS_ALLOWED);
    }
     */

    @Test
    public void testGroupCreate() throws Exception {
        final PnfsId parentPnfsId = new PnfsId("00006E4FCE51400C4FA38F2E10AAB52E6306");
        metadataSource.setMetaData(parentPnfsId, new FileMetaData(DIR, OWNER_UID, OWNER_GID, 0770));
        assertTrue("Group member is allowed to create a new directory in a parent with mode 0770", //
                permissionHandler.canCreateDir(parentPnfsId, subject_groupMember, origin) == AccessType.ACCESS_ALLOWED);
    }

    @Test
    public void testNegativeGroup() throws Exception {
        final PnfsId parentPnfsId = new PnfsId("00006E4FCE51400C4FA38F2E10AAB52E6306");
        metadataSource.setMetaData(parentPnfsId, new FileMetaData(DIR, OWNER_UID, OWNER_GID, 0707));
        assertTrue("Negative group member not allowed to create a new directory in a parent with mode 0707", //
                permissionHandler.canCreateDir(parentPnfsId, subject_groupMember, origin) == AccessType.ACCESS_DENIED);
    }

    @Test
    public void testNegativeOwner() throws Exception {
        final PnfsId parentPnfsId = new PnfsId("00006E4FCE51400C4FA38F2E10AAB52E6306");
        metadataSource.setMetaData(parentPnfsId, new FileMetaData(DIR, OWNER_UID, OWNER_GID, 0077));
        assertTrue("Negative owner not allowed to create a new directory in a parent with mode 0077", //
                permissionHandler.canCreateDir(parentPnfsId, subject_owner, origin) == AccessType.ACCESS_DENIED);
    }

    @Ignore
    // I guess we, should never allow ..... Ignore this test
    @Test
    public void testAnonymousWrite() throws Exception {
        final PnfsId parentPnfsId = new PnfsId("00006E4FCE51400C4FA38F2E10AAB52E6306");
        metadataSource.setMetaData(parentPnfsId, new FileMetaData(DIR, OWNER_UID, OWNER_GID, 0777));

        assertTrue("Anonymous not allowed to create a new files or directories",
                permissionHandler.canCreateDir(parentPnfsId, subject_anonymouos, origin) == AccessType.ACCESS_DENIED);

        assertTrue("Anonymous not allowed to create a new files or directories", //
                permissionHandler.canWriteFile(parentPnfsId, subject_anonymouos, origin) == AccessType.ACCESS_DENIED);
    }
}

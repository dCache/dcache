package org.dcache.tests.namespace;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import org.dcache.acl.ACL;
import org.dcache.acl.Origin;
import org.dcache.acl.enums.AccessType;
import org.dcache.acl.enums.AuthType;
import org.dcache.acl.enums.FileAttribute;
import org.dcache.acl.enums.RsType;
import org.dcache.acl.handler.singleton.AclHandler;
import org.dcache.acl.parser.ACLParser;
import org.dcache.tests.cells.CellAdapterHelper;
import org.dcache.tests.namespace.FileMetaDataProviderHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.security.auth.UnixNumericGroupPrincipal;
import com.sun.security.auth.UnixNumericUserPrincipal;
import javax.security.auth.Subject;
import java.security.Principal;

import diskCacheV111.services.acl.ACLPermissionHandler;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;
import org.dcache.commons.util.SqlHelper;

/**
 * @author Irina Kozlova, David Melkumyan
 *
 */

public class ACLPermissionHandlerTest {

    /***********************************************************************************************************************************************************
    * Constants
    */

    private final static String CELL_ARGS = //
        " -aclConnDriver=org.hsqldb.jdbcDriver"
        + " -aclConnUrl=jdbc:hsqldb:mem:chimeraaclmem"
        + " -aclConnUser=sa"
        + " -meta-data-provider=org.dcache.tests.namespace.FileMetaDataProviderHelper";

    private static final int UID = 111, GID = 1000;

    private static final String PREFIX_USER = "USER:" + UID + ":";

    /***********************************************************************************************************************************************************
    * Static member variables
    */
    private static Connection connection;

    private static CellAdapterHelper cell;
    private static ACLPermissionHandler permissionHandler;
    private static FileMetaDataProviderHelper metadataSource;

    private static FileMetaData fileMetadata, dirMetadata;

    private static Origin origin;
    private static Subject subject;

    /***********************************************************************************************************************************************************
    * Static methods
    */

    @BeforeClass
    public static void setUpClass() throws Exception {
        initConnection(); // Initialize connection to Chimera Database

        cell = new CellAdapterHelper("TestCell", CELL_ARGS); // Initialize dummy CellAdapter

        permissionHandler = new ACLPermissionHandler(cell); // Initialize ACL Permission Handler
        metadataSource = (FileMetaDataProviderHelper) permissionHandler.getMetadataSource(); // Initialize Metadata Source

        Properties aclProps = new Properties();

        aclProps.setProperty("aclConnDriver", "org.hsqldb.jdbcDriver");
        aclProps.setProperty("aclConnUrl", "jdbc:hsqldb:mem:chimeraaclmem");
        aclProps.setProperty("aclConnUser", "sa");

        AclHandler.setAclConfig(aclProps);

        origin = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, "127.0.0.1"); // Initialize origin

        Principal user = new UnixNumericUserPrincipal( UID );
        Principal group = new UnixNumericGroupPrincipal( GID, true );

        subject = new Subject();   // Initialize subject

        //Add principals to the subject:
        subject.getPrincipals().add(user);
        subject.getPrincipals().add(group);

        fileMetadata = new FileMetaData(false, UID, GID, 0600); // Initialize file metadata
        dirMetadata = new FileMetaData(true, UID, GID, 0600); // Initialize directory metadata
    }

    @AfterClass
    public static void tearDown() throws Exception {
        shutdownConnection(); // Shutdown connection to Chimera Database
        }

    /**
    * Initialize connection to Chimera Database
    *
    * @throws Exception
    */

    private static void initConnection() throws Exception {
        Class.forName("org.hsqldb.jdbcDriver");
        connection = DriverManager.getConnection("jdbc:hsqldb:mem:chimeraaclmem", "sa", "");

        BufferedReader dataStr = new BufferedReader(new FileReader("modules/external/Chimera/sql/create-hsqldb.sql"));
        StringBuilder sql = new StringBuilder();
        String inLine = null;
        while ((inLine = dataStr.readLine()) != null)
            sql.append(inLine);

        String[] statements = sql.toString().split(";");
        for (String statement : statements) {
            Statement st = connection.createStatement();
            st.executeUpdate(statement);
            SqlHelper.tryToClose(st);
        }
    }

    /**
    * Shutdown connection to Chimera Database
    *
    * @throws Exception
    */
    private static void shutdownConnection() throws Exception {
        connection.createStatement().execute("SHUTDOWN;");
        SqlHelper.tryToClose(connection);
    }

    private static void setACL(ACL acl) throws Exception {
        if (acl == null)
            throw new IllegalArgumentException("SetAcl failed: argument 'acl' is NULL.");

        final String rsId = acl.getRsId();

        ACL oldACL = AclHandler.getACL(rsId);
        if (oldACL != null) {
            int n = AclHandler.removeACL(rsId);
            Assert.assertTrue("Remove ACL failed! Returns: " + n, n == 1);
            try {
                Assert.assertTrue("Set ACL failed!", AclHandler.setACL(acl));

            } catch (Exception e) { // rollback old ACL
                AclHandler.setACL(oldACL);
            }

        } else
            Assert.assertTrue("Set ACL failed!", AclHandler.setACL(acl));

        Assert.assertEquals("Expected ACL is not equal to actual ACL!", //
                acl.toNFSv4String(), AclHandler.getACL(rsId).toNFSv4String());
    }

    /***********************************************************************************************************************************************************
    * Tests
    */

    @Test
    public void testReadFile() throws Exception {
        final PnfsId pnfsID = new PnfsId("0000416DFB43177548A8ADE89BAB82EC529C");
        metadataSource.setMetaData(pnfsID, fileMetadata);

        assertTrue("Read file should be undefined!", //
                permissionHandler.canReadFile(pnfsID, subject, origin) == AccessType.ACCESS_UNDEFINED);

        setACL(ACLParser.parseAdm(pnfsID.toIdString(), RsType.FILE, PREFIX_USER + "-r"));
        assertTrue("Read file should be denied!", //
                permissionHandler.canReadFile(pnfsID, subject, origin) == AccessType.ACCESS_DENIED);

        setACL(ACLParser.parseAdm(pnfsID.toIdString(), RsType.FILE, PREFIX_USER + "+r"));
        assertTrue("Read file should be allowed!", //
                permissionHandler.canReadFile(pnfsID, subject, origin) == AccessType.ACCESS_ALLOWED);
    }

    @Test
    public void testWriteFile() throws Exception {
        final PnfsId pnfsID = new PnfsId("0000416DFB43177548A8ADE89BAB82EC529C");
        metadataSource.setMetaData(pnfsID, fileMetadata);

        assertTrue("Write file should be undefined!", //
                permissionHandler.canWriteFile(pnfsID, subject, origin) == AccessType.ACCESS_UNDEFINED);

        setACL(ACLParser.parseAdm(pnfsID.toIdString(), RsType.FILE, PREFIX_USER + "-w"));
        assertTrue("Write file should be denied!", //
                permissionHandler.canWriteFile(pnfsID, subject, origin) == AccessType.ACCESS_DENIED);

        setACL(ACLParser.parseAdm(pnfsID.toIdString(), RsType.FILE, PREFIX_USER + "+w"));
        assertTrue("Write file should be allowed!", //
                permissionHandler.canWriteFile(pnfsID, subject, origin) == AccessType.ACCESS_ALLOWED);
    }

    @Test
    public void testCreateFile() throws Exception {
        //pnfsId of parent-directory:
        final PnfsId pnfsID = new PnfsId("00009C3FCDDC3FCDDC3FCDDC3FCDDC3FCDD7");
        metadataSource.setMetaData(pnfsID, dirMetadata);

        assertTrue("Create file should be undefined!", //
                permissionHandler.canCreateFile(pnfsID, subject, origin) == AccessType.ACCESS_UNDEFINED);

        setACL(ACLParser.parseAdm(pnfsID.toIdString(), RsType.DIR, PREFIX_USER + "-f"));
        assertTrue("Create file should be denied!", //
                permissionHandler.canCreateFile(pnfsID, subject, origin) == AccessType.ACCESS_DENIED);

        setACL(ACLParser.parseAdm(pnfsID.toIdString(), RsType.DIR, PREFIX_USER + "+f"));
        assertTrue("Create file should be allowed!", //
                permissionHandler.canCreateFile(pnfsID, subject, origin) == AccessType.ACCESS_ALLOWED);
    }

    @Test
    public void testCreateDir() throws Exception {
        //pnfsId of parent-directory:
        final PnfsId pnfsID = new PnfsId("00009C3FCDDC3FCDDC3FCDDC3FCDDC3FCDD7");
        metadataSource.setMetaData(pnfsID, dirMetadata);

        assertTrue("Create directory should be undefined!", //
                permissionHandler.canCreateDir(pnfsID, subject, origin) == AccessType.ACCESS_UNDEFINED);

        setACL(ACLParser.parseAdm(pnfsID.toIdString(), RsType.DIR, PREFIX_USER + "-s"));
        assertTrue("Create directory should be denied!", //
                permissionHandler.canCreateDir(pnfsID, subject, origin) == AccessType.ACCESS_DENIED);

        setACL(ACLParser.parseAdm(pnfsID.toIdString(), RsType.DIR, PREFIX_USER + "+s"));
        assertTrue("Create directory should be allowed!", //
                permissionHandler.canCreateDir(pnfsID, subject, origin) == AccessType.ACCESS_ALLOWED);
    }
    //testDeleteFile and testDeleteDir are commented for now.
    /*
    @Test
    public void testDeleteFile() throws Exception {
        //pnfsId of file:
        final PnfsId filePnfsID = new PnfsId("00007AFC6292C068435DA9B7661A716F2709");
        metadataSource.setMetaData(filePnfsID, fileMetadata);
        //pnfsId of parent-directory:
        final PnfsId parentDirPnfsID = new PnfsId("00007AFC62920000735DA000070000700007");
        metadataSource.setMetaData(parentDirPnfsID, dirMetadata);

        assertTrue("Delete file should be undefined!", //
                permissionHandler.canDeleteFile(filePnfsID, subject, origin) == AccessType.ACCESS_UNDEFINED);

        setACL(ACLParser.parseAdm(filePnfsID.toIdString(), RsType.FILE, PREFIX_USER + "+d"));
        setACL(ACLParser.parseAdm(parentDirPnfsID.toIdString(), RsType.DIR, PREFIX_USER + "+D"));
        assertTrue("Delete file should be allowed!", //
                permissionHandler.canDeleteFile(filePnfsID, subject, origin) == AccessType.ACCESS_ALLOWED);

        setACL(ACLParser.parseAdm(filePnfsID.toIdString(), RsType.FILE, PREFIX_USER + "-d"));
        setACL(ACLParser.parseAdm(parentDirPnfsID.toIdString(), RsType.DIR, PREFIX_USER + "+D"));
        assertTrue("Delete file should be denied!", //
                permissionHandler.canDeleteFile(filePnfsID, subject, origin) == AccessType.ACCESS_DENIED);

        setACL(ACLParser.parseAdm(filePnfsID.toIdString(), RsType.FILE, PREFIX_USER + "+d"));
        setACL(ACLParser.parseAdm(parentDirPnfsID.toIdString(), RsType.DIR, PREFIX_USER + "-D"));
        assertTrue("Delete file should be denied!", //
                permissionHandler.canDeleteFile(filePnfsID, subject, origin) == AccessType.ACCESS_DENIED);

        setACL(ACLParser.parseAdm(filePnfsID.toIdString(), RsType.FILE, PREFIX_USER + "-d"));
        setACL(ACLParser.parseAdm(parentDirPnfsID.toIdString(), RsType.DIR, PREFIX_USER + "-D"));
        assertTrue("Delete file should be denied!", //
                permissionHandler.canDeleteFile(filePnfsID, subject, origin) == AccessType.ACCESS_DENIED);
    }

    @Test
    public void testDeleteDir() throws Exception {
        //pnfsId of directory:
        final PnfsId dirPnfsID = new PnfsId("00007AFC6292C068435DA9B7661A716F2709");
        metadataSource.setMetaData(dirPnfsID, dirMetadata);
        //pnfsId of parent-directory:
        final PnfsId parentDirPnfsID = new PnfsId("00007AFC62920000735DA000070000700007");
        metadataSource.setMetaData(parentDirPnfsID, dirMetadata);

        assertTrue("Delete directory should be undefined!", //
                permissionHandler.canDeleteDir(dirPnfsID, subject, origin) == AccessType.ACCESS_UNDEFINED);

        setACL(ACLParser.parseAdm(dirPnfsID.toIdString(), RsType.FILE, PREFIX_USER + "+d"));
        setACL(ACLParser.parseAdm(parentDirPnfsID.toIdString(), RsType.DIR, PREFIX_USER + "+D"));
        assertTrue("Delete directory should be allowed!", //
                permissionHandler.canDeleteDir(dirPnfsID, subject, origin) == AccessType.ACCESS_ALLOWED);

        setACL(ACLParser.parseAdm(dirPnfsID.toIdString(), RsType.FILE, PREFIX_USER + "-d"));
        setACL(ACLParser.parseAdm(parentDirPnfsID.toIdString(), RsType.DIR, PREFIX_USER + "+D"));
        assertTrue("Delete directory should be denied!", //
                permissionHandler.canDeleteDir(dirPnfsID, subject, origin) == AccessType.ACCESS_DENIED);

        setACL(ACLParser.parseAdm(dirPnfsID.toIdString(), RsType.FILE, PREFIX_USER + "+d"));
        setACL(ACLParser.parseAdm(parentDirPnfsID.toIdString(), RsType.DIR, PREFIX_USER + "-D"));
        assertTrue("Delete directory should be denied!", //
                permissionHandler.canDeleteDir(dirPnfsID, subject, origin) == AccessType.ACCESS_DENIED);

        setACL(ACLParser.parseAdm(dirPnfsID.toIdString(), RsType.FILE, PREFIX_USER + "-d"));
        setACL(ACLParser.parseAdm(parentDirPnfsID.toIdString(), RsType.DIR, PREFIX_USER + "-D"));
        assertTrue("Delete directory should be denied!", //
                permissionHandler.canDeleteDir(dirPnfsID, subject, origin) == AccessType.ACCESS_DENIED);
    }
*/
    @Test
    public void testListDir() throws Exception {

        final PnfsId pnfsID = new PnfsId("00007AFC6292C068435DA9B7661A71655555");
        metadataSource.setMetaData(pnfsID, dirMetadata);

        assertTrue("List directory should be undefined!", //
                permissionHandler.canListDir(pnfsID, subject, origin) == AccessType.ACCESS_UNDEFINED);

        setACL(ACLParser.parseAdm(pnfsID.toIdString(), RsType.DIR, PREFIX_USER + "+l"));
        assertTrue("List directory should be allowed!", //
                permissionHandler.canListDir(pnfsID, subject, origin) == AccessType.ACCESS_ALLOWED);

        setACL(ACLParser.parseAdm(pnfsID.toIdString(), RsType.DIR, PREFIX_USER + "-l"));
        assertTrue("List directory should be denied!", //
                permissionHandler.canListDir(pnfsID, subject, origin) == AccessType.ACCESS_DENIED);
    }

    @Test
    public void testSetAttributes() throws Exception {

        final PnfsId pnfsID = new PnfsId("00007AFC6292C068435DA9B7661A71655555");
        metadataSource.setMetaData(pnfsID, dirMetadata);

        assertTrue("Set attributes should be undefined!", //
                permissionHandler.canSetAttributes(pnfsID, subject, origin, FileAttribute.FATTR4_ACL) == AccessType.ACCESS_UNDEFINED);

        setACL(ACLParser.parseAdm(pnfsID.toIdString(), RsType.DIR, PREFIX_USER + "+C"));
        assertTrue("Set attributes should be allowed!", //
                permissionHandler.canSetAttributes(pnfsID, subject, origin, FileAttribute.FATTR4_ACL) == AccessType.ACCESS_ALLOWED);

        setACL(ACLParser.parseAdm(pnfsID.toIdString(), RsType.DIR, PREFIX_USER + "-C"));
        assertTrue("Set attributes should be denied!", //
                permissionHandler.canSetAttributes(pnfsID, subject, origin, FileAttribute.FATTR4_ACL) == AccessType.ACCESS_DENIED);
    }

    @Test
    public void testGetAttributes() throws Exception {

        final PnfsId pnfsID = new PnfsId("00007AFC6292C068435DA9B7661A71655555");
        metadataSource.setMetaData(pnfsID, dirMetadata);

        assertTrue("Get attributes (read ACL) should be undefined!", //
                permissionHandler.canGetAttributes(pnfsID, subject, origin, FileAttribute.FATTR4_ACL) == AccessType.ACCESS_UNDEFINED);

        setACL(ACLParser.parseAdm(pnfsID.toIdString(), RsType.DIR, PREFIX_USER + "+c"));
        assertTrue("Get attributes (read ACL) should be allowed!", //
                permissionHandler.canGetAttributes(pnfsID, subject, origin, FileAttribute.FATTR4_ACL) == AccessType.ACCESS_ALLOWED);

        setACL(ACLParser.parseAdm(pnfsID.toIdString(), RsType.DIR, PREFIX_USER + "-c"));
        assertTrue("Get attributes (read ACL) should be denied!", //
                permissionHandler.canGetAttributes(pnfsID, subject, origin, FileAttribute.FATTR4_ACL) == AccessType.ACCESS_DENIED);
    }

}
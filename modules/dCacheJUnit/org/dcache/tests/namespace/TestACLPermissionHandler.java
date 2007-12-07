package org.dcache.tests.namespace;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.dcache.chimera.acl.ACE;
import org.dcache.chimera.acl.ACL;
import org.dcache.chimera.acl.Origin;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.AccessMask;
import org.dcache.chimera.acl.enums.AceType;
import org.dcache.chimera.acl.enums.AuthType;
import org.dcache.chimera.acl.enums.InetAddressType;
import org.dcache.chimera.acl.enums.RsType;
import org.dcache.chimera.acl.enums.Who;
import org.dcache.chimera.acl.handler.AclHandler;

import diskCacheV111.services.ACLPermissionHandler;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FileMetaDataX;
import diskCacheV111.util.PnfsId;

public class TestACLPermissionHandler {


    private static final AuthType authTypeCONST=AuthType.ORIGIN_AUTHTYPE_STRONG;
    private static final InetAddressType inetAddressTypeCONST=InetAddressType.IPv4;
    private static final String hostCONST="127.0.0.1";

    private static Connection _conn;

	private final FileMetaDataProviderHelper _metaDataSource = new FileMetaDataProviderHelper();
	private final static String aclProperties = "modules/dCacheJUnit/org/dcache/tests/namespace/acl.properties";

    private final ACLPermissionHandler _permissionHandler = new ACLPermissionHandler(null, _metaDataSource, aclProperties);

    private final AclHandler aclHandler = new AclHandler(aclProperties);


    @BeforeClass
    public static void setUp() throws Exception {

    	/*
         * init Chimera DB
         */

        Class.forName("org.hsqldb.jdbcDriver");

        _conn = DriverManager.getConnection("jdbc:hsqldb:mem:chimeraaclmem", "sa", "");

        File sqlFile = new File("modules/external/Chimera/sql/create-hsqldb.sql");
        StringBuilder sql = new StringBuilder();

        BufferedReader dataStr = new BufferedReader(new FileReader(sqlFile));
        String inLine = null;

        while ((inLine = dataStr.readLine()) != null) {
            sql.append(inLine);
        }

        Statement st = _conn.createStatement();

        st.executeUpdate(sql.toString());

        tryToClose(st);

    }

    @AfterClass
    public static void tearDown() throws Exception {

        _conn.createStatement().execute("SHUTDOWN;");
        _conn.close();
    }


    @Before
    public void reset() {
        _metaDataSource.cleanAll();
    }

    @Test
    public void testReadFile() throws Exception {

        boolean isAllowed = false;
        String fileId =  "0000416DFB43177548A8ADE89BAB82EC529C";

        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
                0,
                AccessMask.READ_DATA.getValue(),
                Who.USER,
                111,
                ACE.DEFAULT_ADDRESS_MSK,
                0 ) );


        ACL acl = new ACL(fileId, RsType.FILE, aces);

        aclHandler.setACL(acl);

        FileMetaDataX fileMetaData = new FileMetaDataX(new PnfsId(fileId),
        		new FileMetaData(false, 111, 1000, 0600) );

        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);
        Subject subject = new Subject(111, 1000);

        _metaDataSource.setXMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        isAllowed =  _permissionHandler.canReadFile(subject, "/pnfs/desy.de/data/privateFile", origin);

        assertTrue("It is allowed to read file", isAllowed);

    }

    @Test
    public void testWriteFile() throws Exception {

        boolean isAllowed = false;
        String fileId =  "00006E4FCE51400C4FA38F2E10AAB52E6306";

        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
                0,
                AccessMask.WRITE_DATA.getValue(),
                Who.USER,
                111,
                ACE.DEFAULT_ADDRESS_MSK,
                0 ) );

        ACL acl = new ACL(fileId, RsType.FILE, aces);

        aclHandler.setACL(acl);

        FileMetaDataX fileMetaData = new FileMetaDataX(new PnfsId(fileId),
        		new FileMetaData(false, 111, 1000, 0600) );

        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);
        Subject subject = new Subject(111, 1000);

        _metaDataSource.setXMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        isAllowed =  _permissionHandler.canWriteFile(subject, "/pnfs/desy.de/data/privateFile", origin);

        assertTrue("It is allowed to write to a file", isAllowed);

    }
    
    @Test
    public void testCreateDir() throws Exception {

        boolean isAllowed = false;
        String dirId =  "000088AAB6D5022F4A69BC2D4576828EF12B";
        String parentDirId =  "000088AAB6D512B12B12B12B12B12B12B12B";
        
        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
				1,
				AccessMask.ADD_SUBDIRECTORY.getValue(),
				Who.USER,
				111,
				ACE.DEFAULT_ADDRESS_MSK,
				0 ) );
     
        
        //In reality, acl exists only for parentDirId:
        ACL acl = new ACL(parentDirId, RsType.DIR, aces);

        aclHandler.setACL(acl);

        //here 'true' means this is a 'Directory'. 
        //In reality, PnfId is not defined at all here, as we just ask to create an object.
        //That is, dirId is only for the test here.
        FileMetaDataX fileMetaData = new FileMetaDataX(new PnfsId(dirId),
        		new FileMetaData(true, 111, 1000, 0600) );

        //define parent directory with pnfsId, which is the pnfsId that will be checked by ACL
        FileMetaDataX parentMetaData = new FileMetaDataX(new PnfsId(parentDirId),
        		new FileMetaData(true, 111, 1000, 0600) );
        
        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);
        Subject subject = new Subject(111, 1000);

        _metaDataSource.setXMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setXMetaData("/pnfs/desy.de/data/privateDir", fileMetaData);

        isAllowed =  _permissionHandler.canCreateDir(subject, "/pnfs/desy.de/data/privateDir", origin);

        assertTrue("It is allowed to create a directory", isAllowed);

    }
    
    @Test
    public void testCreateFile() throws Exception {

        boolean isAllowed = false;
        //file to create. Actually, fileId does not exist :
        String fileId =  "00009C3FCDDB7FC74D38A3DFE77EA77A8EB3"; 
        
        //Directory where file has to be created. Permission to perform action 'CREATE' 
        //will be checked for this directory  parentDirId :
        String parentDirId =  "00009C3FCDDC3FCDDC3FCDDC3FCDDC3FCDD7";
        
        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
				1,
				AccessMask.ADD_FILE.getValue(),
				Who.USER,
				111,
				ACE.DEFAULT_ADDRESS_MSK,
				0 ) );
        //just add some more ACEs, deny to add a directory:  
        aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
                1,
                AccessMask.ADD_SUBDIRECTORY.getValue(),
                Who.USER,
                111,
                ACE.DEFAULT_ADDRESS_MSK,
                1 ) );
        
        //In reality, acl exists only for parentDirId:
        ACL acl = new ACL(parentDirId, RsType.DIR, aces);

        aclHandler.setACL(acl);

        //here 'false' means this is a 'File'. 
        //In reality, PnfId is not defined at all here, as we just ask to create an object.
        //That is, fileId is only for the test here.
        FileMetaDataX fileMetaData = new FileMetaDataX(new PnfsId(fileId),
        		new FileMetaData(false, 111, 1000, 0600) );

        //here 'true' means this is a 'Directory'. 
        //Define parent directory with pnfsId parentDirId. 
        //ACL of this Id will be checked to allow/deny a permission
        FileMetaDataX parentMetaData = new FileMetaDataX(new PnfsId(parentDirId),
        		new FileMetaData(true, 111, 1000, 0600) );
        
        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);
        Subject subject = new Subject(111, 1000);

        _metaDataSource.setXMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setXMetaData("/pnfs/desy.de/data/newPrivateFile", fileMetaData);

        isAllowed =  _permissionHandler.canCreateFile(subject, "/pnfs/desy.de/data/newPrivateFile", origin);

        assertTrue("It is allowed to create a directory", isAllowed);

    }
    
    
    
    static void tryToClose(Statement o) {
        try {
            if (o != null)
                o.close();
        } catch (SQLException e) {
            // _logNamespace.error("tryToClose PreparedStatement", e);
        }
    }

}
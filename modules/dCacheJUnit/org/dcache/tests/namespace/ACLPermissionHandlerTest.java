package org.dcache.tests.namespace;

import static org.junit.Assert.assertFalse;
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
import org.dcache.chimera.acl.Owner;
import org.dcache.chimera.acl.Permission;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.AccessMask;
import org.dcache.chimera.acl.enums.AceType;
import org.dcache.chimera.acl.enums.Action;
import org.dcache.chimera.acl.enums.AuthType;
import org.dcache.chimera.acl.enums.FileAttribute;
import org.dcache.chimera.acl.enums.RsType;
import org.dcache.chimera.acl.enums.Who;
import org.dcache.chimera.acl.handler.AclHandler;
import org.dcache.chimera.acl.mapper.AclMapper;
import org.dcache.chimera.acl.matcher.AclNFSv4Matcher;
import org.dcache.tests.cells.CellAdapterHelper;

import diskCacheV111.services.acl.ACLPermissionHandler;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FileMetaDataX;
import diskCacheV111.util.PnfsId;

public class ACLPermissionHandlerTest {


    private static final AuthType authTypeCONST=AuthType.ORIGIN_AUTHTYPE_STRONG;
    //private static final InetAddressType inetAddressTypeCONST=InetAddressType.IPv4;
    private static final String hostCONST="127.0.0.1";

    private static Connection _conn;


    private final static String aclProperties = "modules/dCacheJUnit/org/dcache/tests/namespace/acl.properties";
	private final static String cellArgs =
		" -acl-permission-handler-config=" + aclProperties +
		" -meta-data-provider=org.dcache.tests.namespace.FileMetaDataProviderHelper";

	private final static CellAdapterHelper _dummyCell = new CellAdapterHelper("aclTtestCell", cellArgs) ;
	private final FileMetaDataProviderHelper _metaDataSource = new FileMetaDataProviderHelper(_dummyCell);

    private ACLPermissionHandler _permissionHandler;

    private AclHandler _aclHandler; 


    @BeforeClass
    public static void setUpClass() throws Exception {

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
    public void setUp() throws Exception
    {
    	_permissionHandler = new ACLPermissionHandler(_dummyCell);
        _metaDataSource.cleanAll();
        _aclHandler = new AclHandler(aclProperties);
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

        _aclHandler.setACL(acl);

        FileMetaDataX fileMetaData = new FileMetaDataX(new PnfsId(fileId),
        		new FileMetaData(false, 111, 1000, 0600) );

        Origin origin = new Origin(authTypeCONST, hostCONST);
        Subject subject = new Subject(111, 1000);

        _metaDataSource.setXMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        isAllowed =  _permissionHandler.canReadFile("/pnfs/desy.de/data/privateFile", subject, origin);

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

        _aclHandler.setACL(acl);

        FileMetaDataX fileMetaData = new FileMetaDataX(new PnfsId(fileId),
        		new FileMetaData(false, 111, 1000, 0600) );

        Origin origin = new Origin(authTypeCONST, hostCONST);
        Subject subject = new Subject(111, 1000);

        _metaDataSource.setXMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        isAllowed =  _permissionHandler.canWriteFile("/pnfs/desy.de/data/privateFile", subject, origin);

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

        _aclHandler.setACL(acl);

        //here 'true' means this is a 'Directory'.
        //In reality, PnfId is not defined at all here, as we just ask to create an object.
        //That is, dirId is only for the test here.
        FileMetaDataX fileMetaData = new FileMetaDataX(new PnfsId(dirId),
        		new FileMetaData(true, 111, 1000, 0600) );

        //define parent directory with pnfsId, which is the pnfsId that will be checked by ACL
        FileMetaDataX parentMetaData = new FileMetaDataX(new PnfsId(parentDirId),
        		new FileMetaData(true, 111, 1000, 0600) );

        Origin origin = new Origin(authTypeCONST, hostCONST);
        Subject subject = new Subject(111, 1000);

        _metaDataSource.setXMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setXMetaData("/pnfs/desy.de/data/privateDir", fileMetaData);

        isAllowed =  _permissionHandler.canCreateDir("/pnfs/desy.de/data/privateDir", subject, origin);

        assertTrue("It is allowed to create a directory", isAllowed);

    }

    @Test
    public void testCreateFile() throws Exception {

        boolean isAllowed = false;
        //file to create. Actually, fileId does not exist . Only for test:
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

        _aclHandler.setACL(acl);

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

        Origin origin = new Origin(authTypeCONST,  hostCONST);
        Subject subject = new Subject(111, 1000);

        _metaDataSource.setXMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setXMetaData("/pnfs/desy.de/data/newPrivateFile", fileMetaData);

        isAllowed =  _permissionHandler.canCreateFile("/pnfs/desy.de/data/newPrivateFile", subject, origin);

        assertTrue("It is allowed to create a directory", isAllowed);

    }

    @Test
    public void testDeleteFile() throws Exception {

        boolean isAllowed = false;
        //File to delete
        String fileId =  "00007AFC6292C068435DA9B7661A716F2709";

        //Parent directory
        String parentDirId =  "00007AFC62920000735DA000070000700007";

        //Set ACL for the File
        List<ACE> acesForFile = new ArrayList<ACE>();

        acesForFile.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
                0,
                AccessMask.DELETE.getValue(),
                Who.USER,
                111,
                ACE.DEFAULT_ADDRESS_MSK,
                0 ) );


        ACL aclForFile = new ACL(fileId, RsType.FILE, acesForFile);

        _aclHandler.setACL(aclForFile);

      //Set ACL for the parent directory
        List<ACE> acesForDir = new ArrayList<ACE>();

        acesForDir.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
                1,
                AccessMask.DELETE_CHILD.getValue(),
                Who.USER,
                111,
                ACE.DEFAULT_ADDRESS_MSK,
                0 ) );


        ACL aclForDir = new ACL(parentDirId, RsType.DIR, acesForDir);

        _aclHandler.setACL(aclForDir);

        //Define metadata for the File
        FileMetaDataX fileMetaData = new FileMetaDataX(new PnfsId(fileId),
        		new FileMetaData(false, 111, 1000, 0600) );

       //Define metadata for the parent Directory
        FileMetaDataX dirMetaData = new FileMetaDataX(new PnfsId(parentDirId),
        		new FileMetaData(true, 111, 1000, 0600) );

        //Define Origin for the user. (Subject user_id=111)
        Origin origin = new Origin(authTypeCONST,  hostCONST);
        Subject subject = new Subject(111, 1000);

        //Set metadata for the File and for the parent Directory
        _metaDataSource.setXMetaData("/pnfs/desy.de/data/dir1/privateFile", fileMetaData);
        _metaDataSource.setXMetaData("/pnfs/desy.de/data/dir1", dirMetaData);

        isAllowed =  _permissionHandler.canDeleteFile("/pnfs/desy.de/data/dir1/privateFile", subject, origin);

        assertTrue("It is allowed to delete this file", isAllowed);

    }

    @Test
    public void testDeleteDirectory() throws Exception {

        boolean isAllowed = false;
        //Directory to delete
        String dirId =  "0000FF2A3233948A4692A5F5EB22F60C4F05";

        //Parent directory
        String parentDirId =  "0000FF2A323390000FF2A325EB0000FF2A32";

        //Set ACL for the directory (directory to be deleted)
        List<ACE> acesForDir = new ArrayList<ACE>();

        acesForDir.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
                1,
                AccessMask.DELETE.getValue(),
                Who.USER,
                111,
                ACE.DEFAULT_ADDRESS_MSK,
                0 ) );

        ACL aclForDir = new ACL(dirId, RsType.DIR, acesForDir);

        _aclHandler.setACL(aclForDir);

       //Set ACL for the parent directory
        List<ACE> acesForParentDir = new ArrayList<ACE>();

        acesForParentDir.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
                1,
                AccessMask.DELETE_CHILD.getValue(),
                Who.USER,
                111,
                ACE.DEFAULT_ADDRESS_MSK,
                0 ) );


        ACL aclForParentDir = new ACL(parentDirId, RsType.DIR, acesForParentDir);

        _aclHandler.setACL(aclForParentDir);

        //Define metadata for the directory
        FileMetaDataX dirMetaData = new FileMetaDataX(new PnfsId(dirId),
        		new FileMetaData(true, 111, 1000, 0600) );

        //Define metadata for the parent Directory
        FileMetaDataX parentDirMetaData = new FileMetaDataX(new PnfsId(parentDirId),
        		new FileMetaData(true, 111, 1000, 0600) );

       //Define Origin for the user.
        Origin origin = new Origin(authTypeCONST,  hostCONST);
        Subject subject = new Subject(111, 1000);

        //Set metadata for the directory and for the parent directory
        _metaDataSource.setXMetaData("/pnfs/desy.de/data/dir1", dirMetaData);
        _metaDataSource.setXMetaData("/pnfs/desy.de/data", parentDirMetaData);

        //permission to delete this directory
        isAllowed =  _permissionHandler.canDeleteDir("/pnfs/desy.de/data/dir1", subject, origin);

        assertTrue("It is allowed to delete a directory", isAllowed);

    }

/////////////////////////////////////////////
/*
    @Test
    public void testSetAttributesFile() throws Exception {

    	boolean isAllowed = false;

    	//File to set attributes
        String fileId =  "0000FF948A460C4F052A3233948A460C4F05";

                List<ACE> aces = new ArrayList<ACE>();

                int
                masks = (AccessMask.WRITE_ATTRIBUTES.getValue());
                masks |= (AccessMask.WRITE_ACL.getValue());
                masks |= (AccessMask.WRITE_OWNER.getValue());

               aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
                        0,
                        masks,
                        Who.USER,
                        111,
                        ACE.DEFAULT_ADDRESS_MSK,
                        0 ) );

               aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
                       0,
                       AccessMask.WRITE_DATA.getValue(),
                       Who.USER,
                       111,
                       ACE.DEFAULT_ADDRESS_MSK,
                       1 ) );
               
              // aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
               //        0,
                //       AccessMask.WRITE_ACL.getValue(),
                //       Who.USER,
                //       111,
               //       ACE.DEFAULT_ADDRESS_MSK,
                //     0 ) );
               
               ACL newACL = new ACL(fileId, RsType.FILE, aces);

               _aclHandler.setACL(newACL);

             //Define metadata for the file
               FileMetaDataX fileMetaData = new FileMetaDataX(new PnfsId(fileId),
               		new FileMetaData(true, 111, 1000, 0600) );

		     		Subject subject = new Subject(111,1000);

		     		//Define Origin for the user.
		            Origin origin = new Origin(authTypeCONST, hostCONST);

		          //Set metadata for the file
		           _metaDataSource.setXMetaData("/pnfs/desy.de/data/filename", fileMetaData);

		           //permission to set attributes for the file.
		           //Check SETATTR (Attribute ACL). Access flag: WRITE_ACL
		           isAllowed =  _permissionHandler.canSetAttributes("/pnfs/desy.de/data/filename", subject, origin, FileAttribute.FATTR4_ACL);

		           assertTrue("It is allowed to set attributes  FATTR4_ACL ", isAllowed);

		           // next check
		           //Check SETATTR (Attribute OWNER_GROUP). Access flag: WRITE_OWNER
		           isAllowed =  _permissionHandler.canSetAttributes("/pnfs/desy.de/data/filename", subject, origin, FileAttribute.FATTR4_OWNER_GROUP);

		           assertTrue("It is allowed to set attributes  OWNER_GROUP ", isAllowed);

		           //next check
		           //Check SETATTR (Attributes OWNER_GROUP and OWNER). Access flag: WRITE_OWNER

		          int fileAttrTest = (FileAttribute.FATTR4_OWNER_GROUP.getValue());
		                 fileAttrTest|=(FileAttribute.FATTR4_OWNER.getValue());

		          isAllowed =  _permissionHandler.canSetAttributes("/pnfs/desy.de/data/filename", subject, origin, FileAttribute.valueOf(fileAttrTest));

				  assertTrue("It is allowed to set attributes  OWNER_GROUP and OWNER ", isAllowed);


		           //Check SETATTR (Attribute SIZE). Access flag: WRITE_DATA (is denied)
				   isAllowed =  _permissionHandler.canSetAttributes("/pnfs/desy.de/data/filename", subject, origin, FileAttribute.FATTR4_SIZE);

		           assertFalse("It is allowed to set attributes  FATTR4_SIZE ", isAllowed);

    }
*/

/////////////////////////////////////////////
/*
    @Test
    public void testGetAttributes() throws Exception {

    	boolean isAllowed = false;

    	//File pnfsID
        String fileId =  "0000FF948A460C4F00000FF9948A40000FF9";

                List<ACE> aces = new ArrayList<ACE>();
               
                //for user who_id=111: READ_ACL allowed, READ_ATTRIBUTES denied 
               aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
                        0,
                        AccessMask.READ_ACL.getValue(),
                        Who.USER,
                        111,
                        ACE.DEFAULT_ADDRESS_MSK,
                        0 ) );

               aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
                       0,
                       AccessMask.READ_ATTRIBUTES.getValue(),
                       Who.USER,
                       111,
                       ACE.DEFAULT_ADDRESS_MSK,
                       1 ) );
               
               //for user who_id=222: READ_ACL denied, READ_ATTRIBUTES allowed 
               aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
                       0,
                       AccessMask.READ_ACL.getValue(),
                       Who.USER,
                       222,
                       ACE.DEFAULT_ADDRESS_MSK,
                       2 ) );

              aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
                      0,
                      AccessMask.READ_ATTRIBUTES.getValue(),
                      Who.USER,
                      222,
                      ACE.DEFAULT_ADDRESS_MSK,
                      3 ) );
              
               ACL newACL = new ACL(fileId, RsType.FILE, aces);

               _aclHandler.setACL(newACL);

             //Define metadata for the file
               FileMetaDataX fileMetaData = new FileMetaDataX(new PnfsId(fileId),
               		new FileMetaData(true, 111, 1000, 0600) );

		     		Subject subject = new Subject(111,1000);

		     		//Define Origin for the user.
		            Origin origin = new Origin(authTypeCONST,  hostCONST);

		          //Set metadata for the file
		           _metaDataSource.setXMetaData("/pnfs/desy.de/data/filename", fileMetaData);

		        
		           //Check GETATTR (Attribute ACL). 
		           isAllowed =  _permissionHandler.canGetAttributes( "/pnfs/desy.de/data/filename", subject, origin, FileAttribute.FATTR4_ACL);

		           assertTrue("It is allowed to get attribute FATTR4_ACL (read ACL) ", isAllowed);

		          
		           //Check GETATTR (Attribute OWNER_GROUP).
		           isAllowed =  _permissionHandler.canGetAttributes("/pnfs/desy.de/data/filename", subject, origin, FileAttribute.FATTR4_OWNER_GROUP);

		           assertFalse("It is NOT allowed to get attribute OWNER_GROUP, as bit READ_ATTRIBUTES is denied", isAllowed);

		      
		           //Check GETATTR (Attributes OWNER_GROUP and OWNER). 

		           int fileAttrTest = (FileAttribute.FATTR4_OWNER_GROUP.getValue());
		                 fileAttrTest|=(FileAttribute.FATTR4_OWNER.getValue());

		           isAllowed =  _permissionHandler.canGetAttributes("/pnfs/desy.de/data/filename", subject, origin, FileAttribute.valueOf(fileAttrTest));

				   assertFalse("It is NOT allowed to get attributes  OWNER_GROUP and OWNER, as bit READ_ATTRIBUTES is denied ", isAllowed);


		           //Check GETATTR (Attribute SIZE). 
				   isAllowed =  _permissionHandler.canGetAttributes("/pnfs/desy.de/data/filename", subject, origin, FileAttribute.FATTR4_SIZE);

		           assertFalse("It is NOT allowed to get attribute  FATTR4_SIZE, as bit READ_ATTRIBUTES is denied ", isAllowed);

		           ///////////////////////////////////////////////////
		           
		           //Take user who_id=222, where READ_ACL denied, READ_ATTRIBUTES allowed
		       	   Subject subject2 = new Subject(222,1000);
		           
		       	   //Check GETATTR (Attribute ACL). 
		           isAllowed =  _permissionHandler.canGetAttributes("/pnfs/desy.de/data/filename", subject2, origin, FileAttribute.FATTR4_ACL);

		           assertFalse("For who_id=222: It is NOT allowed to get attribute FATTR4_ACL (read ACL), as READ_ACL is denied ", isAllowed);

		          
		           //Check GETATTR (Attribute FATTR4_ARCHIVE).
		           isAllowed =  _permissionHandler.canGetAttributes("/pnfs/desy.de/data/filename", subject2, origin, FileAttribute.FATTR4_ARCHIVE);

		           assertTrue("For who_id=222: It is allowed to get attribute FATTR4_ARCHIVE, as READ_ATTRIBUTES is allowed", isAllowed);
		           
		           //Check GETATTR (Attributes OWNER_GROUP and OWNER).
		           isAllowed =  _permissionHandler.canGetAttributes("/pnfs/desy.de/data/filename", subject2, origin, FileAttribute.valueOf(fileAttrTest));

				   assertTrue("For who_id=222: It is allowed to get attributes  OWNER_GROUP and OWNER, as READ_ATTRIBUTES is allowed ", isAllowed);


		           //Check GETATTR (Attribute FATTR4_TIME_MODIFY_SET). 
				   isAllowed =  _permissionHandler.canGetAttributes("/pnfs/desy.de/data/filename", subject2, origin, FileAttribute.FATTR4_TIME_MODIFY_SET);

		           assertTrue("For who_id=222: It is allowed to get attribute  FATTR4_TIME_MODIFY_SET, as READ_ATTRIBUTES is allowed ", isAllowed);
    }
*/
    static void tryToClose(Statement o) {
        try {
            if (o != null)
                o.close();
        } catch (SQLException e) {
            // _logNamespace.error("tryToClose PreparedStatement", e);
        }
    }

}
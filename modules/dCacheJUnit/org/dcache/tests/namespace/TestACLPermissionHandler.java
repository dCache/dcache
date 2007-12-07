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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.junit.BeforeClass;
import org.junit.Test;

import diskCacheV111.services.ACLPermissionHandler;
import diskCacheV111.services.FileMetaDataSource;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FileMetaDataX;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsId;

public class TestACLPermissionHandler {
	
	
    private static final AuthType authTypeCONST=AuthType.ORIGIN_AUTHTYPE_STRONG;
    private static final InetAddressType inetAddressTypeCONST=InetAddressType.IPv4;
    private static final String hostCONST="127.0.0.1";
	
    private static Connection _conn;
    
	private final static DummyMetadataSource _metaDataSource = new DummyMetadataSource();
	private final static String aclProperties = "modules/dCacheJUnit/org/dcache/tests/namespace/acl.properties";
	
    private final ACLPermissionHandler _permissionHandler = new ACLPermissionHandler(null, _metaDataSource, aclProperties);
    
    private final AclHandler aclHandler = new AclHandler(aclProperties);
   
    
    private static class UserRecord {
        private final int _uid;
        private final int _gid;
        private final int[] _gids;


        UserRecord(int uid, int gid, int[] gids) {
            _uid = uid;
            _gid = gid;
            _gids = gids;
        }


        public int getUid() {
            return _uid;
        }


        public int getGid() {
            return _gid;
        }


        public int[] getGids() {
            return _gids;
        }


    }
    
    private static class DummyMetadataSource implements FileMetaDataSource {


        private final Map<PnfsId, FileMetaData> _metadataById = new HashMap<PnfsId, FileMetaData>();
        private final Map<String, FileMetaData> _metadataByPath = new HashMap<String, FileMetaData>();

        private final Map<PnfsId, FileMetaDataX> _metadataXById = new HashMap<PnfsId, FileMetaDataX>();
        private final Map<String, FileMetaDataX> _metadataXByPath = new HashMap<String, FileMetaDataX>();

        public FileMetaData getMetaData(String path) throws CacheException {

            FileMetaData metaData = _metadataByPath.get(path);

            if( metaData == null ) {
                throw new FileNotFoundCacheException(path + " not found");
            }
            return metaData;
        }

        public FileMetaData getMetaData(PnfsId pnfsId) throws CacheException {

            FileMetaData metaData = _metadataById.get(pnfsId);

            if( metaData == null ) {
                throw new FileNotFoundCacheException(pnfsId + " not found");
            }
            return metaData;
        }


        public FileMetaDataX getXMetaData(String path) throws CacheException {

            FileMetaDataX metaData = _metadataXByPath.get(path);

            if( metaData == null ) {
                throw new FileNotFoundCacheException(path + " not found");
            }
            return metaData;
        }

        public FileMetaDataX getXMetaData(PnfsId pnfsId) throws CacheException {

            FileMetaDataX metaData = _metadataXById.get(pnfsId);

            if( metaData == null ) {
                throw new FileNotFoundCacheException(pnfsId + " not found");
            }
            return metaData;
        }

        public void setMetaData(PnfsId pnfsId,FileMetaData metaData ) {
            _metadataById.put(pnfsId, metaData);
        }


        public void setMetaData(String path,FileMetaData metaData ) {
            _metadataByPath.put(path, metaData);
        }        
        
        public void setXMetaData(PnfsId pnfsId,FileMetaDataX metaData ) {
            _metadataXById.put(pnfsId, metaData);
        }


        public void setXMetaData(String path,FileMetaDataX metaData ) {
            _metadataXByPath.put(path, metaData);
        }        

        public void cleanAll() {
            _metadataById.clear();
            _metadataByPath.clear();
            _metadataXById.clear();
            _metadataXByPath.clear();

        }		
		
		
    }

    
    @BeforeClass
    public static void setUp() throws Exception {
        
    	_metaDataSource.cleanAll();
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

        
        org.dcache.chimera.acl.ACL acl = new ACL(fileId, RsType.FILE, aces); 
        
        aclHandler.setACL(acl);
        
        //FileMetaData parentMetaData =  new FileMetaData(true, 111, 0, 0755);
        FileMetaDataX fileMetaData = new FileMetaDataX(new PnfsId(fileId),
        		new FileMetaData(false, 111, 1000, 0600) );

        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);
        Subject subject = new Subject(111, 1000);
        
        //_metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setXMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);
            
        //canReadFile(int userUid, int[] userGids, String pnfsPath, Origin userOrigin)
        isAllowed =  _permissionHandler.canReadFile(subject, "/pnfs/desy.de/data/privateFile", origin);

        assertTrue("It is allowed to read file", isAllowed);

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
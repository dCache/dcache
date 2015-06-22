package diskCacheV111.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.PatternSyntaxException;

import org.dcache.auth.FQAN;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CheckStagePermissionTests {

    public static final String TEST_PREFIX = "stagePermissionFile";
    public static final FQAN VALID_FQAN = new FQAN("/atlas/Role=production");

    public static final FQAN OTHER_VALID_FQAN = new FQAN("/atlas");

    public static final String VALID_DN = "/DC=org/DC=example/CN=test user";

    public static final String TEST_DN = "/DC=org/DC=example/.*";
    public static final FQAN TEST_FQAN = new FQAN("/atlas/Role=.*");

    public static final String USER_TEST1_DN = "/DC=org/DC=example/CN=test";
    public static final FQAN USER_TEST1_FQAN = new FQAN("/atlas/Role=production");

    public static final String USER_TEST2_DN = "/DC=org/DC=anotherExample/CN=test";

    public static final String VALID_STOREUNIT = "sql:chimera@osm";

    File _testConfigFile;
    CheckStagePermission _check;

    @Before
    public void setUp() throws IOException {
        _testConfigFile = File.createTempFile( TEST_PREFIX, null, null);
        saveContents( "");
        _check = new CheckStagePermission( _testConfigFile.getCanonicalPath());
    }

    @After
    public void tearDown() {
        _testConfigFile.delete();
    }

    // those 2 tests below were adopted for testing the method canPerformStaging(String dn, String fqan, String StoreUnit)
    @Test
    public void testEmptyFilepath() throws PatternSyntaxException, IOException {
        CheckStagePermission check = new CheckStagePermission( "");
        assertTrue( "Empty filepath to config file allows user with DN and FQAN", check.canPerformStaging( VALID_DN,
                                                                                                           VALID_FQAN, VALID_STOREUNIT));
        assertTrue( "Empty filepath to config file allows user with DN", check.canPerformStaging( VALID_DN, null, VALID_STOREUNIT));
    }

    @Test
    public void testNullFilepath() throws PatternSyntaxException, IOException {
        CheckStagePermission check = new CheckStagePermission( null);
        assertTrue( "Null filepath to config file allows user with DN and FQAN", check.canPerformStaging( VALID_DN,
                                                                                                          VALID_FQAN, VALID_STOREUNIT));
        assertTrue( "Null filepath to config file allows user with DN", check.canPerformStaging( VALID_DN, null, VALID_STOREUNIT));
    }

    @Test
    public void testInitialShouldRead() {
        assertTrue( "no initial read required", _check.fileNeedsRereading());
    }

    @Test
    public void testNoReadAfterInitialRead() throws PatternSyntaxException, IOException {
        _check.rereadConfig();
        assertFalse( "read still required after initial read", _check.fileNeedsRereading());
    }

    @Test
    public void testRereadNeededAfterTouch() throws PatternSyntaxException, IOException, InterruptedException {
        _check.rereadConfig();
        // Sleep for 1s to allow filesystem to notice a filechange.
        Thread.sleep(1000);
        saveContents("");
        assertTrue( "no read required after touch", _check.fileNeedsRereading());
    }

    //those 14 tests below were adopted for testing the method canPerformStaging(String dn, String fqan, String StoreUnit)
    @Test
    public void testZeroLengthDNEmptyFile() throws PatternSyntaxException, IOException {
        assertFalse( "user with ZeroLength DN and FQAN can not stage with empty file", _check.canPerformStaging("",
                                                                                                                VALID_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testZeroLengthDN() throws PatternSyntaxException, IOException {
        authoriseDn( VALID_DN );
        assertFalse( "user with ZeroLengthDN and FQAN can not stage", _check.canPerformStaging("", VALID_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testZeroLengthDNAndFqan() throws PatternSyntaxException, IOException {
        authoriseDnFqan( VALID_DN, VALID_FQAN);
        assertFalse( "user with ZeroLengthDN and FQAN can not stage", _check.canPerformStaging("", VALID_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testNullFqan() throws PatternSyntaxException, IOException {
        authoriseDn( VALID_DN );
        assertTrue( "user with DN and FQAN=null staging when DN is in file", _check.canPerformStaging( VALID_DN, null, VALID_STOREUNIT));
    }

    @Test
    public void testNullFqan2() throws PatternSyntaxException, IOException {
        authoriseDnFqan( VALID_DN, VALID_FQAN);
        assertFalse( "user with DN and FQAN=null cannot stage when DN and FQAN is in file", _check.canPerformStaging( VALID_DN, null, VALID_STOREUNIT));
    }

    @Test()
    public void testCanStageWithEmptyFile() throws PatternSyntaxException, IOException {
        assertFalse( "Empty file allowed user to stage", _check.canPerformStaging( VALID_DN, VALID_FQAN, VALID_STOREUNIT));
    }

    @Test()
    public void testCanStageWithMissingFile() throws PatternSyntaxException, IOException {
        _testConfigFile.delete();
        assertFalse( "Missing file allowed user to stage", _check.canPerformStaging( VALID_DN, VALID_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testUserWithDnAuthorisedWithDnCanStage() throws IOException {
        authoriseDn( VALID_DN );
        assertTrue( "user with DN staging when DN is in file", _check.canPerformStaging( VALID_DN, null, VALID_STOREUNIT));
    }

    @Test
    public void testUserWithDnFqanAuthorisedWithDnCanStage() throws IOException {
        authoriseDn( VALID_DN );
        assertTrue( "user with DN and FQAN staging when DN is in file", _check.canPerformStaging( VALID_DN, VALID_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testUserWithDnAuthorisedWithDnAndFqanCanStage() throws IOException {
        authoriseDnFqan( VALID_DN, VALID_FQAN);
        assertFalse( "user with DN staging when DN and FQAN is in file", _check.canPerformStaging( VALID_DN, null, VALID_STOREUNIT));
    }

    @Test
    public void testUserWithDnAndDifferentFqanAuthorisedWithDnAndFqanCanStage() throws IOException {
        authoriseDnFqan( VALID_DN, VALID_FQAN);
        assertFalse( "user with DN and different FQAN staging when DN and FQAN is in file", _check.canPerformStaging( VALID_DN,
                                                                                                                      OTHER_VALID_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testUserWithDnAndSameFqanAuthorisedWithDnAndFqanCanStage() throws IOException {
        authoriseDnFqan( VALID_DN, VALID_FQAN);
        assertTrue( "user with DN and same FQAN staging when DN and FQAN is in file", _check.canPerformStaging( VALID_DN,
                                                                                                                VALID_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testWildcardsMatchingDNandFQAN() throws PatternSyntaxException, IOException {
        authoriseDnFqan( TEST_DN, TEST_FQAN);
        assertTrue( "check pattern .* : user with DN and FQAN can stage when DN and FQAN are in file", _check.canPerformStaging( USER_TEST1_DN,
                                                                                                                                 USER_TEST1_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testWildecardsNotMatchingDN() throws PatternSyntaxException, IOException {
        authoriseDnFqan( TEST_DN, TEST_FQAN);
        assertFalse( "check pattern .* : user's DN does not match, staging not allowed", _check.canPerformStaging( USER_TEST2_DN,
                                                                                                                   USER_TEST1_FQAN, VALID_STOREUNIT));
    }

    // below there are 7 tests for canPerformStaging(String dn, String fqan, String storeUnit),
    // to show that StageConfiguration.conf file may remain unchanged (using only 2 parameters as before: DN, FQAN)
    // when the new stage permission check is performed (using DN, FQAN, StoreUnit)
    @Test
    public void testStringUserWithDnAuthorisedWithDnCanStage() throws IOException {
        authoriseDn( VALID_DN );
        assertTrue( "user with DN staging when DN is in file", _check.canPerformStaging( VALID_DN, null, VALID_STOREUNIT ));
    }

    @Test
    public void testStringUserWithDnFqanAuthorisedWithDnCanStage() throws IOException {
        authoriseDn( VALID_DN );
        assertTrue( "user with DN and FQAN staging when DN is in file", _check.canPerformStaging( VALID_DN, VALID_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testStringUserWithDnAuthorisedWithDnAndFqanCanStage() throws IOException {
        authoriseDnFqan( VALID_DN, VALID_FQAN);
        assertFalse( "user with DN staging when DN and FQAN is in file", _check.canPerformStaging( VALID_DN, null, VALID_STOREUNIT));
    }

    @Test
    public void testStringUserWithDnAndDifferentFqanAuthorisedWithDnAndFqanCanStage() throws IOException {
        authoriseDnFqan( VALID_DN, VALID_FQAN);
        assertFalse( "user with DN and different FQAN staging when DN and FQAN is in file", _check.canPerformStaging( VALID_DN,
                                                                                                                      OTHER_VALID_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testStringUserWithDnAndSameFqanAuthorisedWithDnAndFqanCanStage() throws IOException {
        authoriseDnFqan( VALID_DN, VALID_FQAN);
        assertTrue( "user with DN and same FQAN staging when DN and FQAN is in file", _check.canPerformStaging( VALID_DN,
                                                                                                                VALID_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testStringWildcardsMatchingDNandFQAN() throws PatternSyntaxException, IOException {
        authoriseDnFqan( TEST_DN, TEST_FQAN);
        assertTrue( "check pattern .* : user with DN and FQAN can stage when DN and FQAN are in file", _check.canPerformStaging( USER_TEST1_DN,
                                                                                                                                 USER_TEST1_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testStringWildecardsNotMatchingDN() throws PatternSyntaxException, IOException {
        authoriseDnFqan( TEST_DN, TEST_FQAN);
        assertFalse( "check pattern .* : user's DN does not match, staging not allowed", _check.canPerformStaging( USER_TEST2_DN,
                                                                                                                   USER_TEST1_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testStringNullFqan() throws PatternSyntaxException, IOException {
        authoriseDn( VALID_DN );
        assertTrue( "user with DN and FQAN=null staging when DN is in file", _check.canPerformStaging( VALID_DN, null, VALID_STOREUNIT ));
    }

    @Test
    public void testStringNullFqan2() throws PatternSyntaxException, IOException {
        authoriseDnFqan( VALID_DN, VALID_FQAN);
        assertFalse( "user with DN and FQAN=null cannot stage when DN and FQAN is in file", _check.canPerformStaging( VALID_DN, null, VALID_STOREUNIT ));
    }

    //new tests for the method canPerformStaging(String dn, String fqan, String storeUnit)

    @Test
    public void testNullFqanCanStage() throws PatternSyntaxException, IOException {
        authoriseDnFqanStoreunit( VALID_DN, (String) null, null); //config line: "/DC=org/DC=example/CN=test user"
        assertTrue( "user with DN staging when DN is in file", _check.canPerformStaging( VALID_DN, null, VALID_STOREUNIT ));
    }

    @Test
    public void testDnFqanCanStageWhenDnInConfigfile() throws IOException {
        authoriseDnFqanStoreunit( VALID_DN, (String) null, null); //config line: "/DC=org/DC=example/CN=test user"
        assertTrue( "user with DN and FQAN staging when DN is in file", _check.canPerformStaging( VALID_DN, VALID_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testNullFqanCannotStage() throws PatternSyntaxException, IOException {
        authoriseDnFqanStoreunit( VALID_DN, VALID_FQAN, null); //config line: "/DC=org/DC=example/CN=test user" "/atlas/Role=production"
        assertFalse( "user with DN and FQAN=null cannot stage when DN and FQAN is in file", _check.canPerformStaging( VALID_DN, null, VALID_STOREUNIT));
    }

    @Test
    public void testDnFqanCanStageWhenDnFqanInConfigfile() throws IOException {
        authoriseDnFqanStoreunit( VALID_DN, VALID_FQAN, null); //config line: "/DC=org/DC=example/CN=test user" "/atlas/Role=production"
        assertTrue( "user with DN and FQAN staging when DN and FQAN are in file", _check.canPerformStaging( VALID_DN,
                                                                                                            VALID_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testDnFqanStoreunitCanStageWhenDnFqanStoreunitInConfigfile() throws IOException {
        authoriseDnFqanStoreunit( VALID_DN, VALID_FQAN, VALID_STOREUNIT); //config line: "/DC=org/DC=example/CN=test user" "/atlas/Role=production" "sql:chimera@osm"
        assertTrue( "user with DN and FQAN staging when DN and FQAN are in file; also storage unit is in file ", _check.canPerformStaging( VALID_DN,
                                                                                                                                           VALID_FQAN, VALID_STOREUNIT));
    }

    @Test
    public void testDnAndDifferentFqanAuthorisedWithDnAndFqanCanStage() throws IOException {
        authoriseDnFqanStoreunit( VALID_DN, VALID_FQAN, null); //config line: "/DC=org/DC=example/CN=test user" "/atlas/Role=production"
        assertFalse( "user with DN and wrong FQAN cannot stage when DN and FQAN is in file", _check.canPerformStaging( VALID_DN,
                                                                                                                       OTHER_VALID_FQAN,  VALID_STOREUNIT));
    }

    @Test
    public void testDnAndSameFqanAuthorisedWithDnAndFqanCanStage() throws IOException {
        authoriseDnFqanStoreunit( ".*", ".*", ".*"); //config line:  ".*" ".*" ".*"
        assertTrue( "user with DN, FQAN, and StorageGroup specified can stage in case ALL *s in ConfigFile", _check.canPerformStaging( VALID_DN,
                                                                                                                                       VALID_FQAN, VALID_STOREUNIT ));
    }

    /////////////////////
    private void authoriseDn( String dn ) throws IOException {
        authoriseDnFqanStoreunit( dn, (String) null, null);
    }

    private void authoriseDnFqan( String dn, FQAN fqan ) throws IOException {
        authoriseDnFqanStoreunit( dn, fqan.toString(), null);
    }

    private void authoriseDnFqanStoreunit( String dn, FQAN fqan, String storeUnit) throws IOException {
        authoriseDnFqanStoreunit(dn, fqan.toString(), storeUnit);
    }

    private void authoriseDnFqanStoreunit( String dn, String fqan, String storeUnit) throws IOException {
        StringBuilder sb = new StringBuilder();

        sb.append("\"");
        sb.append(dn);
        sb.append("\"");

        if( fqan != null) {
            sb.append("  \"");
            sb.append( fqan);
            sb.append("\"");
        }

        if( storeUnit != null) {
            sb.append("  \"");
            sb.append( storeUnit);
            sb.append("\"");
        }
        sb.append("\n");

        saveContents( sb.toString());
    }

    /**
    * Overwrite configuration file with provided contents
    * @param contents
    * @throws IOException
    */
    private void saveContents( String contents) throws IOException {
        BufferedWriter writer = new BufferedWriter( new FileWriter( _testConfigFile));
        writer.write( contents);
        writer.close();
    }
}

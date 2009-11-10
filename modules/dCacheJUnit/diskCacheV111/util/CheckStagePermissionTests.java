package diskCacheV111.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.PatternSyntaxException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.FQAN;


public class CheckStagePermissionTests {

    public static final String TEST_PREFIX = "stagePermissionFile";
    public static final String VALID_FQAN_STRING = "/atlas/Role=production";
    public static final FQAN VALID_FQAN = new FQAN( VALID_FQAN_STRING);

    public static final FQAN OTHER_VALID_FQAN = new FQAN( "/atlas");
    public static final String OTHER_VALID_FQAN_STRING = "/atlas";

    public static final String VALID_DN = "/DC=org/DC=example/CN=test user";

    public static final String TEST_DN = "/DC=org/DC=example/.*";
    public static final String TEST_FQAN_STRING = "/atlas/Role=.*";
    public static final FQAN TEST_FQAN = new FQAN(TEST_FQAN_STRING);

    public static final String USER_TEST1_DN = "/DC=org/DC=example/CN=test";
    public static final FQAN USER_TEST1_FQAN = new FQAN("/atlas/Role=production");
    public static final String USER_TEST1_FQAN_STRING = "/atlas/Role=production";

    public static final String USER_TEST2_DN = "/DC=org/DC=anotherExample/CN=test";

    public static final FQAN EMPTY_FQAN = new FQAN( "");
    public static final String EMPTY_FQAN_STRING = "";

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

    @Test
    public void testEmptyFilepath() throws PatternSyntaxException, IOException {
        CheckStagePermission check = new CheckStagePermission( "");
        assertTrue( "Empty filepath to config file allows user with DN and FQAN", check.canPerformStaging( VALID_DN, VALID_FQAN));
        assertTrue( "Empty filepath to config file allows user with DN", check.canPerformStaging( VALID_DN, EMPTY_FQAN));
    }

    @Test
    public void testNullFilepath() throws PatternSyntaxException, IOException {
        CheckStagePermission check = new CheckStagePermission( null);
        assertTrue( "Null filepath to config file allows user with DN and FQAN", check.canPerformStaging( VALID_DN, VALID_FQAN));
        assertTrue( "Null filepath to config file allows user with DN", check.canPerformStaging( VALID_DN, EMPTY_FQAN));
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

    @Test
    public void testZeroLengthDNEmptyFile() throws PatternSyntaxException, IOException {
        assertFalse( "user with ZeroLength DN and FQAN can not stage with empty file", _check.canPerformStaging("", VALID_FQAN));
    }

    @Test
    public void testZeroLengthDN() throws PatternSyntaxException, IOException {
        authorise( VALID_DN, null);
        assertFalse( "user with ZeroLengthDN and FQAN can not stage", _check.canPerformStaging("", VALID_FQAN));
    }

    @Test
    public void testZeroLengthDNAndFqan() throws PatternSyntaxException, IOException {
        authorise( VALID_DN, VALID_FQAN_STRING);
        assertFalse( "user with ZeroLengthDN and FQAN can not stage", _check.canPerformStaging("", VALID_FQAN));
    }

    @Test
    public void testNullFqan() throws PatternSyntaxException, IOException {
        authorise( VALID_DN, null);
        assertTrue( "user with DN and FQAN=null staging when DN is in file", _check.canPerformStaging( VALID_DN, (FQAN)null));
    }

    @Test
    public void testNullFqan2() throws PatternSyntaxException, IOException {
        authorise( VALID_DN, VALID_FQAN_STRING);
        assertFalse( "user with DN and FQAN=null cannot stage when DN and FQAN is in file", _check.canPerformStaging( VALID_DN, (FQAN)null));
    }

    @Test()
    public void testCanStageWithEmptyFile() throws PatternSyntaxException, IOException {
        assertFalse( "Empty file allowed user to stage", _check.canPerformStaging( VALID_DN, VALID_FQAN));
    }

    @Test()
    public void testCanStageWithMissingFile() throws PatternSyntaxException, IOException {
        _testConfigFile.delete();
        assertFalse( "Missing file allowed user to stage", _check.canPerformStaging( VALID_DN, VALID_FQAN));
    }

    @Test
    public void testUserWithDnAuthorisedWithDnCanStage() throws IOException {
        authorise( VALID_DN, null);
        assertTrue( "user with DN staging when DN is in file", _check.canPerformStaging( VALID_DN, EMPTY_FQAN));
    }

    @Test
    public void testUserWithDnFqanAuthorisedWithDnCanStage() throws IOException {
        authorise( VALID_DN, null);
        assertTrue( "user with DN and FQAN staging when DN is in file", _check.canPerformStaging( VALID_DN, VALID_FQAN));
    }

    @Test
    public void testUserWithDnAuthorisedWithDnAndFqanCanStage() throws IOException {
        authorise( VALID_DN, VALID_FQAN_STRING);
        assertFalse( "user with DN staging when DN and FQAN is in file", _check.canPerformStaging( VALID_DN, EMPTY_FQAN));
    }

    @Test
    public void testUserWithDnAndDifferentFqanAuthorisedWithDnAndFqanCanStage() throws IOException {
        authorise( VALID_DN, VALID_FQAN_STRING);
        assertFalse( "user with DN and different FQAN staging when DN and FQAN is in file", _check.canPerformStaging( VALID_DN, OTHER_VALID_FQAN));
    }

    @Test
    public void testUserWithDnAndSameFqanAuthorisedWithDnAndFqanCanStage() throws IOException {
        authorise( VALID_DN, VALID_FQAN_STRING);
        assertTrue( "user with DN and same FQAN staging when DN and FQAN is in file", _check.canPerformStaging( VALID_DN, VALID_FQAN));
    }

    @Test
    public void testWildcardsMatchingDNandFQAN() throws PatternSyntaxException, IOException {
        authorise( TEST_DN, TEST_FQAN_STRING);
        assertTrue( "check pattern .* : user with DN and FQAN can stage when DN and FQAN are in file", _check.canPerformStaging( USER_TEST1_DN, USER_TEST1_FQAN));
    }

    @Test
    public void testWildecardsNotMatchingDN() throws PatternSyntaxException, IOException {
        authorise( TEST_DN, TEST_FQAN_STRING);
        assertFalse( "check pattern .* : user's DN does not match, staging not allowed", _check.canPerformStaging( USER_TEST2_DN, USER_TEST1_FQAN));
    }

    ///////// below there are 7 tests for canPerformStaging(String dn, String fqan)  ///////
    @Test
    public void testStringUserWithDnAuthorisedWithDnCanStage() throws IOException {
        authorise( VALID_DN, null);
        assertTrue( "user with DN staging when DN is in file", _check.canPerformStaging( VALID_DN, EMPTY_FQAN_STRING));
    }

    @Test
    public void testStringUserWithDnFqanAuthorisedWithDnCanStage() throws IOException {
        authorise( VALID_DN, null);
        assertTrue( "user with DN and FQAN staging when DN is in file", _check.canPerformStaging( VALID_DN, VALID_FQAN_STRING));
    }

    @Test
    public void testStringUserWithDnAuthorisedWithDnAndFqanCanStage() throws IOException {
        authorise( VALID_DN, VALID_FQAN_STRING);
        assertFalse( "user with DN staging when DN and FQAN is in file", _check.canPerformStaging( VALID_DN, EMPTY_FQAN_STRING));
    }

    @Test
    public void testStringUserWithDnAndDifferentFqanAuthorisedWithDnAndFqanCanStage() throws IOException {
        authorise( VALID_DN, VALID_FQAN_STRING);
        assertFalse( "user with DN and different FQAN staging when DN and FQAN is in file", _check.canPerformStaging( VALID_DN, OTHER_VALID_FQAN_STRING));
    }

    @Test
    public void testStringUserWithDnAndSameFqanAuthorisedWithDnAndFqanCanStage() throws IOException {
        authorise( VALID_DN, VALID_FQAN_STRING);
        assertTrue( "user with DN and same FQAN staging when DN and FQAN is in file", _check.canPerformStaging( VALID_DN, VALID_FQAN_STRING));
    }

    @Test
    public void testStringWildcardsMatchingDNandFQAN() throws PatternSyntaxException, IOException {
        authorise( TEST_DN, TEST_FQAN_STRING);
        assertTrue( "check pattern .* : user with DN and FQAN can stage when DN and FQAN are in file", _check.canPerformStaging( USER_TEST1_DN, USER_TEST1_FQAN_STRING));
    }

    @Test
    public void testStringWildecardsNotMatchingDN() throws PatternSyntaxException, IOException {
        authorise( TEST_DN, TEST_FQAN_STRING);
        assertFalse( "check pattern .* : user's DN does not match, staging not allowed", _check.canPerformStaging( USER_TEST2_DN, USER_TEST1_FQAN_STRING));
    }

    @Test
    public void testStringNullFqan() throws PatternSyntaxException, IOException {
        authorise( VALID_DN, null);
        assertTrue( "user with DN and FQAN=null staging when DN is in file", _check.canPerformStaging( VALID_DN, (String)null ));
    }

    @Test
    public void testStringNullFqan2() throws PatternSyntaxException, IOException {
        authorise( VALID_DN, VALID_FQAN_STRING);
        assertFalse( "user with DN and FQAN=null cannot stage when DN and FQAN is in file", _check.canPerformStaging( VALID_DN, (String)null));
    }
    /////////////////////

    private void authorise( String dn, String voms) throws IOException {
        StringBuffer sb = new StringBuffer();

        sb.append("\"");
        sb.append(dn);
        sb.append("\"");

        if( voms != null) {
            sb.append("  \"");
            sb.append( voms);
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

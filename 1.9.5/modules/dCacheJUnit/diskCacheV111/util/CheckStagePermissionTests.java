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

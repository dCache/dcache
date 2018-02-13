package diskCacheV111.util;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static diskCacheV111.util.SerializableUtils.assertDeserialisationExpected;
import static diskCacheV111.util.SerializableUtils.assertSerialisationExpected;
import static org.junit.Assert.*;

/**
 * Test the PnfsId class.
 *
 * The tests can be grouped into three classes of test: those that test a
 * "simple" (without a domain) PNFS ID , those that test a PNFS ID with
 * domain and those that test a Chimera ID.
 *
 * Features, such as star-expanding and support for truncated PNFS IDs are
 * also tested, but only that they are equal to the corresponding simple PNFS
 * ID.
 */
public class PnfsIdTests {

    private static final String SERIALISED_DATA_STRING_PREFIX = "EXPECTED_ENCODED_SERIALISED_PNFSID_";

    private static final String VALID_PATH = "/pnfs/example.org/data/dteam/test-file-001";

    private static final String CHIMERA_ID =
            "80D1B8B90CED30430608C58002811B3285FC";

    private static final String PNFS_DATABASE = "000F";
    private static final String PNFS_COUNT_ZEROS = "00000000000000";
    private static final String PNFS_COUNT_NONZEROS = "389FC0";

    private static final String PNFS_SIMPLE_ID =
            PNFS_DATABASE + PNFS_COUNT_ZEROS + PNFS_COUNT_NONZEROS;
    private static final String PNFS_SIMPLE_ID_SHORT =
            PNFS_DATABASE + PNFS_COUNT_NONZEROS;

    private static final String EXPECTED_ENCODED_SERIALISED_SIMPLE_PNFS_PNFSID =
         "aced0005737200196469736b4361636865563131312e7574696c2e506e667"
       + "34964fe7150258b9baecf0200015b00025f617400025b427870757200025b"
       + "42acf317f8060854e002000078700000000c000f00000000000000389fc0";

    private static final String ENCODED_SERIALISED_SIMPLE_PNFS_PNFSIDS_FOR_DESERIALISATION[] = {
        "aced0005737200196469736b4361636865563131312e7574696c2e506e667"
      + "34964fe7150258b9baecf0200045b00025f617400025b424c00075f646f6d"
      + "61696e7400124c6a6176612f6c616e672f537472696e673b4c00095f69645"
      + "37472696e6771007e00024c00095f746f537472696e6771007e0002787075"
      + "7200025b42acf317f8060854e002000078700000000c000f0000000000000"
      + "0389fc0707400183030304630303030303030303030303030303338394643"
      + "30740018303030463030303030303030303030303030333839464330",

        "aced0005737200196469736b4361636865563131312e7574696c2e506e667"
      + "34964fe7150258b9baecf0200045b00025f617400025b424c00075f646f6d"
      + "61696e7400124c6a6176612f6c616e672f537472696e673b4c00095f69645"
      + "37472696e6771007e00024c00095f746f537472696e6771007e0002787075"
      + "7200025b42acf317f8060854e002000078700000000c000f0000000000000"
      + "0389fc0707400183030304630303030303030303030303030303338394643"
      + "3071007e0006",

        // SIMPLE_PNFS_PNFSID with a non-null '_domain' field.
        "aced0005737200196469736b4361636865563131312e7574696c2e506e667"
      + "34964fe7150258b9baecf0200045b00025f617400025b424c00075f646f6d"
      + "61696e7400124c6a6176612f6c616e672f537472696e673b4c00095f69645"
      + "37472696e6771007e00024c00095f746f537472696e6771007e0002787075"
      + "7200025b42acf317f8060854e002000078700000000c000f0000000000000"
      + "0389fc0740006646f6d61696e740018303030463030303030303030303030"
      + "30303033383946433074001f3030304630303030303030303030303030303"
      + "338394643302e646f6d61696e",

        // SIMPLE_PNFS_PNFSID with a null '_domain' field.
        "aced0005737200196469736b4361636865563131312e7574696c2e506e667"
      + "34964fe7150258b9baecf0200025b00025f617400025b424c00075f646f6d"
      + "61696e7400124c6a6176612f6c616e672f537472696e673b7870757200025"
      + "b42acf317f8060854e002000078700000000c000f00000000000000389fc0"
      + "70"
    };

    private static final String EXPECTED_ENCODED_SERIALISED_CHIMERA_PNFSID =
         "aced0005737200196469736b4361636865563131312e7574696c2e506e667"
       + "34964fe7150258b9baecf0200015b00025f617400025b427870757200025b"
       + "42acf317f8060854e002000078700000001280d1b8b90ced30430608c5800"
       + "2811b3285fc";

    private static final String ENCODED_SERIALISED_CHIMERA_PNFSIDS_FOR_DESERIALISATION[] = {
        "aced0005737200196469736b4361636865563131312e7574696c2e506e667"
      + "34964fe7150258b9baecf0200045b00025f617400025b424c00075f646f6d"
      + "61696e7400124c6a6176612f6c616e672f537472696e673b4c00095f69645"
      + "37472696e6771007e00024c00095f746f537472696e6771007e0002787075"
      + "7200025b42acf317f8060854e002000078700000001280d1b8b90ced30430"
      + "608c58002811b3285fc707400243830443142384239304345443330343330"
      + "3630384335383030323831314233323835464374002438304431423842393"
      + "0434544333034333036303843353830303238313142333238354643",

        "aced0005737200196469736b4361636865563131312e7574696c2e506e667"
      + "34964fe7150258b9baecf0200045b00025f617400025b424c00075f646f6d"
      + "61696e7400124c6a6176612f6c616e672f537472696e673b4c00095f69645"
      + "37472696e6771007e00024c00095f746f537472696e6771007e0002787075"
      + "7200025b42acf317f8060854e002000078700000001280d1b8b90ced30430"
      + "608c58002811b3285fc707400243830443142384239304345443330343330"
      + "3630384335383030323831314233323835464371007e0006",

        // CHIMERA_PNFSID with a null '_domain' field.
        "aced0005737200196469736b4361636865563131312e7574696c2e506e667"
      + "34964fe7150258b9baecf0200025b00025f617400025b424c00075f646f6d"
      + "61696e7400124c6a6176612f6c616e672f537472696e673b7870757200025"
      + "b42acf317f8060854e002000078700000001280d1b8b90ced30430608c580"
      + "02811b3285fc70"
    };

    PnfsId _chimeraId;
    PnfsId _simplePnfsId;

    @Before
    public void setUp() {
        _simplePnfsId = new PnfsId( PNFS_SIMPLE_ID);
        _chimeraId = new PnfsId( CHIMERA_ID);
    }

    @Test
    public void testSimplePnfsIdEqualsReflexive() {
        PnfsId otherId = new PnfsId( PNFS_SIMPLE_ID);
        assertEquals( "check simple-PNFS equality reflexive", otherId,
                _simplePnfsId);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZeroLengthPnfsId() {
        new PnfsId( "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooLongPnfsId() {
        new PnfsId( PNFS_SIMPLE_ID + "0");
    }

    @Test
    public void testExpandingPnfsId() {
        PnfsId shortPnfs = new PnfsId( PNFS_SIMPLE_ID.substring( 2));
        assertEquals( "short PNFS", shortPnfs, _simplePnfsId);
    }

    /*
     * Test the PnfsId.isValid static method
     */

    @Test
    public void testIsValidForSimplePnfsId() {
        assertTrue( PnfsId.isValid(PNFS_SIMPLE_ID));
    }

    @Test
    public void testIsValidForSimplePnfsIdShort() {
        assertTrue( PnfsId.isValid(PNFS_SIMPLE_ID_SHORT));
    }

    @Test
    public void testIsValidForChimeraId() {
        assertTrue( PnfsId.isValid(CHIMERA_ID));
    }

    @Test
    public void testIsValidForZeroLengthPnfsId() {
        assertFalse(PnfsId.isValid(""));
    }


    @Test
    public void testIsValidForTooLongPnfsId() {
        assertFalse(PnfsId.isValid( PNFS_SIMPLE_ID + "0"));
    }

    @Test
    public void testIsValidForPath() {
        assertFalse(PnfsId.isValid( VALID_PATH));
    }

    /*
     * Test equality
     */

    @Test
    public void testChimeraIdEqualsReflexive() {
        PnfsId otherId = new PnfsId( CHIMERA_ID);
        assertEquals( "check Chimera PNFS equality reflexive", otherId,
                _chimeraId);
    }

    @Test
    public void testSimplePnfsIdNotEqualToChimeraId() {
        PnfsId otherId = new PnfsId( CHIMERA_ID);
        assertFalse( "check PNFS equality", otherId.equals( _simplePnfsId));
        assertFalse( "check PNFS equality", _simplePnfsId.equals( otherId));
    }

    @Test
    public void testSimpleToString() {
        String value = _simplePnfsId.toString();
        assertEquals( "toString", PNFS_SIMPLE_ID, value);
    }

    @Test
    public void testChimeraToString() {
        String value = _chimeraId.toString();
        assertEquals( "toString", CHIMERA_ID, value);
    }

    /*
     * The following three tests (methods with names like "test*Serialise")
     * are intended to check that the previous branch-release of dCache can
     * deserlialise PnfsId objects generated by the current branch. This is
     * to honour our promise of supporting a mixed deployment of dCache
     * version A.B.(C-1) [previous stable release branch] and version A.B.C
     * [this release branch].
     *
     * However, to test backward compatibility correctly would require a copy
     * of the previous branch's PnfsId implementation with which we could
     * test. Since we don't have that code, these tests check that the PnfsId
     * serialises to a known-good byte-sequence.
     *
     * Testing the byte-sequence matches precisely is too strict a test: we
     * can change PnfsId so it's serialisation changes *provided* the latest
     * release-branch is unaffected by such a change.
     *
     * So, these tests are allowed to fail, in the sense that updating PnfsId
     * will require an update to these tests as well. The tests serve as a
     * reminder to check that the change in serialisation doesn't break
     * backwards compatibility.
     *
     * Three methods, with names like testEmit*Serialisation provide output
     * (on stdout) that may be copy-and-pasted into the code to update the
     * known-good serialised data.  To generate the data, uncomment the
     * @Ignore statement and re-run the unit-tests.
     */

    @Test
    public void testSimpleSerialise() throws IOException {
        assertSerialisationExpected( "serialised simple pnfs",
                EXPECTED_ENCODED_SERIALISED_SIMPLE_PNFS_PNFSID, _simplePnfsId);
    }

    @Test
    public void testChimeraSerialise() throws IOException {
        assertSerialisationExpected( "serialised chimera",
                EXPECTED_ENCODED_SERIALISED_CHIMERA_PNFSID, _chimeraId);
    }

    @Ignore("Only needed when generating new expected serialisation data")
    @Test
    public void testEmitSimplePnfsSerialisation() throws IOException {
        SerializableUtils.emitJavaStringDeclaration(SERIALISED_DATA_STRING_PREFIX
                + "SIMPLE_PNFS", _simplePnfsId);
    }

    @Ignore("Only needed when generating new expected serialisation data")
    @Test
    public void testEmitChimeraSerialisation() throws IOException {
        SerializableUtils.emitJavaStringDeclaration(SERIALISED_DATA_STRING_PREFIX
                + "CHIMERA", _chimeraId);
    }

    @Test
    public void testSimpleDeserialise() throws IOException,
            ClassNotFoundException {
        for( String serialisedData : ENCODED_SERIALISED_SIMPLE_PNFS_PNFSIDS_FOR_DESERIALISATION) {
            assertDeserialisationExpected("deserialise simple pnfs",
                    _simplePnfsId, serialisedData);
        }
    }

    @Test
    public void testChimeraDeserialise() throws IOException,
            ClassNotFoundException {
        for( String serialisedData : ENCODED_SERIALISED_CHIMERA_PNFSIDS_FOR_DESERIALISATION) {
            assertDeserialisationExpected("deserialise chimera", _chimeraId,
                    serialisedData);
        }
    }
}

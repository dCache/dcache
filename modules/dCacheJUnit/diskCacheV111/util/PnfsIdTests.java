package diskCacheV111.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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

    private static final String SERIALISED_DATA_STRING_PREFIX = "ENCODED_SERIALISED_PNFSID_";
    private static final int STRING_LINE_LENGTH = 61;

    private static final String CHIMERA_ID =
            "80D1B8B90CED30430608C58002811B3285FC";

    private static final String PNFS_DATABASE = "000F";
    private static final String PNFS_COUNT_ZEROS = "00000000000000";
    private static final String PNFS_COUNT_NONZEROS = "389FC0";
    private static final String PNFS_DOMAIN = "domain";

    private static final String PNFS_SIMPLE_ID =
            PNFS_DATABASE + PNFS_COUNT_ZEROS + PNFS_COUNT_NONZEROS;
    private static final String PNFS_SIMPLE_ID_SHORT =
            PNFS_DATABASE + PNFS_COUNT_NONZEROS;
    private static final String PNFS_DOMAIN_ID =
            PNFS_SIMPLE_ID + "." + PNFS_DOMAIN;
    private static final String PNFS_STAR_ID =
            PNFS_DATABASE + "*" + PNFS_COUNT_NONZEROS;

    private static final String ENCODED_SERIALISED_PNFSID_SIMPLE_PNFS =
            "aced0005737200196469736b4361636865563131312e7574696c2e506e667"
                    + "34964fe7150258b9baecf0200045b00025f617400025b424c00075f646f6d"
                    + "61696e7400124c6a6176612f6c616e672f537472696e673b4c00095f69645"
                    + "37472696e6771007e00024c00095f746f537472696e6771007e0002787075"
                    + "7200025b42acf317f8060854e002000078700000000c000f0000000000000"
                    + "0389fc0707400183030304630303030303030303030303030303338394643"
                    + "30740018303030463030303030303030303030303030333839464330";

    private static final String ENCODED_SERIALISED_PNFSID_DOMAIN_PNFS =
            "aced0005737200196469736b4361636865563131312e7574696c2e506e667"
                    + "34964fe7150258b9baecf0200045b00025f617400025b424c00075f646f6d"
                    + "61696e7400124c6a6176612f6c616e672f537472696e673b4c00095f69645"
                    + "37472696e6771007e00024c00095f746f537472696e6771007e0002787075"
                    + "7200025b42acf317f8060854e002000078700000000c000f0000000000000"
                    + "0389fc0740006646f6d61696e740018303030463030303030303030303030"
                    + "30303033383946433074001f3030304630303030303030303030303030303"
                    + "338394643302e646f6d61696e";

    private static final String ENCODED_SERIALISED_PNFSID_CHIMERA =
            "aced0005737200196469736b4361636865563131312e7574696c2e506e667"
                    + "34964fe7150258b9baecf0200045b00025f617400025b424c00075f646f6d"
                    + "61696e7400124c6a6176612f6c616e672f537472696e673b4c00095f69645"
                    + "37472696e6771007e00024c00095f746f537472696e6771007e0002787075"
                    + "7200025b42acf317f8060854e002000078700000001280d1b8b90ced30430"
                    + "608c58002811b3285fc707400243830443142384239304345443330343330"
                    + "3630384335383030323831314233323835464374002438304431423842393"
                    + "0434544333034333036303843353830303238313142333238354643";

    private static final String ENCODED_TOBINPNFSID_PNFS =
            "0f00000000000000c09f3800";

    private static final String ENCODED_TOBINPNFSID_CHIMERA =
            CHIMERA_ID.toLowerCase();

    PnfsId _chimeraId;
    PnfsId _simplePnfsId;
    PnfsId _domainPnfsId;

    @Before
    public void setUp() {
        _simplePnfsId = new PnfsId( PNFS_SIMPLE_ID);
        _domainPnfsId = new PnfsId( PNFS_DOMAIN_ID);
        _chimeraId = new PnfsId( CHIMERA_ID);
    }

    @Test
    public void testSimplePnfsIdEqualsReflexive() {
        PnfsId otherId = new PnfsId( PNFS_SIMPLE_ID);
        assertEquals( "check simple-PNFS equality reflexive", otherId,
                _simplePnfsId);
    }

    @Test
    public void testStarPnfsIdEqualsSimple() {
        PnfsId starPnfsId = new PnfsId( PNFS_STAR_ID);
        assertEquals( "check PNFS equality", starPnfsId, _simplePnfsId);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalInitialStarPnfsId() {
        new PnfsId( "*000");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalFinalStarPnfsId() {
        new PnfsId( "000*");
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

    @Test
    public void testDomainPnfsIdEqualsReflexive() {
        PnfsId otherId = new PnfsId( PNFS_DOMAIN_ID);
        assertEquals( "check domain-PNFS equality reflexive", otherId,
                _domainPnfsId);
    }

    @Test
    public void testChimeraIdEqualsReflexive() {
        PnfsId otherId = new PnfsId( CHIMERA_ID);
        assertEquals( "check Chimera PNFS equality reflexive", otherId,
                _chimeraId);
    }

    @Test
    public void testSimplePnfsIdNotEqualToDomainPnfsId() {
        PnfsId otherId = new PnfsId( PNFS_DOMAIN_ID);
        assertFalse( "check PNFS equality", otherId.equals( _simplePnfsId));
        assertFalse( "check PNFS equality", _simplePnfsId.equals( otherId));
    }

    @Test
    public void testSimplePnfsIdNotEqualToChimeraId() {
        PnfsId otherId = new PnfsId( CHIMERA_ID);
        assertFalse( "check PNFS equality", otherId.equals( _simplePnfsId));
        assertFalse( "check PNFS equality", _simplePnfsId.equals( otherId));
    }

    @Test
    public void testChimeraIdNotEqualToDomainPnfsId() {
        PnfsId otherId = new PnfsId( PNFS_DOMAIN_ID);
        assertFalse( "check PNFS equality", otherId.equals( _chimeraId));
        assertFalse( "check PNFS equality", _chimeraId.equals( otherId));
    }

    @Test
    public void testSimpleGetBytes() {
        byte[] bytes = _simplePnfsId.getBytes();
        String encodedBytes = encode( bytes);
        String expected = PNFS_SIMPLE_ID.toLowerCase();
        assertEquals( "getBytes", expected, encodedBytes);
    }

    @Test
    public void testDomainGetBytes() {
        byte[] bytes = _domainPnfsId.getBytes();
        String encodedBytes = encode( bytes);
        String expected = PNFS_SIMPLE_ID.toLowerCase();
        assertEquals( "getBytes", expected, encodedBytes);
    }

    @Test
    public void testChimeraGetBytes() {
        byte[] bytes = _chimeraId.getBytes();
        String encodedBytes = encode( bytes);
        String expected = CHIMERA_ID.toLowerCase();
        assertEquals( "getBytes", expected, encodedBytes);
    }

    @Test
    public void testSimpleGetDatabaseId() {
        int dbId = _simplePnfsId.getDatabaseId();
        int expectedId = Integer.parseInt( PNFS_DATABASE, 16);
        assertEquals( "database ID", expectedId, dbId);
    }

    @Test
    public void testDomainGetDatabaseId() {
        int dbId = _domainPnfsId.getDatabaseId();
        int expectedId = Integer.parseInt( PNFS_DATABASE, 16);
        assertEquals( "database ID", expectedId, dbId);
    }

    @Test
    public void testChimeraGetDatabaseId() {
        int dbId = _chimeraId.getDatabaseId();
        String firstFourCharacters = CHIMERA_ID.substring( 0, 4);
        int expectedId = Integer.parseInt( firstFourCharacters, 16);
        assertEquals( "database ID", expectedId, dbId);
    }

    @Test
    public void testSimpleGetDomain() {
        String domain = _simplePnfsId.getDomain();
        assertNull( "getDomain", domain);
    }

    @Test
    public void testDomainGetDomain() {
        String domain = _domainPnfsId.getDomain();
        assertEquals( "getDomain", PNFS_DOMAIN, domain);
    }

    @Test
    public void testChimeraGetDomain() {
        String domain = _chimeraId.getDomain();
        assertNull( "getDomain", domain);
    }

    @Test
    public void testSimpleGetId() {
        String id = _simplePnfsId.getId();
        assertEquals( "getId", PNFS_SIMPLE_ID, id);
    }

    @Test
    public void testDomainGetId() {
        String id = _domainPnfsId.getId();
        assertEquals( "getId", PNFS_SIMPLE_ID, id);
    }

    @Test
    public void testChimeraGetId() {
        String id = _chimeraId.getId();
        assertEquals( "getId", CHIMERA_ID, id);
    }

    @Test
    public void testSimpleToString() {
        String value = _simplePnfsId.toString();
        assertEquals( "toString", PNFS_SIMPLE_ID, value);
    }

    @Test
    public void testDomainToString() {
        String value = _domainPnfsId.toString();
        assertEquals( "toString", PNFS_DOMAIN_ID, value);
    }

    @Test
    public void testChimeraToString() {
        String value = _chimeraId.toString();
        assertEquals( "toString", CHIMERA_ID, value);
    }

    @Test
    public void testSimpleToShortString() {
        String value = _simplePnfsId.toShortString();
        assertEquals( "toShortString", PNFS_SIMPLE_ID_SHORT, value);
    }

    @Test
    public void testDomainToShortString() {
        String value = _domainPnfsId.toShortString();
        assertEquals( "toShortString", PNFS_SIMPLE_ID_SHORT, value);
    }

    @Test
    public void testChimeraToShortString() {
        String value = _chimeraId.toShortString();
        assertEquals( "toShortString", CHIMERA_ID, value);
    }

    @Test
    public void testSimpleToIdString() {
        String value = _simplePnfsId.toIdString();
        assertEquals( "toIdString", PNFS_SIMPLE_ID, value);
    }

    @Test
    public void testDomainToIdString() {
        String value = _domainPnfsId.toIdString();
        assertEquals( "toIdString", PNFS_SIMPLE_ID, value);
    }

    @Test
    public void testChimeraToIdString() {
        String value = _chimeraId.toIdString();
        assertEquals( "toIdString", CHIMERA_ID, value);
    }

    @Ignore("Implementation seems to be broken")
    @Test()
    public void testToCompleteId() {
        String completeId = PnfsId.toCompleteId( PNFS_SIMPLE_ID_SHORT);
        assertEquals( "toCompleteId", PNFS_SIMPLE_ID, completeId);
    }

    @Test
    public void testSimpleToBinPnfsId() {
        byte[] result = _simplePnfsId.toBinPnfsId();
        String encodedResult = encode( result);
        assertEquals( "toBinPnfsId", ENCODED_TOBINPNFSID_PNFS, encodedResult);
    }

    @Test
    public void testDomainToBinPnfsId() {
        byte[] result = _domainPnfsId.toBinPnfsId();
        String encodedResult = encode( result);
        assertEquals( "toBinPnfsId", ENCODED_TOBINPNFSID_PNFS, encodedResult);
    }

    @Test
    public void testChimeraToBinPnfsId() {
        byte[] result = _chimeraId.toBinPnfsId();
        String encodedResult = encode( result);
        assertEquals( "toBinPnfsId", ENCODED_TOBINPNFSID_CHIMERA, encodedResult);
    }

    @Test
    public void testSimpleIntValue() {
        int value = _simplePnfsId.intValue();
        assertEquals( "intValue", PNFS_SIMPLE_ID.hashCode(), value);
    }

    @Test
    public void testDomainIntValue() {
        int value = _domainPnfsId.intValue();
        assertEquals( "intValue", PNFS_DOMAIN_ID.hashCode(), value);
    }

    @Test
    public void testIntValue() {
        int value = _chimeraId.intValue();
        assertEquals( "intValue", CHIMERA_ID.hashCode(), value);
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
                ENCODED_SERIALISED_PNFSID_SIMPLE_PNFS, _simplePnfsId);
    }

    @Test
    public void testDomainSerialise() throws IOException {
        assertSerialisationExpected( "serialised domain pnfs",
                ENCODED_SERIALISED_PNFSID_DOMAIN_PNFS, _domainPnfsId);
    }

    @Test
    public void testChimeraSerialise() throws IOException {
        assertSerialisationExpected( "serialised chimera",
                ENCODED_SERIALISED_PNFSID_CHIMERA, _chimeraId);
    }

    @Ignore("Only needed when generating new known-good serialisation data")
    @Test
    public void testEmitSimplePnfsSerialisation() throws IOException {
        String serialised = serialiseAndEncodeObject( _simplePnfsId);
        emitJavaStringDeclaration( SERIALISED_DATA_STRING_PREFIX + "SIMPLE_PNFS", serialised);
    }

    @Ignore("Only needed when generating new known-good serialisation data")
    @Test
    public void testEmitDomainPnfsSerialisation() throws IOException {
        String serialised = serialiseAndEncodeObject( _domainPnfsId);
        emitJavaStringDeclaration( SERIALISED_DATA_STRING_PREFIX + "DOMAIN_PNFS", serialised);
    }

    @Ignore("Only needed when generating new known-good serialisation data")
    @Test
    public void testEmitChimeraSerialisation() throws IOException {
        String serialised = serialiseAndEncodeObject( _chimeraId);
        emitJavaStringDeclaration( SERIALISED_DATA_STRING_PREFIX + "CHIMERA", serialised);
    }

    @Test
    public void testSimpleDeserialise() throws IOException,
            ClassNotFoundException {
        assertDeserialisationExpected( "deserialise simple pnfs",
                _simplePnfsId, ENCODED_SERIALISED_PNFSID_SIMPLE_PNFS);
    }

    @Test
    public void testDomainDeserialise() throws IOException,
            ClassNotFoundException {
        assertDeserialisationExpected( "deserialise domain pnfs",
                _domainPnfsId, ENCODED_SERIALISED_PNFSID_DOMAIN_PNFS);
    }

    @Test
    public void testChimeraDeserialise() throws IOException,
            ClassNotFoundException {
        assertDeserialisationExpected( "deserialise chimera", _chimeraId,
                ENCODED_SERIALISED_PNFSID_CHIMERA);
    }

    /*
     * SUPPORT METHODS
     */

    private void assertSerialisationExpected( String message,
                                              String encodedExpected,
                                              Object object) throws IOException {
        String encodedResult = serialiseAndEncodeObject( object);
        assertEquals( message, encodedExpected, encodedResult);
    }

    private String serialiseAndEncodeObject( Object object) throws IOException {
        ByteArrayOutputStream storage = new ByteArrayOutputStream();
        ObjectOutput objectOutput = new ObjectOutputStream( storage);
        objectOutput.writeObject( object);
        objectOutput.close();

        return encode( storage.toByteArray());
    }

    private void assertDeserialisationExpected( String message,
                                                PnfsId expectedObject,
                                                String encodedSerialisedObject)
            throws ClassNotFoundException, IOException {
        byte[] serialisedData = decode( encodedSerialisedObject);
        ByteArrayInputStream byteStream =
                new ByteArrayInputStream( serialisedData);
        ObjectInput objectInput = new ObjectInputStream( byteStream);

        Object deserialisedObject = objectInput.readObject();

        assertEquals( message, expectedObject, deserialisedObject);
    }

    @Test
    public void testDecodeEncode() throws IOException {
        assertDecodeEncodeEquals( ENCODED_SERIALISED_PNFSID_SIMPLE_PNFS);
        assertDecodeEncodeEquals( ENCODED_SERIALISED_PNFSID_DOMAIN_PNFS);
        assertDecodeEncodeEquals( ENCODED_SERIALISED_PNFSID_CHIMERA);
    }

    @Test
    public void testEncodeDecode() throws IOException {
        byte[] rawSimple = decode( ENCODED_SERIALISED_PNFSID_SIMPLE_PNFS);
        assertEncodeDecodeEquals( rawSimple);
        byte[] rawDomain = decode( ENCODED_SERIALISED_PNFSID_DOMAIN_PNFS);
        assertEncodeDecodeEquals( rawDomain);
        byte[] rawChimera = decode( ENCODED_SERIALISED_PNFSID_CHIMERA);
        assertEncodeDecodeEquals( rawChimera);
    }

    private void assertEncodeDecodeEquals( byte[] data) {
        String encodedData = encode( data);
        byte[] decodedEncodedData = decode( encodedData);
        assertArrayEquals(
                "mismatch between data and decoded version of encoded data",
                data, decodedEncodedData);
    }

    private void assertDecodeEncodeEquals( String data) {
        byte[] decodedData = decode( data);
        String encodedDecodedData = encode( decodedData);
        assertEquals(
                "mismatch between data and encoded version of decoded data",
                data, encodedDecodedData);
    }

    // Based on code from http://...
    private static String encode( byte[] byteStream) {
        StringBuilder result = new StringBuilder();
        for( byte curr : byteStream)
            result.append( Integer.toString( (curr & 0xff) + 0x100, 16)
                    .substring( 1));
        return result.toString();
    }

    // Taken from
    // http://stackoverflow.com/questions/140131/convert-a-string-representation-of-a-hex-dump-to-a-byte-array-using-java
    private static byte[] decode( final String encoded) {
        if( (encoded.length() % 2) != 0)
            throw new IllegalArgumentException(
                                                "Input string must contain an even number of characters");

        final byte result[] = new byte[encoded.length() / 2];
        final char enc[] = encoded.toCharArray();
        for( int i = 0; i < enc.length; i += 2) {
            StringBuilder curr = new StringBuilder( 2);
            curr.append( enc[i]).append( enc[i + 1]);
            result[i / 2] = (byte) Integer.parseInt( curr.toString(), 16);
        }
        return result;
    }


    private void emitJavaStringDeclaration( String name, String data) {
        List<String> lines = breakStringIntoLines( data);
        String declaration = buildJavaStringDeclaration( name, lines);
        System.out.println( declaration);
    }

    private List<String> breakStringIntoLines( String data) {
        List<String> lines = new ArrayList<String>();

        String remaining;
        for( String current = data; current.length() > 0; current = remaining) {

            int thisLineLength =
                    current.length() < STRING_LINE_LENGTH ? current.length()
                            : STRING_LINE_LENGTH;

            String thisLine = current.substring( 0, thisLineLength);
            remaining = current.substring( thisLineLength, current.length());

            lines.add( thisLine);
        }

        return lines;
    }

    private String buildJavaStringDeclaration( String name, List<String> lines) {
        StringBuilder sb = new StringBuilder();

        sb.append( "    private static final String " +
                   name + " =\n");

        boolean isFirstLine = true;

        for( String line : lines) {
            if( isFirstLine)
                sb.append( "                     ");
            else
                sb.append( "\n                   + ");
            sb.append( "\"" + line + "\"");
            isFirstLine = false;
        }

        sb.append( ";\n");

        return sb.toString();
    }


}

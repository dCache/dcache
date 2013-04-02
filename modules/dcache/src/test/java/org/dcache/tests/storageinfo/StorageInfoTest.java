package org.dcache.tests.storageinfo;


import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.OptionalDataException;
import java.io.StreamCorruptedException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import static org.junit.Assert.*;

public class StorageInfoTest {

    private StorageInfo _storageInfo;

    @Before
    public void setUp() throws Exception {
        _storageInfo = readStorageInfo(ClassLoader.getSystemResourceAsStream("org/dcache/tests/storageinfo/storageInfo-1.7"));
    }

    @Test
    public void testStorageInfoLocations17() throws Exception {

        List<URI> locations = _storageInfo.locations();
        assertNotNull("pre 1.8 storageInfo should return non null locations list", locations);
    }


    @Test
    public void testStorageinfoAccessLatency() throws Exception {
        AccessLatency accessLatency = _storageInfo.getLegacyAccessLatency();
        assertNotNull("pre 1.8 storageInfo should return non null access latency", accessLatency);
    }

    @Test
    public void testStorageInfoRetentionPolicy() throws Exception {
        RetentionPolicy retentionPolicy = _storageInfo.getLegacyRetentionPolicy();
        assertNotNull("pre 1.8 storageInfo should return non null retention policy", retentionPolicy);
    }

    @Test
    public void testStorageInfoLocationSet() throws Exception {
       _storageInfo.isSetAddLocation();
        // do nothing , just check for null pointer exception
    }


    @Test
    public void testStorageInfoToString() throws Exception {
        _storageInfo.toString();
        // do nothing , just check for null pointer exception
    }


    @Test
    public void testStorageInfoMap() throws Exception {
        Map<String, String> keyMap = _storageInfo.getMap();
        assertNotNull("pre 1.8 storageInfo should return non null keyMap", keyMap);
    }

    @Test
    public void testStorageGetHsm() throws Exception {
        String hsm = _storageInfo.getHsm();
        assertNotNull("pre 1.8 storageInfo should return non null hsm name", hsm);
    }

    @Test
    public void testStorageIsStoredAndBfid() throws Exception {
        String bfid = _storageInfo.getBitfileId();

        assertNotNull("String representation of bit file id should be not a null", bfid);
        if( !bfid.equals("<Unknown>")) {
            assertTrue("with known bitfileid storage info should declared itself as stored", _storageInfo.isStored());
        }
    }

    @Test
    public void testSameEquals() {

        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");

        assertTrue("equal storageInfo did not pass", storageInfo.equals(storageInfo) );

    }

    @Test
    public void testEquals() {

        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        StorageInfo otherInfo = new GenericStorageInfo("osm", "h1:raw");

        assertTrue("equal storageInfo did not pass", storageInfo.equals(otherInfo) );
        assertTrue("equals requre hash codes to be the same", storageInfo.hashCode() == otherInfo.hashCode());
    }

    @Test
    public void testEqualsWithMultipleLocations() throws URISyntaxException {
        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        StorageInfo otherInfo = new GenericStorageInfo("osm", "h1:raw");

        final String URI_STRING = "osm://osm/&foo=bar";

        storageInfo.addLocation( new URI( URI_STRING));
        storageInfo.addLocation( new URI( URI_STRING));

        otherInfo.addLocation( new URI( URI_STRING));

        assertTrue( "storageInfo.equals() with uneven number of identical location URIs", storageInfo.equals(  otherInfo));
        assertTrue( "stoageInfo.hashCode() was different with uneven number of identical location URIs", storageInfo.hashCode() == otherInfo.hashCode());
    }

    @Test
    public void testNotEquals() {

        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        StorageInfo otherInfo = new GenericStorageInfo("osm", "h1:rawd");

        assertFalse("not equal storageInfo pass", storageInfo.equals(otherInfo) );
    }


    @Test
    public void testNotEqualsByRP() {

        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        storageInfo.setLegacyRetentionPolicy(RetentionPolicy.REPLICA);

        StorageInfo otherInfo = new GenericStorageInfo("osm", "h1:raw");
        otherInfo.setLegacyRetentionPolicy(RetentionPolicy.OUTPUT);

        assertFalse("not equal by RetantionPolicy storageInfo pass", storageInfo.equals(otherInfo) );
    }

    @Test
    public void testNotEqualsByAL() {

        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        storageInfo.setLegacyAccessLatency(AccessLatency.NEARLINE);

        StorageInfo otherInfo = new GenericStorageInfo("osm", "h1:raw");
        otherInfo.setLegacyAccessLatency(AccessLatency.ONLINE);

        assertFalse("not equal by AccessLatency storageInfo pass", storageInfo.equals(otherInfo) );
    }

    @Test
    public void testNotEqualsByHSM() {

        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        StorageInfo otherInfo = new GenericStorageInfo("enstore", "h1:raw");

        assertFalse("not equal by HSM storageInfo pass", storageInfo.equals(otherInfo) );
    }

    @Test
    public void testNotEqualsByFileSize() {

        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        storageInfo.setLegacySize(17);
        StorageInfo otherInfo = new GenericStorageInfo("osm", "h1:raw");
        otherInfo.setLegacySize(21);

        assertFalse("not equal by file size storageInfo pass", storageInfo.equals(otherInfo) );
    }

    @Test
    public void testNotEqualsByMap() {

        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        storageInfo.setKey("bla", "bla");
        StorageInfo otherInfo = new GenericStorageInfo("osm", "h1:raw");
        otherInfo.setKey("not bla", "bla");

        assertFalse("not equal by map storageInfo pass", storageInfo.equals(otherInfo) );
    }


    @Test
    public void testNotEqualsByLocation() throws Exception {

        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        storageInfo.addLocation(new URI("osm://osm?bf1"));
        StorageInfo otherInfo = new GenericStorageInfo("osm", "h1:raw");
        otherInfo.addLocation(new URI("enstore://enstore?bf2"));

        assertFalse("not equal by location storageInfo pass", storageInfo.equals(otherInfo) );
    }


    @Test
    public void testNotEqualsByIsStored() throws Exception {

        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        StorageInfo otherInfo = new GenericStorageInfo("osm", "h1:raw");

        storageInfo.setIsStored(false);
        otherInfo.setIsStored(true);

        assertFalse("not equal by isSored storageInfo pass", storageInfo.equals(otherInfo) );
    }

    @Test
    public void testNotEqualsByIsNew() throws Exception {

        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        StorageInfo otherInfo = new GenericStorageInfo("osm", "h1:raw");

        storageInfo.setIsNew(false);
        otherInfo.setIsNew(true);

        assertFalse("not equal by isNew storageInfo pass", storageInfo.equals(otherInfo) );
    }

    @Test
    public void testNotEqualsByBitfileId() throws Exception {

        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        StorageInfo otherInfo = new GenericStorageInfo("osm", "h1:raw");

        storageInfo.setBitfileId("1");
        otherInfo.setBitfileId("2");

        assertFalse("not equal by BitfileId storageInfo pass", storageInfo.equals(otherInfo) );
    }

    @Test
    public void testClone() throws URISyntaxException
    {
        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");

        final String URI_STRING = "osm://osm/&foo=bar";

        storageInfo.setBitfileId("1");
        storageInfo.addLocation(new URI(URI_STRING));
        storageInfo.addLocation(new URI(URI_STRING));
        storageInfo.setKey("bla", "bla");
        storageInfo.setLegacySize(17);
        storageInfo.setLegacyRetentionPolicy(RetentionPolicy.REPLICA);
        storageInfo.setLegacyAccessLatency(AccessLatency.NEARLINE);

        assertTrue("Clone is not equals to original",
                   storageInfo.equals(storageInfo.clone()));
    }

    private static StorageInfo readStorageInfo(InputStream objIn)
        throws IOException
    {
        ObjectInputStream in = null;
        StorageInfo storageInfo = null;

        try {

            in = new ObjectInputStream(new BufferedInputStream(objIn));
            storageInfo = (StorageInfo) in.readObject();

        } catch (ClassNotFoundException cnf) {

        } catch (InvalidClassException ife) {
            // valid exception if siFIle is broken
        } catch( StreamCorruptedException sce ) {
            // valid exception if siFIle is broken
        } catch (OptionalDataException ode) {
            // valid exception if siFIle is broken
        } catch (EOFException eof){
            // object file size mismatch
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException we) {
                    // close on read can be ignored
                }
            }
        }

        return storageInfo;
    }

}

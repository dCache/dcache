package org.dcache.tests.storageinfo;


import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.sun.corba.se.impl.io.OptionalDataException;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;

public class StorageInfoTest {

    private StorageInfo _storageInfo;

    @Before
    public void setUp() throws Exception {
        _storageInfo = readStorageInfo(new File("modules/dCacheJUnit/org/dcache/tests/storageinfo/storageInfo-1.7"));
    }

    @Test
    public void testStorageInfoLocations17() throws Exception {

        List<URI> locations = _storageInfo.locations();
        assertNotNull("pre 1.8 storageInfo should return non null locations list", locations);
    }


    @Test
    public void testStorageinfoAccessLatency() throws Exception {
        AccessLatency accessLatency = _storageInfo.getAccessLatency();
        assertNotNull("pre 1.8 storageInfo should return non null access latency", accessLatency);
    }

    @Test
    public void testStorageInfoRetentionPolicy() throws Exception {
        RetentionPolicy retentionPolicy = _storageInfo.getRetentionPolicy();
        assertNotNull("pre 1.8 storageInfo should return non null retention policy", retentionPolicy);
    }

    @Test
    public void testStorageInfoRetentionPolicySet() throws Exception {
        _storageInfo.isSetRetentionPolicy();
        // do nothing , just check for null pointer exception
    }


    @Test
    public void testStorageInfoAccessLatencySet() throws Exception {
        _storageInfo.isSetAccessLatency();
        // do nothing , just check for null pointer exception
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



    private static StorageInfo readStorageInfo(File objIn) throws IOException {

        ObjectInputStream in = null;
        StorageInfo storageInfo = null;

        try {

            in = new ObjectInputStream(
                    new BufferedInputStream(new FileInputStream(objIn))
                    );
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

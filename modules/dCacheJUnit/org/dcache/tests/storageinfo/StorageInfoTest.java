package org.dcache.tests.storageinfo;


import static org.junit.Assert.assertTrue;

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

import org.junit.Test;

import com.sun.corba.se.impl.io.OptionalDataException;

import diskCacheV111.vehicles.StorageInfo;

public class StorageInfoTest {

    @Test
    public void testStorageInfoLocations17() throws Exception {

        StorageInfo storageInfo = readStorageInfo(new File("modules/dCacheJUnit/org/dcache/tests/storageinfo/storageInfo-1.7"));

        List<URI> locations = storageInfo.locations();
        assertTrue("pre 1.8 storageInfo should return empty locations list", locations.isEmpty());

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

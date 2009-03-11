package org.dcache.tests.repository;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import dmg.util.Args;

import org.dcache.pool.repository.Account;
import org.dcache.pool.repository.EventType;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.FlatFileStore;
import org.dcache.pool.repository.MetaDataStore;
import org.dcache.pool.repository.meta.db.BerkeleyDBMetaDataRepository;
import org.dcache.pool.repository.v4.CacheRepositoryV4;

import org.dcache.pool.classic.SpaceSweeper2;
import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.event.CacheRepositoryEvent;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

public class PoolRepository {


    private CacheRepositoryV4 _repository;
    private Account _account;
    private File _base;
    @Before
    public void setUp() throws Exception {

        Logger.getLogger("logger.org.dcache.repository").setLevel(Level.ERROR);

        _base = new File("/tmp/repository");

        _base.mkdirs();
        new File(_base, "control").mkdir();
        new File(_base, "data").mkdir();

        _account = new Account();
        _account.setTotal(1024);
        _repository = new CacheRepositoryV4(_base);
        FileStore fileStore = new FlatFileStore(_base);
        MetaDataStore metaDataStore =
            new BerkeleyDBMetaDataRepository(fileStore, _repository, _base);
        _repository.setAccount(_account);
        _repository.setMetaDataStore(metaDataStore);
        _repository.setFileStore(fileStore);
    }


    @After
    public void tearDown() throws Exception {

        // TODO: remove /tmp/repository

    }

    /*
     *  in all tests we are using Chimera id's
     */
    static  String generateNewID() {
        return UUID.randomUUID().toString().toUpperCase().replace('-','0');
    }
    @Test
    public void testCreate() throws Exception {

        String id = generateNewID();
        PnfsId pnfsId = new PnfsId(id);
        _repository.createEntry(pnfsId);

        assertEquals(true, _repository.contains(pnfsId));

        /*
         * increment link count and then remove
         */


        CacheRepositoryEntry entry = _repository.getEntry(pnfsId);
        entry.incrementLinkCount();

        assertEquals(1, entry.getLinkCount());

        _repository.removeEntry(entry);

        try {
            assertNull("Removed entry shold not be accessable", _repository.getEntry(pnfsId));
        }catch(FileNotInCacheException e) {
            // OK
        }

        assertNotNull("Removed entry accessable as generic", _repository.getGenericEntry(pnfsId));

    }

    @Test
    public void testSticky() throws Exception {

        String id = generateNewID();
        PnfsId pnfsId = new PnfsId(id);
        _repository.createEntry(pnfsId);


        CacheRepositoryEntry entry = _repository.getEntry(pnfsId);

        entry.setCached();
        entry.setSticky("repository test", System.currentTimeMillis() + 5000, true);

        assertEquals("setSticky makes file sticky",true, entry.isSticky());
        Thread.sleep(5100);
        assertEquals("entry shold be not stich when life time is over",false, entry.isSticky());

        _repository.removeEntry(entry);
    }

    @Test
    public void testStatTransitions() throws Exception {

        String id = generateNewID();
        PnfsId pnfsId = new PnfsId(id);

        CacheRepositoryEntry entry = _repository.createEntry(pnfsId);

        try {
            /* Setting sticky early should be allowed, as
             * there is otherwise no way to set cached +
             * sticky without either going via precious or
             * risking the file to be garbage collected.
             */
            entry.setSticky(true);
            assertTrue("Sticky not set", entry.isSticky());
        }catch(CacheException e) {
            fail("Setting sticky should not throw an exception");
        }

        try {
            entry.setReceivingFromClient();
            assertEquals("Recieving from client not set",true, entry.isReceivingFromClient());

            entry.setReceivingFromStore();
            fail("transition from-client => from-store should throws exception");
        }catch(CacheException e) {
            // OK illegal state
        }

        try {
            // we are in from client state
            entry.setCached();
            assertEquals("Cached not set",true, entry.isCached());

        }catch(CacheException e) {
            fail("Valid transition from-clent => cached");
        }

        try {
            // we are in cached state
            entry.setPrecious();
            assertEquals("Precious not set",true, entry.isPrecious());

        }catch(CacheException e) {
            fail("Transition cached => precious should not throw exception");
        }


        try {
            // we are in precious state
            entry.setReceivingFromClient();
            fail("Transition precious => from-clent should throw exception");

        }catch(CacheException e) {
            // OK
        }

        try {
            // we are in precious state
            entry.setReceivingFromStore();
            fail("Transition precious => from-store should throw exception");

        }catch(CacheException e) {
            // OK
        }

        try {
            // we are in precious state
            entry.setSendingToStore(true);
            assertEquals("TO Store not set",true, entry.isSendingToStore());

        }catch(CacheException e) {
            fail("transition precious => to store should not throw exception");
        }

        try {
            // we are in precious state
            entry.setCached();
            assertEquals("Cached not set",true, entry.isCached());

        }catch(CacheException e) {
            fail("Transition precious => cached should not throw exception");
        }

        try {
            // we are in cached state
            entry.setReceivingFromClient();
            fail("Transition cached => from-client should throw exception");

        }catch(CacheException e) {
            // OK
        }

        try {
            // we are in caches state
            entry.setReceivingFromStore();
            fail("Transition cached => from-store should throw exception");

        }catch(CacheException e) {
            // OK
        }

        try {
            // we are in cached state
            entry.setSendingToStore(true);
            assertEquals("Cached files can't be in TO STORE state",true, entry.isSendingToStore());

        }catch(CacheException e) {
            // OK
        }


        _repository.removeEntry(entry);
    }

    @Test
    public void testBad2Precious() throws Exception {

        String id = generateNewID();
        PnfsId pnfsId = new PnfsId(id);

        CacheRepositoryEntry entry = _repository.createEntry(pnfsId);

        entry.setCached();
        entry.setBad(true);
        entry.setPrecious();
        assertTrue("Entry should still be bad", entry.isBad());

        _repository.removeEntry(entry);
    }

    @Test
    public void testReadTransient() throws Exception {

        String id = generateNewID();
        PnfsId pnfsId = new PnfsId(id);

        CacheRepositoryEntry entry = _repository.createEntry(pnfsId);

        entry.setReceivingFromStore();

    }


    @Test
    public void testBad2Good() throws Exception {

        String id = generateNewID();
        PnfsId pnfsId = new PnfsId(id);
        _repository.createEntry(pnfsId);


        CacheRepositoryEntry entry = _repository.getEntry(pnfsId);

        entry.setCached();
        entry.setBad(true);
        assertTrue("setBad(true) did not change the state to BAD", entry.isBad());

        entry.setBad(false);
        assertFalse("setBad(false) did not remove BAD state", entry.isBad());

        _repository.removeEntry(entry);
    }


    @Test
    public void testCallBackAvailable() throws Exception {

        RepositoryCallbacksHelper listener = new RepositoryCallbacksHelper();

        _repository.addCacheRepositoryListener(listener);

        String id = generateNewID();
        PnfsId pnfsId = new PnfsId(id);

        CacheRepositoryEntry entry = _repository.createEntry(pnfsId);

        entry.setReceivingFromClient();
        entry.setCached();


        assertEquals("Available callback not called", 1, listener.getAvailableCalled());

    }

    @Test
    public void testCallBackCached() throws Exception {

        RepositoryCallbacksHelper listener = new RepositoryCallbacksHelper();

        _repository.addCacheRepositoryListener(listener);

        String id = generateNewID();
        PnfsId pnfsId = new PnfsId(id);

        CacheRepositoryEntry entry = _repository.createEntry(pnfsId);

        entry.setCached();

        assertEquals("Cached callback not called", 1, listener.getCachedCalled());

    }

    @Test
    public void testCallBackRemoved() throws Exception {

        RepositoryCallbacksHelper listener = new RepositoryCallbacksHelper();

        _repository.addCacheRepositoryListener(listener);

        String id = generateNewID();
        PnfsId pnfsId = new PnfsId(id);

        CacheRepositoryEntry entry = _repository.createEntry(pnfsId);

        entry.setCached();
        _repository.removeEntry(entry);

        assertEquals("Removed callback not called", 1, listener.getRemovedCalled());
        assertEquals("Destroyed callback not called", 1, listener.getDestroyedCalled());

    }

    @Test
    public void testCallBackPrecious() throws Exception {

        RepositoryCallbacksHelper listener = new RepositoryCallbacksHelper();

        _repository.addCacheRepositoryListener(listener);

        String id = generateNewID();
        PnfsId pnfsId = new PnfsId(id);

        CacheRepositoryEntry entry = _repository.createEntry(pnfsId);

        entry.setPrecious();

        assertEquals("Precious callback not called", 1, listener.getPreciousCalled());
    }

    @Test
    public void testCallBackSticky() throws Exception {

        RepositoryCallbacksHelper listener = new RepositoryCallbacksHelper();

        _repository.addCacheRepositoryListener(listener);

        String id = generateNewID();
        PnfsId pnfsId = new PnfsId(id);

        CacheRepositoryEntry entry = _repository.createEntry(pnfsId);

        entry.setPrecious();

        assertEquals("Precious callback not called", 1, listener.getPreciousCalled());
        assertEquals("Sticky callback is called", 0, listener.getStickyCalled());

        entry.setSticky(true);
        assertEquals("Sticky callback not called", 1, listener.getStickyCalled());


        entry.setSticky(false);
        assertEquals("Sticky callback not called", 2, listener.getStickyCalled());

    }

    @Test
    public void testFileSize() throws Exception {


        String id = generateNewID();
        PnfsId pnfsId = new PnfsId(id);

        CacheRepositoryEntry entry = _repository.createEntry(pnfsId);

        entry.setSize(17);

        assertEquals(17, entry.getSize());
    }

    @Test(timeout = 500)
    public void testSpaceAllocation() {

        /* for now this test is impossible due to repository design */

        // try {
        // _repository.allocateSpace(10);
        // fail("out of index shold be thrown");
        // } catch (InterruptedException e) {
        // fail("Allocation failed with exception : " + e.getMessage());
        // }

    }

}

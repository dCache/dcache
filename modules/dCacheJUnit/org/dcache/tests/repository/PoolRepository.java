package org.dcache.tests.repository;

import java.io.File;
import java.util.UUID;

import org.dcache.pool.repository.v3.CacheRepositoryV3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsId;

public class PoolRepository extends junit.framework.TestCase {


	private CacheRepositoryV3 _repository;

	@Before
	public void setUp() throws Exception {

	    Logger.getLogger("logger.org.dcache.repository").setLevel(Level.ERROR);

		File base = new File("/tmp/repository");

		base.mkdirs();
		new File(base, "control").mkdir();
		new File(base, "data").mkdir();

		_repository = new CacheRepositoryV3(base);
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
		entry.setSticky(true, "repository test", System.currentTimeMillis() + 5000);

		assertEquals("setSticky makes file sticky",true, entry.isSticky());
		Thread.sleep(5100);
		assertEquals("entry shold be not stich when life time is over",false, entry.isSticky());

		_repository.removeEntry(entry);
	}

	@Test
	public void testStatTransitions() throws Exception {

		String id = generateNewID();
		PnfsId pnfsId = new PnfsId(id);
		_repository.createEntry(pnfsId);


		CacheRepositoryEntry entry = _repository.getEntry(pnfsId);

		try {
			entry.setSticky(true);
			fail("Sticky not allowed for undefined state");
		}catch(CacheException e) {
			// OK illegal state
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
			fail("Vavlid transition from-clent => cached");
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
			fail("Transition cached => from-clent should throw exception");

		}catch(CacheException e) {
			// OK
		}

		try {
			// we are in caches state
			entry.setReceivingFromStore();
			fail("Transition cached => from-store shoud throw exception");

		}catch(CacheException e) {
			// OK
		}

		try {
			// we are in cached state
			entry.setSendingToStore(true);
			assertEquals("Cached files cant be in TO STORE state",true, entry.isSendingToStore());

		}catch(CacheException e) {
			// OK
		}


		_repository.removeEntry(entry);
	}

	@Test
	public void testBad2Precious() throws Exception {

		String id = generateNewID();
		PnfsId pnfsId = new PnfsId(id);
		_repository.createEntry(pnfsId);


		CacheRepositoryEntry entry = _repository.getEntry(pnfsId);

		entry.setCached();
		entry.setBad(true);

		try {
			entry.setPrecious();
			fail("IllegalStateException shold be thrown on atempt to set bad file precious");
		} catch ( CacheException ise ) {
			// OK
		}

		entry.setPrecious(true);
		_repository.removeEntry(entry);
	}

	@Test
	public void testBackwardCompatibility() throws Exception {
		// to be done
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

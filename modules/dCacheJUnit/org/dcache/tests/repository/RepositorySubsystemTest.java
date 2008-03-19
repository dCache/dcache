
package org.dcache.tests.repository;

import static org.junit.Assert.*;

import java.util.*;
import java.io.*;

import org.junit.*;
import junit.framework.AssertionFailedError;

import com.sleepycat.je.DatabaseException;
import diskCacheV111.repository.*;
import diskCacheV111.util.*;
import diskCacheV111.vehicles.*;
import dmg.cells.nucleus.*;
import dmg.util.*;
import org.dcache.tests.cells.CellAdapterHelper;

import org.dcache.pool.repository.v4.CacheRepositoryV4;
import org.dcache.pool.repository.v5.CacheRepositoryV5;

import org.dcache.pool.repository.SpaceRecord;
import org.dcache.pool.repository.EntryState;
import static org.dcache.pool.repository.EntryState.*;
import org.dcache.pool.repository.ReadHandle;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.StateChangeListener;
import org.dcache.pool.repository.StateChangeEvent;;

public class RepositorySubsystemTest
    implements StateChangeListener
{
    private long size1 = 1024;
    private long size2 = 1024;
    private long size3 = 1024;

    private PnfsId id1; // Precious
    private PnfsId id2; // Cached
    private PnfsId id3; // Cached + Sticky
    private PnfsId id4; // Non existing entry

    private StorageInfo info1;
    private StorageInfo info2;
    private StorageInfo info3;

    private PnfsHandler pnfs;

    private CacheRepositoryV5 repository;

    private File root;
    private File metaDir;
    private File dataDir;

    private Queue<StateChangeEvent> stateChangeEvents;

    private String args =
        "-metaDataRepository=org.dcache.pool.repository.meta.db.BerkeleyDBMetaDataRepository -sweeper=diskCacheV111.pools.SpaceSweeper2";
    private CellAdapterHelper cell;

    private void createFile(File file, long size)
        throws IOException
    {
        RandomAccessFile handle = new RandomAccessFile(file, "rw");
        try {
            for (long i = 0; i < size; i++)
                handle.writeByte(0);
        } finally {
            handle.close();
        }
    }

    private CacheRepositoryEntry createEntry(CacheRepository repository,
                                             PnfsId id,
                                             StorageInfo info)
        throws CacheException, IOException
    {
        CacheRepositoryEntry entry = repository.createEntry(id);
        createFile(entry.getDataFile(), info.getFileSize());
        entry.setStorageInfo(info);
        return entry;
    }

    private StorageInfo createStorageInfo(long size)
    {
        GenericStorageInfo info = new GenericStorageInfo();
        info.setFileSize(size);
        return info;
    }

    private void deleteDirectory(File dir)
    {
        File[] fileArray = dir.listFiles();

        if (fileArray != null)
        {
            for (int i = 0; i < fileArray.length; i++)
            {
                if (fileArray[i].isDirectory())
                    deleteDirectory(fileArray[i]);
                else
                    fileArray[i].delete();
            }
        }
        dir.delete();
    }

    @Before
    public void setUp()
        throws IOException, CacheException, DatabaseException
    {
        id1 = new PnfsId("000000000001");
        id2 = new PnfsId("000000000002");
        id3 = new PnfsId("000000000003");
        id4 = new PnfsId("000000000004");

        info1 = createStorageInfo(size1);
        info2 = createStorageInfo(size2);
        info3 = createStorageInfo(size3);

        root = File.createTempFile("dtest", null);
        if (!root.delete())
            throw new IOException("Could not delete temp file");
        if (!root.mkdir())
            throw new IOException("Could not create temp dir");
        dataDir = new File(root, "data");
        metaDir = new File(root, "meta");

        if (!dataDir.mkdir())
            throw new IOException("Could not create data dir");
        if (!metaDir.mkdir())
            throw new IOException("Could not create meta dir");

        CacheRepositoryV4 rep = new CacheRepositoryV4(root, new Args(args));
        createEntry(rep, id1, info1).setPrecious(true);
        createEntry(rep, id2, info2).setCached();
        CacheRepositoryEntry entry = createEntry(rep, id3, info3);
        entry.setCached();
        entry.setSticky(true);
        rep.close();

        cell = new CellAdapterHelper("pool", args);
        pnfs = new PnfsHandler(cell, new CellPath("pnfs"), "pool");
        repository = new CacheRepositoryV5(cell, pnfs, root, new Args(args));
        repository.setSize(5120);
        repository.runInventory();
        repository.addListener(this);

        stateChangeEvents = new LinkedList<StateChangeEvent>();
    }

    @After
    public void tearDown()
    {
        if (root != null)
            deleteDirectory(root);
        cell.die();
    }

    public void stateChanged(StateChangeEvent event)
    {
        stateChangeEvents.add(event);
    }

    public void expectStateChangeEvent(PnfsId id, EntryState oldState, EntryState newState)
    {
        StateChangeEvent event = stateChangeEvents.remove();
        assertEquals(id, event.getPnfsId());
        assertEquals(oldState, event.getOldState());
        assertEquals(newState, event.getNewState());
    }

    public void assertNoStateChangeEvent()
    {
        if (stateChangeEvents.size() > 0)
            throw new AssertionFailedError("Unexpected state change event: "
                                           + stateChangeEvents.remove());
    }

    private void assertSpaceRecord(long total, long free, long precious, long removable)
    {
        SpaceRecord space = repository.getSpaceRecord();
        assertEquals(total, space.getTotalSpace());
        assertEquals(free, space.getFreeSpace());
        assertEquals(precious, space.getPreciousSpace());
        assertEquals(removable, space.getRemovableSpace());
    }

    private void assertCacheEntry(CacheEntry entry, PnfsId id,
                                  long size, EntryState state)
    {
        assertEquals(id, entry.getPnfsId());
        assertEquals(size, entry.getReplicaSize());
        assertEquals(state, entry.getState());
    }

    private void assertCanOpen(PnfsId id, long size, EntryState state)
    {
        try {
            ReadHandle handle = repository.openEntry(id);
            try {
                assertEquals(new File(dataDir, id.toString()), handle.getFile());
                assertCacheEntry(handle.getEntry(), id, size, state);
            } finally {
                handle.close();
            }
        } catch (FileNotInCacheException e) {
            throw new AssertionFailedError("Expected entry " + id + " not found");
        }
    }

    @Test
    public void testGetSpaceRecord()
    {
        assertSpaceRecord(5120, 2048, 1024, 1024);
    }

    @Test
    public void testOpenEntry()
    {
        assertCanOpen(id1, size1, PRECIOUS);
        assertCanOpen(id2, size2, CACHED);
        assertCanOpen(id3, size3, CACHED);
    }

    @Test
    public void testStateChange()
        throws FileNotInCacheException
    {
        assertCanOpen(id1, size1, PRECIOUS);
        repository.setState(id1, CACHED);
        expectStateChangeEvent(id1, PRECIOUS, CACHED);
        assertCanOpen(id1, size1, CACHED);

        repository.setState(id1, PRECIOUS);
        expectStateChangeEvent(id1, CACHED, PRECIOUS);
        assertCanOpen(id1, size1, PRECIOUS);
    }

    @Test(expected=IllegalStateException.class)
    public void testClosedReadHandleClose()
        throws FileNotInCacheException
    {
        ReadHandle handle = repository.openEntry(id1);
        handle.close();
        handle.close();
    }

    @Test(expected=IllegalStateException.class)
    public void testClosedReadHandleGetFile()
        throws FileNotInCacheException
    {
        ReadHandle handle = repository.openEntry(id1);
        handle.close();
        handle.getFile();
    }

    @Test(expected=IllegalStateException.class)
    public void testClosedReadHandleGetEntry()
        throws FileNotInCacheException
    {
        ReadHandle handle = repository.openEntry(id1);
        handle.close();
        handle.getEntry();
    }

    @Test
    public void testSetSize()
    {
        repository.setSize(3072);
        assertSpaceRecord(3072, 0, 1024, 1024);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetSizeToSmall()
    {
        repository.setSize(3071);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetSizeNegative()
    {
        repository.setSize(-1);
    }

    @Test(expected=FileNotInCacheException.class)
    public void testRemoval()
        throws FileNotInCacheException
    {
        repository.setState(id1, REMOVED);
        expectStateChangeEvent(id1, PRECIOUS, REMOVED);
        expectStateChangeEvent(id1, REMOVED, DESTROYED);
        repository.openEntry(id1);
    }

    @Test
    public void testRemoveWhileReading()
        throws FileNotInCacheException
    {
        ReadHandle handle1 = repository.openEntry(id1);
        repository.setState(id1, REMOVED);
        expectStateChangeEvent(id1, PRECIOUS, REMOVED);
        assertNoStateChangeEvent();

        handle1.close();
        expectStateChangeEvent(id1, REMOVED, DESTROYED);
    }

    @Test(expected=FileNotInCacheException.class)
    public void testRemoveOpenAgainFails()
        throws FileNotInCacheException
    {
        repository.openEntry(id1);
        repository.setState(id1, REMOVED);
        repository.openEntry(id1);
    }

}



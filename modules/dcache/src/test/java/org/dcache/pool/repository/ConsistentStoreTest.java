package org.dcache.pool.repository;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.net.URI;
import java.net.URISyntaxException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.pool.classic.ALRPReplicaStatePolicy;
import org.dcache.tests.repository.MetaDataRepositoryHelper;
import org.dcache.tests.repository.RepositoryHealerTestChimeraHelper;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.pool.repository.EntryState.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.*;

public class ConsistentStoreTest
{
    private final static String POOL = "test-pool";
    private final static PnfsId PNFSID =
        new PnfsId("000000000000000000000000000000000001");

    private PnfsHandler _pnfs;
    private RepositoryHealerTestChimeraHelper _fileStore;
    private MetaDataStore _metaDataStore;
    private ConsistentStore _consistentStore;

    @Before
    public void setup() throws Exception
    {
        _pnfs = mock(PnfsHandler.class);
        _fileStore = new RepositoryHealerTestChimeraHelper();
        _metaDataStore = new MetaDataRepositoryHelper(_fileStore);
        _consistentStore =
            new ConsistentStore(_pnfs, null, _fileStore, _metaDataStore,
                                new ALRPReplicaStatePolicy());
        _consistentStore.setPoolName(POOL);
    }

    @After
    public void tearDown()
    {
        _fileStore.shutdown();
    }

    private static StorageInfo createStorageInfo(long size)
        throws URISyntaxException
    {
        StorageInfo info = new OSMStorageInfo("h1", "rawd");
        info.addLocation(new URI("osm://mystore/?store=mystore&group=mygroup&bdid=1"));
        info.setLegacySize(size);
        return info;
    }

    private static PnfsGetStorageInfoMessage
        storageInfoMessage(PnfsId pnfsId, StorageInfo info)
    {
        PnfsGetStorageInfoMessage message =
            new PnfsGetStorageInfoMessage(pnfsId);
        FileAttributes attributes = new FileAttributes();
        attributes.setSize(info.getLegacySize());
        attributes.setAccessLatency(info.getLegacyAccessLatency());
        attributes.setRetentionPolicy(info.getLegacyRetentionPolicy());
        attributes.setStorageInfo(info);
        message.setFileAttributes(attributes);
        return message;
    }

    private void givenStoreHasFileOfSize(PnfsId pnfsId, long size)
        throws ChimeraFsException
    {
        _fileStore.add(pnfsId).setSize(17);
    }

    private void givenMetaDataStoreHas(PnfsId pnfsId, EntryState state,
                                              StorageInfo info)
        throws DuplicateEntryException, CacheException
    {
        MetaDataRecord entry = _metaDataStore.create(pnfsId);
        entry.setState(state);
        FileAttributes attributes = StorageInfos.injectInto(info, new FileAttributes());
        attributes.setPnfsId(pnfsId);
        entry.setFileAttributes(attributes);
    }

    @Test
    public void shouldMarkNonTransientReplicasWithWrongSizeAsBroken()
        throws Exception
    {
        // Given a replica with one file size
        givenStoreHasFileOfSize(PNFSID, 17);

        // and given the meta data indicates a different size, but is
        // otherwise in a valid non-transient state
        StorageInfo info = createStorageInfo(20);
        givenMetaDataStoreHas(PNFSID, CACHED, info);

        // and given that the name space provides the same storage info
        given(_pnfs.getStorageInfoByPnfsId(PNFSID))
            .willReturn(storageInfoMessage(PNFSID, info));

        // when reading the meta data record
        MetaDataRecord record = _consistentStore.get(PNFSID);

        // then the replica is marked broken
        assertThat(record.getState(), is(EntryState.BROKEN));

        // and the storage info size is unaltered
        assertThat(record.getFileAttributes().getSize(), is(20L));

        // and no attributes are updated in the name space
        verify(_pnfs, never())
            .setFileAttributes(eq(PNFSID), Mockito.any(FileAttributes.class));
        verify(_pnfs, never())
            .clearCacheLocation(PNFSID);
    }

    @Test
    public void shouldRegisterFileSizeAndLocationOnIncompleteUpload()
        throws Exception
    {
        // Given a replica
        givenStoreHasFileOfSize(PNFSID, 17);

        // and given the replica is an incomplete upload
        StorageInfo info = createStorageInfo(0);
        givenMetaDataStoreHas(PNFSID, FROM_CLIENT, info);

        // and given the name space entry exists without any size
        given(_pnfs.getStorageInfoByPnfsId(PNFSID))
            .willReturn(storageInfoMessage(PNFSID, info));

        // when reading the meta data record
        MetaDataRecord record = _consistentStore.get(PNFSID);

        // then the correct size is set in storage info
        assertThat(_metaDataStore.get(PNFSID).getFileAttributes().getSize(),
                   is(17L));

        // and the correct file size and location is registered in
        // the name space
        ArgumentCaptor<FileAttributes> captor =
            ArgumentCaptor.forClass(FileAttributes.class);
        verify(_pnfs).setFileAttributes(eq(PNFSID), captor.capture());
        assertThat(captor.getValue().getSize(), is(17L));
        assertThat(captor.getValue().getLocations(), hasItem(POOL));

        // and the record is no longer in an upload state
        assertThat(record.getState(), is(not(EntryState.FROM_CLIENT)));
    }

    @Test
    public void shouldDeleteIncompleteReplicasWithNoNameSpaceEntry()
        throws Exception
    {
        // Given a replica
        givenStoreHasFileOfSize(PNFSID, 17);

        // and given the replica is in an incomplete upload
        StorageInfo info = createStorageInfo(0);
        givenMetaDataStoreHas(PNFSID, FROM_CLIENT, info);

        // and given the name space entry does not exist
        given(_pnfs.getStorageInfoByPnfsId(PNFSID))
            .willThrow(new FileNotFoundCacheException("No such file"));

        // when reading the meta data record
        MetaDataRecord record = _consistentStore.get(PNFSID);

        // then nothing is returned
        assertThat(record, is(nullValue()));

        // and the replica is deleted
        assertThat(_metaDataStore.get(PNFSID), is(nullValue()));
        assertThat(_fileStore.get(PNFSID).exists(), is(false));

        // and the location is cleared
        verify(_pnfs).clearCacheLocation(PNFSID);

        // and the name space entry is not touched
        verify(_pnfs, never())
            .setFileAttributes(eq(PNFSID), Mockito.any(FileAttributes.class));
    }

    @Test
    public void shouldDeleteBrokenReplicasWithNoNameSpaceEntry()
        throws Exception
    {
        // Given a replica with no meta data
        givenStoreHasFileOfSize(PNFSID, 17);

        // and given the name space entry does not exist
        given(_pnfs.getStorageInfoByPnfsId(PNFSID))
            .willThrow(new FileNotFoundCacheException("No such file"));

        // when reading the meta data record
        MetaDataRecord record = _consistentStore.get(PNFSID);

        // then recovery is attempted
        verify(_pnfs).getStorageInfoByPnfsId(PNFSID);

        // but nothing is returned
        assertThat(record, is(nullValue()));

        // and the replica is deleted
        assertThat(_metaDataStore.get(PNFSID), is(nullValue()));
        assertThat(_fileStore.get(PNFSID).exists(), is(false));

        // and the location is cleared
        verify(_pnfs).clearCacheLocation(PNFSID);

        // and the name space entry is not touched
        verify(_pnfs, never())
            .setFileAttributes(eq(PNFSID), Mockito.any(FileAttributes.class));
    }

    @Test
    public void shouldRecoverBrokenEntries()
            throws Exception
    {
        // Given a replica
        givenStoreHasFileOfSize(PNFSID, 17);

        // and given the replica is marked broken
        StorageInfo info = createStorageInfo(0);
        givenMetaDataStoreHas(PNFSID, BROKEN, info);

        // and given the name space entry exists without any size
        given(_pnfs.getStorageInfoByPnfsId(PNFSID))
                .willReturn(storageInfoMessage(PNFSID, info));

        // when reading the meta data record
        MetaDataRecord record = _consistentStore.get(PNFSID);

        // then the correct size is set in storage info
        assertThat(_metaDataStore.get(PNFSID).getFileAttributes().getSize(),
                is(17L));

        // and the correct file size and location is registered in
        // the name space
        ArgumentCaptor<FileAttributes> captor =
                ArgumentCaptor.forClass(FileAttributes.class);
        verify(_pnfs).setFileAttributes(eq(PNFSID), captor.capture());
        assertThat(captor.getValue().getSize(), is(17L));
        assertThat(captor.getValue().getLocations(), hasItem(POOL));

        // and the record is no longer in a broken state
        assertThat(record.getState(), is(EntryState.CACHED));
    }

    @Test
    public void shouldDeleteIncompleteRestores()
        throws Exception
    {
        // Given a replica with one file size
        givenStoreHasFileOfSize(PNFSID, 17);

        // and given the replica meta data indicates the file was
        // being restored from tape and has is supposed to have a
        // different file size,
        StorageInfo info = createStorageInfo(20);
        givenMetaDataStoreHas(PNFSID, FROM_STORE, info);

        // when reading the meta data record
        MetaDataRecord record = _consistentStore.get(PNFSID);

        // then nothing is returned
        assertThat(record, is(nullValue()));

        // and the replica is deleted
        assertThat(_metaDataStore.get(PNFSID), is(nullValue()));
        assertThat(_fileStore.get(PNFSID).exists(), is(false));

        // and the location is cleared
        verify(_pnfs).clearCacheLocation(PNFSID);

        // and the name space entry is not touched
        verify(_pnfs, never())
            .setFileAttributes(eq(PNFSID), Mockito.any(FileAttributes.class));
    }

    @Test
    public void shouldMarkMissingEntriesWithWrongSizeAsBroken()
        throws Exception
    {
        // Given a replica with missing meta data
        givenStoreHasFileOfSize(PNFSID, 17);

        // and given the name space entry reports a different size
        StorageInfo info = createStorageInfo(20);
        given(_pnfs.getStorageInfoByPnfsId(PNFSID))
            .willReturn(storageInfoMessage(PNFSID, info));

        // when reading the meta data record
        MetaDataRecord record = _consistentStore.get(PNFSID);

        // then the replica is marked broken
        assertThat(record.getState(), is(EntryState.BROKEN));

        // and the name space entry is untouched
        verify(_pnfs, never())
            .setFileAttributes(eq(PNFSID), Mockito.any(FileAttributes.class));
    }

    @Test
    public void shouldNotTalkToNameSpaceForIntactEntries()
        throws Exception
    {
        // Given a replica
        givenStoreHasFileOfSize(PNFSID, 17);

        // and given the replica has intact meta data
        givenMetaDataStoreHas(PNFSID, CACHED, createStorageInfo(17));

        // when reading the meta data record
        MetaDataRecord record = _consistentStore.get(PNFSID);

        // then there is no interaction with the name space
        verifyNoMoreInteractions(_pnfs);
    }

    @Test
    public void shouldSilentlyIgnoreRemoveOfNonExistingReplicas()
    {
        _consistentStore.remove(PNFSID);
    }
}

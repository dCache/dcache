package org.dcache.pool.repository;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.cells.CellStub;
import org.dcache.pool.classic.ALRPReplicaStatePolicy;
import org.dcache.tests.repository.MetaDataRepositoryHelper;
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
    private FlatFileStore _fileStore;
    private MetaDataStore _metaDataStore;
    private ConsistentStore _consistentStore;
    private CellStub _broadcast;

    @Before
    public void setup() throws Exception
    {
        _pnfs = mock(PnfsHandler.class);
        _broadcast = mock(CellStub.class);

        FileSystem fileSystem = Jimfs.newFileSystem(Configuration.unix());
        _fileStore = new FlatFileStore(fileSystem.getPath("/"));
        _metaDataStore = new MetaDataRepositoryHelper(_fileStore);
        _consistentStore =
            new ConsistentStore(_pnfs, null, _metaDataStore,
                                new ALRPReplicaStatePolicy());
        _consistentStore.setPoolName(POOL);
    }

    private static FileAttributes createFileAttributes(PnfsId pnfsId)
        throws URISyntaxException
    {
        StorageInfo info = new OSMStorageInfo("h1", "rawd");
        info.addLocation(new URI("osm://mystore/?store=mystore&group=mygroup&bdid=1"));
        FileAttributes attributes = new FileAttributes();
        attributes.setPnfsId(pnfsId);
        attributes.setStorageInfo(info);
        attributes.setAccessLatency(AccessLatency.NEARLINE);
        attributes.setRetentionPolicy(RetentionPolicy.CUSTODIAL);
        return attributes;
    }

    private static FileAttributes createFileAttributes(PnfsId pnfsId, long size)
        throws URISyntaxException
    {
        FileAttributes attributes = createFileAttributes(pnfsId);
        attributes.setSize(size);
        return attributes;
    }

    private void givenStoreHasFileOfSize(PnfsId pnfsId, long size)
            throws IOException
    {
        Path path = _fileStore.get(pnfsId);
        try (OutputStream stream = Files.newOutputStream(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            stream.write(new byte[(int) size]);
        }
    }

    private void givenMetaDataStoreHas(EntryState state, FileAttributes attributes)
        throws DuplicateEntryException, CacheException
    {
        MetaDataRecord entry = _metaDataStore.create(attributes.getPnfsId(), EnumSet.noneOf(Repository.OpenFlags.class));
        entry.update(r -> {
            r.setState(state);
            r.setFileAttributes(attributes);
            return null;
        });
    }

    @Test
    public void shouldMarkNonTransientReplicasWithWrongSizeAsBroken()
        throws Exception
    {
        // Given a replica with one file size
        givenStoreHasFileOfSize(PNFSID, 17);

        // and given the meta data indicates a different size, but is
        // otherwise in a valid non-transient state
        FileAttributes info = createFileAttributes(PNFSID, 20);
        givenMetaDataStoreHas(CACHED, info);

        // and given that the name space provides the same storage info
        given(_pnfs.getFileAttributes(eq(PNFSID), Mockito.anySet())).willReturn(info);

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
        FileAttributes info = createFileAttributes(PNFSID);
        givenMetaDataStoreHas(FROM_CLIENT, info);

        // and given the name space entry exists without any size
        given(_pnfs.getFileAttributes(eq(PNFSID), Mockito.anySet())).willReturn(info);

        // when reading the meta data record
        MetaDataRecord record = _consistentStore.get(PNFSID);

        // then the correct size is set in file attributes info
        assertThat(_metaDataStore.get(PNFSID).getFileAttributes().getSize(), is(17L));

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
        FileAttributes info = createFileAttributes(PNFSID, 0);
        givenMetaDataStoreHas(FROM_CLIENT, info);

        // and given the name space entry does not exist
        given(_pnfs.getFileAttributes(eq(PNFSID), Mockito.anySet()))
            .willThrow(new FileNotFoundCacheException("No such file"));

        // when reading the meta data record
        MetaDataRecord record = _consistentStore.get(PNFSID);

        // then nothing is returned
        assertThat(record, is(nullValue()));

        // and the replica is deleted
        assertThat(_metaDataStore.get(PNFSID), is(nullValue()));
        assertThat(Files.exists(_fileStore.get(PNFSID)), is(false));

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
        given(_pnfs.getFileAttributes(eq(PNFSID), Mockito.anySet()))
            .willThrow(new FileNotFoundCacheException("No such file"));

        // when reading the meta data record
        MetaDataRecord record = _consistentStore.get(PNFSID);

        // then recovery is attempted
        verify(_pnfs).getFileAttributes(eq(PNFSID), Mockito.anySet());

        // but nothing is returned
        assertThat(record, is(nullValue()));

        // and the replica is deleted
        assertThat(_metaDataStore.get(PNFSID), is(nullValue()));
        assertThat(Files.exists(_fileStore.get(PNFSID)), is(false));

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
        FileAttributes info = createFileAttributes(PNFSID);
        givenMetaDataStoreHas(BROKEN, info);

        // and given the name space entry exists without any size
        given(_pnfs.getFileAttributes(eq(PNFSID), Mockito.anySet())).willReturn(info);

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
        // being restored from tape and is supposed to have a
        // different file size,
        FileAttributes info = createFileAttributes(PNFSID, 20);
        givenMetaDataStoreHas(FROM_STORE, info);

        // when reading the meta data record
        MetaDataRecord record = _consistentStore.get(PNFSID);

        // then nothing is returned
        assertThat(record, is(nullValue()));

        // and the replica is deleted
        assertThat(_metaDataStore.get(PNFSID), is(nullValue()));
        assertThat(Files.exists(_fileStore.get(PNFSID)), is(false));

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
        FileAttributes info = createFileAttributes(PNFSID, 20);
        given(_pnfs.getFileAttributes(eq(PNFSID), Mockito.anySet())).willReturn(info);

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
        givenMetaDataStoreHas(CACHED, createFileAttributes(PNFSID, 17));

        // when reading the meta data record
        MetaDataRecord record = _consistentStore.get(PNFSID);

        // then there is no interaction with the name space
        verifyNoMoreInteractions(_pnfs);
    }

    @Test
    public void shouldSilentlyIgnoreRemoveOfNonExistingReplicas() throws CacheException
    {
        _consistentStore.remove(PNFSID);
    }
}

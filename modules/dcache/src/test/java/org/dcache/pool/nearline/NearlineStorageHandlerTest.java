package org.dcache.pool.nearline;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.MoreExecutors;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.GenericStorageInfo;
import java.net.URI;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.dcache.cells.CellStub;
import org.dcache.chimera.InodeId;
import org.dcache.namespace.FileType;
import org.dcache.pool.classic.ChecksumModule;
import org.dcache.pool.nearline.spi.NearlineRequest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.ModifiableReplicaDescriptor;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class NearlineStorageHandlerTest {

    private NearlineStorageHandler nsh;
    private Repository repository;
    private NearlineStorage nearlineStorage;
    private HsmSet hsmSet;
    private CompletionHandler<Void, PnfsId> hsmMigrationRequestCallack;
    private CompletionHandler<Void, URI> hsmRemoveRequestCallack;
    private PnfsHandler pnfs;
    private FileStore fileStore;
    private ChecksumModule csm;
    private ModifiableReplicaDescriptor desc;

    @Before
    public void setUp() throws Exception {

        repository = mock(Repository.class);
        desc = mock(ModifiableReplicaDescriptor.class);
        pnfs = mock(PnfsHandler.class);
        fileStore = mock(FileStore.class);
        csm = mock(ChecksumModule.class);

        hsmSet = mock(HsmSet.class);
        when(hsmSet.describe(any())).thenReturn(mock(HsmSet.HsmDescription.class));
        nearlineStorage = mock(NearlineStorage.class);

        when(hsmSet.getNearlineStorageByType(any())).thenReturn(nearlineStorage);
        when(hsmSet.getNearlineStorageByName(any())).thenReturn(nearlineStorage);

        var billingCell = mock(CellStub.class);
        nsh = new NearlineStorageHandler();
        nsh.setExecutor(MoreExecutors.newDirectExecutorService());
        nsh.setBillingStub(billingCell);
        nsh.setHsmSet(hsmSet);
        nsh.setRepository(repository);
        nsh.setPnfsHandler(pnfs);
        nsh.setFileStore(fileStore);
        nsh.setChecksumModule(csm);
        nsh.setStickyOnStageDuration(5);
        nsh.setStickyOnStageDurationUnit(TimeUnit.MINUTES);

        hsmMigrationRequestCallack = mock(CompletionHandler.class);
        hsmRemoveRequestCallack = mock(CompletionHandler.class);
    }

    @Test
    public void testQueuedFlushOnSubmit() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));

        assertThat(nsh.getStoreQueueSize(), is(0));
        nsh.flush("foo", Set.of(attr.getPnfsId()), hsmMigrationRequestCallack);
        assertThat(nsh.getStoreQueueSize(), is(1));
    }

    @Test
    public void testQueuedStageOnSubmit() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));

        assertThat(nsh.getFetchQueueSize(), is(0));
        nsh.stage("foo", attr, hsmMigrationRequestCallack);
        assertThat(nsh.getFetchQueueSize(), is(1));
    }

    @Test
    public void testQueuedRemoveOnSubmit() throws CacheException {

        assertThat(nsh.getRemoveQueueSize(), is(0));
        nsh.remove("foo", Set.of(URI.create("foo://bar/271")), hsmRemoveRequestCallack);
        assertThat(nsh.getRemoveQueueSize(), is(1));
    }

    @Test
    public void testFailFlush() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));

        nsh.flush("foo", Set.of(attr.getPnfsId()), hsmMigrationRequestCallack);

        var requests = givenAllFlushesActive();

        requests.forEach(r -> r.failed(1, "forcefully failed"));

        verify(hsmMigrationRequestCallack).failed(any(), any());
        assertThat(nsh.getStoreQueueSize(), is(0));
    }

    @Test
    public void testFailStage() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));

        nsh.stage("foo", attr, hsmMigrationRequestCallack);

        var requests = givenAllStagesActive();

        requests.forEach(r -> r.failed(1, "forcefully failed"));

        verify(hsmMigrationRequestCallack).failed(any(), any());
        assertThat(nsh.getStoreQueueSize(), is(0));
    }

    @Test
    public void testFailRemove() throws CacheException {

        nsh.remove("foo", Set.of(URI.create("foo://foo/217")), hsmRemoveRequestCallack);

        var requests = givenAllRemovesActive();

        requests.forEach(r -> r.failed(1, "forcefully failed"));

        verify(hsmRemoveRequestCallack).failed(any(), any());
        assertThat(nsh.getRemoveQueueSize(), is(0));
    }

    @Test
    public void testActiveFlushOnActivate() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));

        nsh.flush("foo", Set.of(attr.getPnfsId()), hsmMigrationRequestCallack);
        givenAllFlushesActive();
        assertThat(nsh.getStoreQueueSize(), is(0));
        assertThat(nsh.getActiveStoreJobs(), is(1));
    }

    @Test
    public void testActiveStageOnActivate() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));

        nsh.stage("foo", attr, hsmMigrationRequestCallack);
        givenAllStagesActive();
        assertThat(nsh.getFetchQueueSize(), is(0));
        assertThat(nsh.getActiveFetchJobs(), is(1));
    }

    @Test
    public void testActiveRemoveOnActivate() throws CacheException {

        nsh.remove("foo", Set.of(URI.create("foo://foo/217")), hsmRemoveRequestCallack);
        givenAllRemovesActive();

        assertThat(nsh.getRemoveQueueSize(), is(0));
        assertThat(nsh.getActiveRemoveJobs(), is(1));
    }

    @Test
    public void testFailTwiceFlush() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));

        nsh.flush("foo", Set.of(attr.getPnfsId()), hsmMigrationRequestCallack);

        var requests = givenAllFlushesActive();

        requests.forEach(r -> r.failed(1, "forcefully failed"));
        requests.forEach(r -> r.failed(1, "forcefully failed"));

        assertThat(nsh.getStoreQueueSize(), is(0));
        verify(hsmMigrationRequestCallack, times(1)).failed(any(), any());
    }

    @Test
    public void testFailTwiceStage() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));

        nsh.stage("foo", attr, hsmMigrationRequestCallack);

        var requests = givenAllStagesActive();

        requests.forEach(r -> r.failed(1, "forcefully failed"));
        requests.forEach(r -> r.failed(1, "forcefully failed"));

        verify(hsmMigrationRequestCallack, times(1)).failed(any(), any());
        assertThat(nsh.getStoreQueueSize(), is(0));
    }

    @Test
    public void testFailTwiceRemove() throws CacheException {

        nsh.remove("foo", Set.of(URI.create("foo://foo/217")), hsmRemoveRequestCallack);

        var requests = givenAllRemovesActive();

        requests.forEach(r -> r.failed(1, "forcefully failed"));
        requests.forEach(r -> r.failed(1, "forcefully failed"));

        verify(hsmRemoveRequestCallack, times(1)).failed(any(), any());
        assertThat(nsh.getRemoveQueueSize(), is(0));
    }

    @Test
    public void testCompleteActiveFlush() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));

        nsh.flush("foo", Set.of(attr.getPnfsId()), hsmMigrationRequestCallack);
        var requests = givenAllFlushesActive();

        requests.forEach(r -> r.completed(Set.of(URI.create("foo://bar/271"))));
        assertThat(nsh.getActiveStoreJobs(), is(0));
        verify(hsmMigrationRequestCallack).completed(any(), any());
    }

    @Test
    public void testFailExceptionallyActiveFlush() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));

        doThrow(new IllegalStateException("injected")).when(desc).close();
        nsh.flush("foo", Set.of(attr.getPnfsId()), hsmMigrationRequestCallack);
        var requests = givenAllFlushesActive();

        requests.forEach(r -> r.failed(1, "injected"));
        assertThat(nsh.getActiveStoreJobs(), is(0));
        verify(hsmMigrationRequestCallack).failed(any(), any());
    }

    @Test
    public void testFailExceptionallyActiveStage() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));

        doThrow(new IllegalStateException("injected")).when(desc).close();
        nsh.stage("foo", attr, hsmMigrationRequestCallack);
        var requests = givenAllStagesActive();

        requests.forEach(r -> r.failed(1, "injected"));
        assertThat(nsh.getActiveFetchJobs(), is(0));
        verify(hsmMigrationRequestCallack).failed(any(), any());
    }

    @Test
    public void testCompleteActiveStage() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));

        nsh.stage("foo", attr, hsmMigrationRequestCallack);
        var requests = givenAllStagesActive();

        requests.forEach(r -> r.completed(Set.of(Checksum.parseChecksum("1:00000001"))));
        assertThat(nsh.getActiveFetchJobs(), is(0));
        verify(hsmMigrationRequestCallack).completed(any(), any());
    }

    @Test
    public void testCompleteActiveRemove() throws CacheException {

        nsh.remove("foo", Set.of(URI.create("foo://bar/271")), hsmRemoveRequestCallack);
        var requests = givenAllRemovesActive();

        requests.forEach(r -> r.completed(null));
        assertThat(nsh.getActiveRemoveJobs(), is(0));
        verify(hsmRemoveRequestCallack).completed(any(), any());
    }

    @Test
    public void testCompleteTwiceActiveFlush() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));

        nsh.flush("foo", Set.of(attr.getPnfsId()), hsmMigrationRequestCallack);
        var requests = givenAllFlushesActive();

        requests.forEach(r -> r.completed(Set.of(URI.create("foo://bar/271"))));
        requests.forEach(r -> r.completed(Set.of(URI.create("foo://bar/271"))));
        assertThat(nsh.getActiveStoreJobs(), is(0));
        verify(hsmMigrationRequestCallack, times(1)).completed(any(), any());
    }

    @Test
    public void testFlushQueueStateOnError() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));


        when(repository.openEntry(any(), any())).thenThrow(new FileNotInCacheException("injected"));
        nsh.flush("foo", Set.of(attr.getPnfsId()), hsmMigrationRequestCallack);

        assertThat(nsh.getActiveStoreJobs(), is(0));
        assertThat(nsh.getStoreQueueSize(), is(0));
        verify(hsmMigrationRequestCallack).failed(any(), any());
    }

    @Test
    public void testStageQueueStateOnError() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));


        when(repository.createEntry(any(), any(), any(), any(), any(), any())).thenThrow(new FileExistsCacheException("injected"));
        nsh.stage("foo", attr, hsmMigrationRequestCallack);

        assertThat(nsh.getActiveFetchJobs(), is(0));
        assertThat(nsh.getFetchQueueSize(), is(0));
        verify(hsmMigrationRequestCallack).failed(any(), any());
    }

    @Test
    public void testCompleteTwiceActiveStage() throws CacheException {
        var attr = given(aFile()
              .withStorageClass("a:b", "foo")
              .withSize(34567));

        nsh.stage("foo", attr, hsmMigrationRequestCallack);
        var requests = givenAllStagesActive();

        requests.forEach(r -> r.completed(Set.of(Checksum.parseChecksum("1:00000001"))));
        requests.forEach(r -> r.completed(Set.of(Checksum.parseChecksum("1:00000001"))));
        assertThat(nsh.getActiveFetchJobs(), is(0));
        verify(hsmMigrationRequestCallack, times(1)).completed(any(), any());
    }

    @Test
    public void testCompleteTwiceActiveRemove() throws CacheException {

        nsh.remove("foo", Set.of(URI.create("foo://bar/271")), hsmRemoveRequestCallack);
        var requests = givenAllRemovesActive();

        requests.forEach(r -> r.completed(null));
        requests.forEach(r -> r.completed(null));
        assertThat(nsh.getActiveRemoveJobs(), is(0));
        verify(hsmRemoveRequestCallack, times(1)).completed(any(), any());
    }


    // Regression test for #7511: racing lifecycle transitions (activate, cancel, failure)
    @Test
    public void testConcurrentCancelActivateAndFailure() throws Exception {
        int nFiles = 200;
        List<PnfsId> ids = new ArrayList<>();
        for (int i = 0; i < nFiles; i++) {
            var fa = FileAttributes.of()
                  .fileType(FileType.REGULAR)
                  .accessLatency(AccessLatency.NEARLINE)
                  .retentionPolicy(RetentionPolicy.CUSTODIAL)
                  .pnfsId(InodeId.newID(0))
                  .storageClass("a:b")
                  .hsm("foo")
                  .storageInfo(GenericStorageInfo.valueOf("a:b@foo", "*"))
                  .size(34567)
                  .build();
            var descriptor = mock(ModifiableReplicaDescriptor.class);
            when(descriptor.getFileAttributes()).thenReturn(fa);
            when(repository.openEntry(eq(fa.getPnfsId()), any())).thenReturn(descriptor);
            ids.add(fa.getPnfsId());
        }

        nsh.flush("foo", ids, hsmMigrationRequestCallack);

        var ac = ArgumentCaptor.forClass(Iterable.class);
        verify(nearlineStorage).flush(ac.capture());
        List<NearlineRequest> requests = (List<NearlineRequest>) StreamSupport.stream(
              ac.getValue().spliterator(), false).collect(
              Collectors.toList());

        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < requests.size(); i++) {
                NearlineRequest request = requests.get(i);
                PnfsId pnfsId = ids.get(i);
                futures.add(pool.submit(() -> request.activate()));
                futures.add(pool.submit(() -> request.activate()));
                futures.add(pool.submit(() -> nsh.stateChanged(removedEvent(pnfsId))));
                futures.add(pool.submit(() -> nsh.stateChanged(removedEvent(pnfsId))));
                futures.add(pool.submit(() -> request.failed(1, "forcefully failed")));
            }
            for (Future<?> future : futures) {
                /* propagates any uncaught invariant violation, e.g. a negative counter */
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(nsh.getStoreQueueSize(), is(0));
        assertThat(nsh.getActiveStoreJobs(), is(0));
        verify(hsmMigrationRequestCallack, times(nFiles)).failed(any(), any());
        verify(hsmMigrationRequestCallack, never()).completed(any(), any());
    }

    private StateChangeEvent removedEvent(PnfsId pnfsId) {
        CacheEntry entry = mock(CacheEntry.class);
        when(entry.getPnfsId()).thenReturn(pnfsId);
        return new StateChangeEvent("file deleted", entry, entry,
              ReplicaState.CACHED, ReplicaState.REMOVED);
    }

    private class FileBuilder {

        FileAttributes.Builder faBuilder;

        private FileBuilder() {
            faBuilder = FileAttributes.of();
            faBuilder.fileType(FileType.REGULAR)
                  .accessLatency(AccessLatency.NEARLINE)
                  .retentionPolicy(RetentionPolicy.CUSTODIAL)
                  .pnfsId(InodeId.newID(0));
        }

        FileBuilder withStorageClass(String storageClass, String hsm) {
            faBuilder.storageClass(storageClass);
            faBuilder.hsm(hsm);
            faBuilder.storageInfo(GenericStorageInfo.valueOf(storageClass + "@" + hsm, "*"));
            return this;
        }

        FileBuilder withSize(long size) {
            faBuilder.size(size);
            return this;
        }

        FileAttributes build() throws CacheException {
            var fa = faBuilder.build();
            var id = fa.getPnfsId();

            when(desc.getFileAttributes()).thenReturn(fa);

            when(repository.openEntry(eq(id), any())).thenReturn(desc);
            when(repository.createEntry(any(), any(), any(), any(), any(), any())).thenReturn(desc);

            return fa;
        }
    }

    private FileAttributes given(FileBuilder fileBuilder) throws CacheException {
        return fileBuilder.build();
    }

    private FileBuilder aFile() {
        return new FileBuilder();
    }

    private List<NearlineRequest> givenAllFlushesActive() {

        var ac = ArgumentCaptor.forClass(Iterable.class);
        verify(nearlineStorage).flush(ac.capture());
        List<NearlineRequest> requests = (List<NearlineRequest>) StreamSupport.stream(
              ac.getValue().spliterator(), false).collect(
              Collectors.toList());

        requests.forEach(r -> r.activate());
        return requests;
    }

    private Collection<NearlineRequest> givenAllStagesActive() {

        var ac = ArgumentCaptor.forClass(Iterable.class);
        verify(nearlineStorage).stage(ac.capture());
        List<NearlineRequest> requests = (List<NearlineRequest>) StreamSupport.stream(
              ac.getValue().spliterator(), false).collect(
              Collectors.toList());

        requests.forEach(r -> r.activate());
        return requests;
    }

    private Collection<NearlineRequest> givenAllRemovesActive() {

        var ac = ArgumentCaptor.forClass(Iterable.class);
        verify(nearlineStorage).remove(ac.capture());
        List<NearlineRequest> requests = (List<NearlineRequest>) StreamSupport.stream(
              ac.getValue().spliterator(), false).collect(
              Collectors.toList());

        requests.forEach(r -> r.activate());
        return requests;
    }
}

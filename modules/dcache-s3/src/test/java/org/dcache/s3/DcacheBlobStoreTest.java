package org.dcache.s3;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileType;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListSource;
import org.dcache.util.list.DirectoryStream;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.gaul.s3proxy.S3ErrorCode;
import org.gaul.s3proxy.S3ProxyException;
import org.gaul.s3proxy.blobstore.S3Exceptions;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectStorageClass;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.RestoreObjectRequest;
import software.amazon.awssdk.services.s3.model.RestoreRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.StorageClass;

public class DcacheBlobStoreTest {

    private static final FsPath ROOT = FsPath.create("/data/s3");
    private static final long MTIME = 1_700_000_000_000L;
    private static final String MD5 = "9e107d9d372bb6826bd81d3542a419d6";

    private final Map<FsPath, FileAttributes> namespace = new HashMap<>();
    private final Map<FsPath, byte[]> content = new HashMap<>();
    private final Set<FsPath> onTapeOnly = new HashSet<>();
    private final List<FsPath> listed = new ArrayList<>();
    private final List<Subject> requestSubjects = new ArrayList<>();
    private Set<FsPath> denied = new HashSet<>();

    private Subject subject;
    private CellStub pnfsStub;
    private DirectoryListSource lister;
    private PoolMonitor poolMonitor;
    private S3ObjectReader reader;
    private S3ObjectRestorer restorer;
    private DcacheBlobStore store;
    private int nextId;

    @Before
    public void setup() throws Exception {
        subject = new Subject();
        subject.getPrincipals().add(new UidPrincipal(1000));

        pnfsStub = mock(CellStub.class);
        when(pnfsStub.send(any(PnfsGetFileAttributes.class), anyLong())).thenAnswer(i -> {
            PnfsGetFileAttributes msg = i.getArgument(0);
            requestSubjects.add(msg.getSubject());
            getFileAttributes(msg);
            return Futures.immediateFuture(msg);
        });

        lister = mock(DirectoryListSource.class);
        when(lister.list(any(), any(), any(), any(), any(), any())).thenAnswer(
              i -> list(i.getArgument(2)));

        poolMonitor = mock(PoolMonitor.class);
        when(poolMonitor.getFileLocality(any(), eq("192.0.2.1"))).thenAnswer(i -> {
            FileAttributes attributes = i.getArgument(0);
            return onTapeOnly.stream()
                  .anyMatch(p -> namespace.get(p).getPnfsId().equals(attributes.getPnfsId()))
                  ? FileLocality.NEARLINE : FileLocality.ONLINE;
        });

        restorer = mock(S3ObjectRestorer.class);

        reader = mock(S3ObjectReader.class);
        when(reader.read(any(), any(), any(), anyLong(), anyLong())).thenAnswer(i -> {
            FsPath path = i.getArgument(1);
            long offset = i.getArgument(3);
            long length = i.getArgument(4);
            return new ByteArrayInputStream(content.get(path), (int) offset, (int) length);
        });

        store = newStore(1000);

        givenDirectory("/data/s3");
    }

    // Buckets

    @Test
    public void shouldListBucketsSorted() {
        givenDirectory("/data/s3/zeta");
        givenDirectory("/data/s3/alpha");
        givenDirectory("/data/s3/.hidden");
        givenFile("/data/s3/file", "x");

        assertThat(store.list().buckets().stream().map(Bucket::name).toList(),
              contains("alpha", "zeta"));
    }

    @Test
    public void shouldFindExistingBucket() {
        givenDirectory("/data/s3/bucket");

        store.headBucket(HeadBucketRequest.builder().bucket("bucket").build());
    }

    @Test
    public void shouldNotFindMissingBucket() {
        assertThrows(NoSuchBucketException.class,
              () -> store.headBucket(HeadBucketRequest.builder().bucket("bucket").build()));
    }

    @Test
    public void shouldNotTreatFileAsBucket() {
        givenFile("/data/s3/bucket", "x");

        assertThrows(NoSuchBucketException.class,
              () -> store.headBucket(HeadBucketRequest.builder().bucket("bucket").build()));
    }

    @Test
    public void shouldRunRequestsAsUser() {
        givenDirectory("/data/s3/bucket");

        store.headBucket(HeadBucketRequest.builder().bucket("bucket").build());

        assertThat(requestSubjects, contains(subject));
    }

    // Object metadata

    @Test
    public void shouldReturnObjectMetadata() {
        givenDirectory("/data/s3/bucket");
        FileAttributes attributes = givenFile("/data/s3/bucket/dir/file", "hello");
        attributes.setChecksums(Set.of(new Checksum(ChecksumType.MD5_TYPE, MD5)));
        attributes.setXattrs(Map.of("s3.content-type", "text/plain", "s3.meta.color", "blue",
              "other", "x"));

        HeadObjectResponse head = store.blobMetadata("bucket", "dir/file");

        assertThat(head.contentLength(), is(5L));
        assertThat(head.eTag(), is('"' + MD5 + '"'));
        assertThat(head.contentType(), is("text/plain"));
        assertThat(head.metadata(), is(equalTo(Map.of("color", "blue"))));
        assertThat(head.lastModified(), is(Instant.ofEpochMilli(MTIME)));
        assertThat(head.storageClass(), is(StorageClass.STANDARD));
    }

    @Test
    public void shouldUseDefaultContentType() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello");

        assertThat(store.blobMetadata("bucket", "file").contentType(),
              is("binary/octet-stream"));
    }

    @Test
    public void shouldUseOpaqueETagWithoutMd5() {
        givenDirectory("/data/s3/bucket");
        FileAttributes attributes = givenFile("/data/s3/bucket/file", "hello");
        String eTag = store.blobMetadata("bucket", "file").eTag();

        attributes.setModificationTime(MTIME + 1000);

        assertThat(eTag, endsWith("-1\""));
        assertThat(store.blobMetadata("bucket", "file").eTag(), is(not(eTag)));
    }

    @Test
    public void shouldReportTapeOnlyFileAsGlacier() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello");
        givenOnTapeOnly("/data/s3/bucket/file");

        assertThat(store.blobMetadata("bucket", "file").storageClass(), is(StorageClass.GLACIER));
    }

    @Test
    public void shouldNotFindMissingKey() {
        givenDirectory("/data/s3/bucket");

        assertThrows(NoSuchKeyException.class, () -> store.blobMetadata("bucket", "a/file"));
    }

    @Test
    public void shouldNotFindKeyBelowFile() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/a", "x");

        assertThrows(NoSuchKeyException.class, () -> store.blobMetadata("bucket", "a/file"));
    }

    @Test
    public void shouldNotFindKeyInMissingBucket() {
        assertThrows(NoSuchBucketException.class, () -> store.blobMetadata("bucket", "file"));
    }

    @Test
    public void shouldNotTreatDirectoryAsObject() {
        givenDirectory("/data/s3/bucket");
        givenDirectory("/data/s3/bucket/dir");

        assertThrows(NoSuchKeyException.class, () -> store.blobMetadata("bucket", "dir"));
    }

    @Test
    public void shouldRejectInvalidKey() {
        givenDirectory("/data/s3/bucket");

        S3Exception e = assertThrows(S3Exception.class,
              () -> store.blobMetadata("bucket", "a//b"));
        assertThat(S3Exceptions.errorCode(e), is("InvalidArgument"));
    }

    @Test
    public void shouldDenyAccessWhenNamespaceDenies() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "x");
        denied.add(FsPath.create("/data/s3/bucket/file"));

        S3ProxyException e = assertThrows(S3ProxyException.class,
              () -> store.blobMetadata("bucket", "file"));
        assertThat(e.getError(), is(S3ErrorCode.ACCESS_DENIED));
    }

    // Object content

    @Test
    public void shouldReadWholeObject() throws Exception {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello world");

        ResponseInputStream<GetObjectResponse> blob = store.getBlob(get("file").build());

        assertThat(blob.response().contentLength(), is(11L));
        assertThat(blob.response().contentRange(), is(nullValue()));
        assertThat(new String(blob.readAllBytes(), UTF_8), is("hello world"));
    }

    @Test
    public void shouldReadRange() throws Exception {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello world");

        assertRange("bytes=2-5", "llo ", "bytes 2-5/11");
        assertRange("bytes=-3", "rld", "bytes 8-10/11");
        assertRange("bytes=6-", "world", "bytes 6-10/11");
        assertRange("bytes=6-100", "world", "bytes 6-10/11");
    }

    @Test
    public void shouldRejectUnsatisfiableRange() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello");

        S3Exception e = assertThrows(S3Exception.class,
              () -> store.getBlob(get("file").range("bytes=5-").build()));
        assertThat(e.statusCode(), is(416));
    }

    @Test
    public void shouldNotStartTransferForEmptyObject() throws Exception {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "");

        ResponseInputStream<GetObjectResponse> blob = store.getBlob(get("file").build());

        assertThat(blob.readAllBytes().length, is(0));
        verify(reader, never()).read(any(), any(), any(), anyLong(), anyLong());
    }

    @Test
    public void shouldRefuseToReadTapeOnlyObject() throws Exception {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello");
        givenOnTapeOnly("/data/s3/bucket/file");

        S3Exception e = assertThrows(S3Exception.class, () -> store.getBlob(get("file").build()));

        assertThat(e.statusCode(), is(403));
        assertThat(S3Exceptions.errorCode(e), is("InvalidObjectState"));
        verify(reader, never()).read(any(), any(), any(), anyLong(), anyLong());
    }

    @Test
    public void shouldHonourIfMatch() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello")
              .setChecksums(Set.of(new Checksum(ChecksumType.MD5_TYPE, MD5)));

        store.getBlob(get("file").ifMatch(MD5).build());
        S3Exception e = assertThrows(S3Exception.class,
              () -> store.getBlob(get("file").ifMatch("\"other\"").build()));
        assertThat(e.statusCode(), is(412));
    }

    @Test
    public void shouldHonourIfNoneMatch() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello")
              .setChecksums(Set.of(new Checksum(ChecksumType.MD5_TYPE, MD5)));

        S3Exception e = assertThrows(S3Exception.class,
              () -> store.getBlob(get("file").ifNoneMatch('"' + MD5 + '"').build()));
        assertThat(e.statusCode(), is(304));
    }

    @Test
    public void shouldHonourIfModifiedSince() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello");

        store.getBlob(get("file").ifModifiedSince(Instant.ofEpochMilli(MTIME - 1000)).build());
        S3Exception e = assertThrows(S3Exception.class, () -> store.getBlob(
              get("file").ifModifiedSince(Instant.ofEpochMilli(MTIME)).build()));
        assertThat(e.statusCode(), is(304));
    }

    @Test
    public void shouldHonourIfUnmodifiedSince() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello");

        store.getBlob(get("file").ifUnmodifiedSince(Instant.ofEpochMilli(MTIME)).build());
        S3Exception e = assertThrows(S3Exception.class, () -> store.getBlob(
              get("file").ifUnmodifiedSince(Instant.ofEpochMilli(MTIME - 1000)).build()));
        assertThat(e.statusCode(), is(412));
    }

    // Restore

    @Test
    public void shouldAnswerRestoreOfObjectOnDiskWithOk() throws Exception {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello");

        assertThat(store.restoreObject(restore("file", 7)).sdkHttpResponse().statusCode(),
              is(200));
        verify(restorer, never()).restore(any(), any(), any(), anyInt());
    }

    @Test
    public void shouldStartRestoreOfTapeOnlyObject() throws Exception {
        givenDirectory("/data/s3/bucket");
        FileAttributes attributes = givenFile("/data/s3/bucket/dir/file", "hello");
        givenDirectory("/data/s3/bucket/dir");
        givenOnTapeOnly("/data/s3/bucket/dir/file");

        assertThat(store.restoreObject(restore("dir/file", 7)).sdkHttpResponse().statusCode(),
              is(202));
        verify(restorer).restore(any(), eq(FsPath.create("/data/s3/bucket/dir/file")),
              eq(attributes), eq(7));
    }

    @Test
    public void shouldRestoreForOneDayByDefault() throws Exception {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello");
        givenOnTapeOnly("/data/s3/bucket/file");

        store.restoreObject(RestoreObjectRequest.builder().bucket("bucket").key("file").build());

        verify(restorer).restore(any(), any(), any(), eq(1));
    }

    @Test
    public void shouldRefuseRestoreWhileRestoring() throws Exception {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello");
        givenOnTapeOnly("/data/s3/bucket/file");
        when(restorer.isRestoring(any(), any())).thenReturn(true);

        S3Exception e = assertThrows(S3Exception.class,
              () -> store.restoreObject(restore("file", 1)));

        assertThat(e.statusCode(), is(409));
        assertThat(S3Exceptions.errorCode(e), is("RestoreAlreadyInProgress"));
        verify(restorer, never()).restore(any(), any(), any(), anyInt());
    }

    @Test
    public void shouldNotRestoreMissingObject() {
        givenDirectory("/data/s3/bucket");

        assertThrows(NoSuchKeyException.class, () -> store.restoreObject(restore("file", 1)));
    }

    @Test
    public void shouldReportOngoingRestoreOnHead() throws Exception {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello");
        givenOnTapeOnly("/data/s3/bucket/file");
        when(restorer.isRestoring(any(), any())).thenReturn(true);

        assertThat(store.blobMetadata("bucket", "file").restore(),
              is("ongoing-request=\"true\""));
    }

    @Test
    public void shouldNotReportRestoreOfTapeOnlyObjectNotBeingRestored() throws Exception {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello");
        givenOnTapeOnly("/data/s3/bucket/file");

        assertThat(store.blobMetadata("bucket", "file").restore(), is(nullValue()));
    }

    @Test
    public void shouldNotAskForRestoreOfObjectOnDisk() throws Exception {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello");

        assertThat(store.blobMetadata("bucket", "file").restore(), is(nullValue()));
        verify(restorer, never()).isRestoring(any(), any());
    }

    @Test
    public void shouldAnswerHeadWhenRestoreStatusIsUnknown() throws Exception {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello");
        givenOnTapeOnly("/data/s3/bucket/file");
        when(restorer.isRestoring(any(), any())).thenThrow(new TimeoutCacheException("down"));

        HeadObjectResponse head = store.blobMetadata("bucket", "file");

        assertThat(head.restore(), is(nullValue()));
        assertThat(head.storageClass(), is(StorageClass.GLACIER));
    }

    private static RestoreObjectRequest restore(String key, int days) {
        return RestoreObjectRequest.builder().bucket("bucket").key(key)
              .restoreRequest(RestoreRequest.builder().days(days).build())
              .build();
    }

    // Listing

    @Test
    public void shouldListWithDelimiter() {
        givenTree();

        ListObjectsV2Response page = store.list(list().delimiter("/").build());

        assertThat(keys(page), contains("a-b", "b"));
        assertThat(prefixes(page), contains("a/", "c/"));
        assertThat(page.isTruncated(), is(false));
    }

    @Test
    public void shouldListRecursivelyInKeyOrder() {
        givenTree();

        ListObjectsV2Response page = store.list(list().build());

        assertThat(keys(page), contains("a-b", "a/x", "a/y/z", "b", "c/d"));
    }

    @Test
    public void shouldListWithPrefix() {
        givenTree();

        assertThat(keys(store.list(list().prefix("a/").build())), contains("a/x", "a/y/z"));
        assertThat(keys(store.list(list().prefix("a").build())),
              contains("a-b", "a/x", "a/y/z"));
        ListObjectsV2Response page = store.list(list().prefix("a/").delimiter("/").build());
        assertThat(keys(page), contains("a/x"));
        assertThat(prefixes(page), contains("a/y/"));
    }

    @Test
    public void shouldListNothingForMissingPrefixDirectory() {
        givenTree();

        ListObjectsV2Response page = store.list(list().prefix("nope/").build());

        assertThat(page.contents(), is(emptyIterable()));
    }

    @Test
    public void shouldListNothingForPrefixWithoutPath() {
        givenTree();

        ListObjectsV2Response page = store.list(list().prefix("a//").build());

        assertThat(page.contents(), is(emptyIterable()));
    }

    @Test
    public void shouldNotListMissingBucket() {
        assertThrows(NoSuchBucketException.class, () -> store.list(list().build()));
        assertThrows(NoSuchBucketException.class,
              () -> store.list(list().prefix("a/").build()));
    }

    @Test
    public void shouldHideReservedNames() {
        givenTree();
        givenFile("/data/s3/bucket/.s3-tmp-1234", "x");
        givenDirectory("/data/s3/bucket/.s3-multipart");
        givenFile("/data/s3/bucket/.s3-multipart/part", "x");

        assertThat(keys(store.list(list().build())), contains("a-b", "a/x", "a/y/z", "b", "c/d"));
    }

    @Test
    public void shouldPageListing() {
        givenTree();

        ListObjectsV2Response first = store.list(list().maxKeys(2).build());
        ListObjectsV2Response second = store.list(
              list().maxKeys(2).continuationToken(first.nextContinuationToken()).build());
        ListObjectsV2Response third = store.list(
              list().maxKeys(2).continuationToken(second.nextContinuationToken()).build());

        assertThat(keys(first), contains("a-b", "a/x"));
        assertThat(first.isTruncated(), is(true));
        assertThat(keys(second), contains("a/y/z", "b"));
        assertThat(second.isTruncated(), is(true));
        assertThat(keys(third), contains("c/d"));
        assertThat(third.isTruncated(), is(false));
    }

    @Test
    public void shouldNotMarkFullLastPageAsTruncated() {
        givenTree();

        ListObjectsV2Response page = store.list(list().maxKeys(5).build());

        assertThat(page.isTruncated(), is(false));
        assertThat(page.nextContinuationToken(), is(nullValue()));
    }

    @Test
    public void shouldPageCommonPrefixes() {
        givenTree();

        ListObjectsV2Response first = store.list(list().delimiter("/").maxKeys(2).build());
        ListObjectsV2Response second = store.list(list().delimiter("/").maxKeys(2)
              .continuationToken(first.nextContinuationToken()).build());

        assertThat(keys(first), contains("a-b"));
        assertThat(prefixes(first), contains("a/"));
        assertThat(keys(second), contains("b"));
        assertThat(prefixes(second), contains("c/"));
    }

    @Test
    public void shouldNotListDirectoriesBeforeMarker() {
        givenTree();

        ListObjectsV2Response page = store.list(list().startAfter("b").build());

        assertThat(keys(page), contains("c/d"));
        assertThat(listed.contains(FsPath.create("/data/s3/bucket/a")), is(false));
    }

    @Test
    public void shouldEndPageEarlyAfterReadingTooManyEntries() {
        givenTree();
        store = newStore(1);

        var keys = new ArrayList<String>();
        String token = null;
        do {
            ListObjectsV2Response page = store.list(list().continuationToken(token).build());
            assertThat(page.contents().isEmpty(), is(false));
            keys.addAll(keys(page));
            token = page.nextContinuationToken();
        } while (token != null);

        assertThat(keys, contains("a-b", "a/x", "a/y/z", "b", "c/d"));
    }

    @Test
    public void shouldReportListedObjectAttributes() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello")
              .setChecksums(Set.of(new Checksum(ChecksumType.MD5_TYPE, MD5)));
        givenOnTapeOnly("/data/s3/bucket/file");

        S3Object object = store.list(list().build()).contents().get(0);

        assertThat(object.key(), is("file"));
        assertThat(object.size(), is(5L));
        assertThat(object.eTag(), is('"' + MD5 + '"'));
        assertThat(object.lastModified(), is(Instant.ofEpochMilli(MTIME)));
        assertThat(object.storageClass(), is(ObjectStorageClass.GLACIER));
    }

    @Test
    public void shouldRejectOtherDelimiters() {
        givenTree();

        S3ProxyException e = assertThrows(S3ProxyException.class,
              () -> store.list(list().delimiter("-").build()));
        assertThat(e.getError(), is(S3ErrorCode.NOT_IMPLEMENTED));
    }

    // Writes

    @Test
    public void shouldNotImplementWrites() {
        givenDirectory("/data/s3/bucket");

        S3ProxyException e = assertThrows(S3ProxyException.class, () -> store.putBlob(
              PutObjectRequest.builder().bucket("bucket").key("file").build(),
              InputStream.nullInputStream()));
        assertThat(e.getError(), is(S3ErrorCode.NOT_IMPLEMENTED));
    }

    // Helpers

    private DcacheBlobStore newStore(int maxListEntries) {
        S3User user = new S3User("AKIAEXAMPLE", subject, Restrictions.none(), ROOT);
        return new DcacheBlobStore(() -> user, new PnfsHandler(pnfsStub), lister, poolMonitor,
              reader, restorer, "192.0.2.1", maxListEntries);
    }

    /**
     * bucket/{a-b, a/x, a/y/z, b, c/d}: "a-b" sorts before "a/" as '-' is before '/'.
     */
    private void givenTree() {
        givenDirectory("/data/s3/bucket");
        givenDirectory("/data/s3/bucket/a");
        givenDirectory("/data/s3/bucket/a/y");
        givenDirectory("/data/s3/bucket/c");
        givenFile("/data/s3/bucket/c/d", "d");
        givenFile("/data/s3/bucket/b", "b");
        givenFile("/data/s3/bucket/a/y/z", "z");
        givenFile("/data/s3/bucket/a/x", "x");
        givenFile("/data/s3/bucket/a-b", "ab");
    }

    private void assertRange(String range, String expected, String contentRange)
          throws Exception {
        ResponseInputStream<GetObjectResponse> blob = store.getBlob(
              get("file").range(range).build());
        assertThat(range, new String(blob.readAllBytes(), UTF_8), is(expected));
        assertThat(range, blob.response().contentLength(), is((long) expected.length()));
        assertThat(range, blob.response().contentRange(), is(contentRange));
    }

    private static GetObjectRequest.Builder get(String key) {
        return GetObjectRequest.builder().bucket("bucket").key(key);
    }

    private static ListObjectsV2Request.Builder list() {
        return ListObjectsV2Request.builder().bucket("bucket");
    }

    private static List<String> keys(ListObjectsV2Response page) {
        return page.contents().stream().map(S3Object::key).toList();
    }

    private static List<String> prefixes(ListObjectsV2Response page) {
        return page.commonPrefixes().stream().map(CommonPrefix::prefix).toList();
    }

    private void givenDirectory(String path) {
        namespace.put(FsPath.create(path), FileAttributes.of()
              .fileType(FileType.DIR)
              .pnfsId(nextPnfsId())
              .creationTime(MTIME)
              .modificationTime(MTIME)
              .build());
    }

    private FileAttributes givenFile(String path, String data) {
        FileAttributes attributes = FileAttributes.of()
              .fileType(FileType.REGULAR)
              .pnfsId(nextPnfsId())
              .size(data.length())
              .creationTime(MTIME)
              .modificationTime(MTIME)
              .build();
        namespace.put(FsPath.create(path), attributes);
        content.put(FsPath.create(path), data.getBytes(UTF_8));
        return attributes;
    }

    private void givenOnTapeOnly(String path) {
        onTapeOnly.add(FsPath.create(path));
    }

    private PnfsId nextPnfsId() {
        return new PnfsId(String.format("0000%032X", nextId++));
    }

    private void getFileAttributes(PnfsGetFileAttributes msg) {
        FsPath path = FsPath.create(msg.getPnfsPath());
        for (FsPath parent = path.parent(); !parent.isRoot(); parent = parent.parent()) {
            FileAttributes attributes = namespace.get(parent);
            if (attributes != null && attributes.getFileType() != FileType.DIR) {
                msg.setFailed(CacheException.NOT_DIR, "Not a directory");
                return;
            }
        }
        FileAttributes attributes = namespace.get(path);
        if (attributes == null) {
            msg.setFailed(CacheException.FILE_NOT_FOUND, "No such file or directory");
        } else if (denied.contains(path)) {
            msg.setFailed(CacheException.PERMISSION_DENIED, "Permission denied");
        } else {
            msg.setFileAttributes(attributes);
        }
    }

    private DirectoryStream list(FsPath directory) throws CacheException {
        listed.add(directory);
        FileAttributes attributes = namespace.get(directory);
        if (attributes == null || attributes.getFileType() != FileType.DIR) {
            throw new FileNotFoundCacheException("No such directory: " + directory);
        }
        List<DirectoryEntry> entries = namespace.entrySet().stream()
              .filter(e -> !e.getKey().isRoot() && e.getKey().parent().equals(directory))
              .map(e -> new DirectoryEntry(e.getKey().name(), e.getValue()))
              .collect(Collectors.toList());
        return new DirectoryStream() {
            @Override
            public Iterator<DirectoryEntry> iterator() {
                return entries.iterator();
            }

            @Override
            public void close() {
            }
        };
    }
}

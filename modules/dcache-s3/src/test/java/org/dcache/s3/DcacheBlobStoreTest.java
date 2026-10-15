package org.dcache.s3;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.TimeoutCacheException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.list.DirectoryListSource;
import org.dcache.vehicles.FileAttributes;
import org.gaul.s3proxy.S3ErrorCode;
import org.gaul.s3proxy.S3ProxyException;
import org.gaul.s3proxy.blobstore.S3Exceptions;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectStorageClass;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.RestoreObjectRequest;
import software.amazon.awssdk.services.s3.model.RestoreRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.StorageClass;

public class DcacheBlobStoreTest {

    private static final FsPath ROOT = FsPath.create("/data/s3");
        private static final String MD5 = "9e107d9d372bb6826bd81d3542a419d6";

    /**
     * The MD5 checksum of "hello world".
     */
    private static final String HELLO_MD5 = "5eb63bbbe01eeed093cb22bb8f5acdc3";

    private final FakeDcache dcache = new FakeDcache();
    private final List<FsPath> listed = dcache.listed;
    private final List<Subject> requestSubjects = dcache.requestSubjects;
    private final Set<FsPath> denied = dcache.denied;

    private Subject subject;
    private PnfsHandler pnfs;
    private DirectoryListSource lister;
    private S3ObjectReader reader;
    private S3ObjectRestorer restorer;
    private DcacheBlobStore store;

    @Before
    public void setup() throws Exception {
        subject = new Subject();
        subject.getPrincipals().add(new UidPrincipal(1000));

        pnfs = dcache.pnfsHandler();
        lister = dcache.lister();
        restorer = mock(S3ObjectRestorer.class);
        reader = mock(S3ObjectReader.class, delegatesTo(dcache.reader()));

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
        assertThat(head.lastModified(), is(Instant.ofEpochMilli(FakeDcache.MTIME)));
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

        attributes.setModificationTime(FakeDcache.MTIME + 1000);

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
    public void shouldNotReportFileWithReplicaAndTapeCopyAsGlacier() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello").getStorageInfo()
              .addLocation(URI.create("osm://osm/?store=test&bfid=1"));

        assertThat(store.blobMetadata("bucket", "file").storageClass(), is(StorageClass.STANDARD));
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

        store.getBlob(get("file")
              .ifModifiedSince(Instant.ofEpochMilli(FakeDcache.MTIME - 1000)).build());
        S3Exception e = assertThrows(S3Exception.class, () -> store.getBlob(
              get("file").ifModifiedSince(Instant.ofEpochMilli(FakeDcache.MTIME)).build()));
        assertThat(e.statusCode(), is(304));
    }

    @Test
    public void shouldHonourIfUnmodifiedSince() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello");

        store.getBlob(get("file").ifUnmodifiedSince(Instant.ofEpochMilli(FakeDcache.MTIME)).build());
        S3Exception e = assertThrows(S3Exception.class, () -> store.getBlob(
              get("file").ifUnmodifiedSince(Instant.ofEpochMilli(FakeDcache.MTIME - 1000)).build()));
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
        assertThat(object.lastModified(), is(Instant.ofEpochMilli(FakeDcache.MTIME)));
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
    public void shouldPutObject() throws Exception {
        givenDirectory("/data/s3/bucket");

        String eTag = put("file", "hello world").eTag();

        assertThat(eTag, is('"' + HELLO_MD5 + '"'));
        assertThat(contentOf("/data/s3/bucket/file"), is("hello world"));
        assertThat(store.blobMetadata("bucket", "file").eTag(), is(eTag));
        assertThat(dcache.written.get(0).parent(), is(FsPath.create("/data/s3/bucket")));
        assertThat(dcache.written.get(0).name(), startsWith(".s3-tmp-"));
        assertThat(filesIn("/data/s3/bucket"), contains("file"));
    }

    @Test
    public void shouldPutEmptyObject() throws Exception {
        givenDirectory("/data/s3/bucket");

        put("empty", "");

        assertThat(contentOf("/data/s3/bucket/empty"), is(""));
        assertThat(dcache.written.size(), is(1));
    }

    @Test
    public void shouldStoreContentTypeAndUserMetadata() {
        givenDirectory("/data/s3/bucket");

        store.putBlob(PutObjectRequest.builder().bucket("bucket").key("file")
                    .contentLength(5L).contentType("text/plain").metadata(Map.of("color", "blue"))
                    .build(),
              new ByteArrayInputStream("hello".getBytes(UTF_8)));

        HeadObjectResponse head = store.blobMetadata("bucket", "file");
        assertThat(head.contentType(), is("text/plain"));
        assertThat(head.metadata(), is(equalTo(Map.of("color", "blue"))));
    }

    @Test
    public void shouldCreateDirectoriesOfKey() throws Exception {
        givenDirectory("/data/s3/bucket");

        put("a/b/file", "hello world");

        assertThat(contentOf("/data/s3/bucket/a/b/file"), is("hello world"));
    }

    @Test
    public void shouldReplaceObject() throws Exception {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "old");

        put("file", "hello world");

        assertThat(contentOf("/data/s3/bucket/file"), is("hello world"));
        assertThat(filesIn("/data/s3/bucket"), contains("file"));
    }

    @Test
    public void shouldAcceptMatchingContentMd5() throws Exception {
        givenDirectory("/data/s3/bucket");

        put(putRequest("file", "hello world").contentMD5(base64(HELLO_MD5)), "hello world");

        assertThat(contentOf("/data/s3/bucket/file"), is("hello world"));
    }

    @Test
    public void shouldRejectBadDigestAndKeepObject() throws Exception {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "old");

        S3ProxyException e = assertThrows(S3ProxyException.class,
              () -> put(putRequest("file", "hello").contentMD5(base64(HELLO_MD5)), "hello"));

        assertThat(e.getError(), is(S3ErrorCode.BAD_DIGEST));
        assertThat(contentOf("/data/s3/bucket/file"), is("old"));
        assertThat(filesIn("/data/s3/bucket"), contains("file"));
    }

    @Test
    public void shouldKeepObjectWhenWriteFails() throws Exception {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "old");
        dcache.writeError = new CacheException(CacheException.NO_POOL_ONLINE, "No pool online");

        S3Exception e = assertThrows(S3Exception.class, () -> put("file", "hello"));

        assertThat(e.statusCode(), is(503));
        assertThat(contentOf("/data/s3/bucket/file"), is("old"));
    }

    @Test
    public void shouldNotPutIntoMissingBucket() {
        assertThrows(NoSuchBucketException.class, () -> put("file", "hello"));
    }

    @Test
    public void shouldRefuseKeyBelowObject() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "x");

        S3Exception e = assertThrows(S3Exception.class, () -> put("file/below", "hello"));

        assertThat(e.awsErrorDetails().errorCode(), is("InvalidArgument"));
    }

    @Test
    public void shouldRefuseKeyOfDirectory() {
        givenDirectory("/data/s3/bucket/dir");
        givenFile("/data/s3/bucket/dir/file", "x");

        S3Exception e = assertThrows(S3Exception.class, () -> put("dir", "hello"));

        assertThat(e.awsErrorDetails().errorCode(), is("InvalidArgument"));
        assertThat(filesIn("/data/s3/bucket"), contains("dir"));
    }

    @Test
    public void shouldNotImplementPublicObjects() {
        givenDirectory("/data/s3/bucket");

        S3ProxyException e = assertThrows(S3ProxyException.class,
              () -> put(putRequest("file", "x").acl(ObjectCannedACL.PUBLIC_READ), "x"));

        assertThat(e.getError(), is(S3ErrorCode.NOT_IMPLEMENTED));
    }

    @Test
    public void shouldCopyObjectWithItsMetadata() throws Exception {
        givenDirectory("/data/s3/bucket");
        FileAttributes source = givenFile("/data/s3/bucket/file", "hello world");
        source.setXattrs(Map.of("s3.content-type", "text/plain", "s3.meta.color", "blue"));

        String eTag = store.copyBlob(copy("file", "copy").build()).copyObjectResult().eTag();

        assertThat(eTag, is('"' + HELLO_MD5 + '"'));
        assertThat(contentOf("/data/s3/bucket/copy"), is("hello world"));
        HeadObjectResponse head = store.blobMetadata("bucket", "copy");
        assertThat(head.contentType(), is("text/plain"));
        assertThat(head.metadata(), is(equalTo(Map.of("color", "blue"))));
    }

    @Test
    public void shouldCopyObjectReplacingMetadata() throws Exception {
        givenDirectory("/data/s3/bucket");
        FileAttributes source = givenFile("/data/s3/bucket/file", "hello world");
        source.setXattrs(Map.of("s3.content-type", "text/plain", "s3.meta.color", "blue"));

        store.copyBlob(copy("file", "file").metadataDirective(MetadataDirective.REPLACE)
              .contentType("text/html").metadata(Map.of("size", "big")).build());

        assertThat(contentOf("/data/s3/bucket/file"), is("hello world"));
        HeadObjectResponse head = store.blobMetadata("bucket", "file");
        assertThat(head.contentType(), is("text/html"));
        assertThat(head.metadata(), is(equalTo(Map.of("size", "big"))));
    }

    @Test
    public void shouldNotCopyTapeOnlyObject() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello world");
        givenOnTapeOnly("/data/s3/bucket/file");

        S3Exception e = assertThrows(S3Exception.class,
              () -> store.copyBlob(copy("file", "copy").build()));

        assertThat(e.awsErrorDetails().errorCode(), is("InvalidObjectState"));
    }

    @Test
    public void shouldHonourCopySourceIfMatch() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "hello world");

        S3Exception e = assertThrows(S3Exception.class,
              () -> store.copyBlob(copy("file", "copy").copySourceIfMatch("\"other\"").build()));

        assertThat(e.statusCode(), is(412));
        assertThat(dcache.written, is(emptyIterable()));
    }

    @Test
    public void shouldDeleteObjectAndKeepDirectories() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/dir/file", "x");

        store.removeBlob("bucket", "dir/file");

        assertThat(filesIn("/data/s3/bucket"), contains("dir"));
        assertThat(filesIn("/data/s3/bucket/dir"), is(emptyIterable()));
    }

    @Test
    public void shouldDeleteMissingObject() {
        givenDirectory("/data/s3/bucket");

        store.removeBlob("bucket", "missing/file");
    }

    @Test
    public void shouldNotDeleteDirectoryAsObject() {
        givenDirectory("/data/s3/bucket/dir");

        store.removeBlob("bucket", "dir");

        assertThat(filesIn("/data/s3/bucket"), contains("dir"));
    }

    @Test
    public void shouldNotDeleteInMissingBucket() {
        assertThrows(NoSuchBucketException.class, () -> store.removeBlob("bucket", "file"));
    }

    @Test
    public void shouldDenyDeleteWhenNamespaceDenies() {
        givenDirectory("/data/s3/bucket");
        givenFile("/data/s3/bucket/file", "x");
        denied.add(FsPath.create("/data/s3/bucket/file"));

        S3ProxyException e = assertThrows(S3ProxyException.class,
              () -> store.removeBlob("bucket", "file"));

        assertThat(e.getError(), is(S3ErrorCode.ACCESS_DENIED));
    }

    @Test
    public void shouldCreateBucket() {
        store.createContainer("bucket");

        assertThat(filesIn("/data/s3"), contains("bucket"));
    }

    @Test
    public void shouldRefuseToCreateExistingBucket() {
        givenDirectory("/data/s3/bucket");

        assertThrows(BucketAlreadyOwnedByYouException.class,
              () -> store.createContainer("bucket"));
    }

    @Test
    public void shouldReportExistingBucketWhereBucketsCannotBeCreated() {
        givenDirectory("/data/s3/bucket");
        denied.add(FsPath.create("/data/s3"));

        assertThrows(BucketAlreadyOwnedByYouException.class,
              () -> store.createContainer("bucket"));
    }

    @Test
    public void shouldDenyCreatingBucketWhereBucketsCannotBeCreated() {
        denied.add(FsPath.create("/data/s3"));

        S3ProxyException e = assertThrows(S3ProxyException.class,
              () -> store.createContainer("bucket"));

        assertThat(e.getError(), is(S3ErrorCode.ACCESS_DENIED));
    }

    @Test
    public void shouldNotImplementPublicBuckets() {
        S3ProxyException e = assertThrows(S3ProxyException.class,
              () -> store.createContainer(CreateBucketRequest.builder().bucket("bucket")
                    .acl(BucketCannedACL.PUBLIC_READ)
                    .build()));

        assertThat(e.getError(), is(S3ErrorCode.NOT_IMPLEMENTED));
    }

    @Test
    public void shouldDeleteBucketWithEmptyDirectories() {
        givenDirectory("/data/s3/bucket/a/b");
        givenDirectory("/data/s3/bucket/c");

        store.deleteBucket("bucket");

        assertThat(filesIn("/data/s3"), is(emptyIterable()));
    }

    @Test
    public void shouldNotDeleteBucketWithObjects() {
        givenDirectory("/data/s3/bucket/a");
        givenFile("/data/s3/bucket/c/file", "x");

        S3Exception e = assertThrows(S3Exception.class, () -> store.deleteBucket("bucket"));

        assertThat(e.awsErrorDetails().errorCode(), is("BucketNotEmpty"));
        assertThat(filesIn("/data/s3/bucket"), containsInAnyOrder("a", "c"));
    }

    @Test
    public void shouldNotDeleteBucketWithUploadInProgress() {
        givenFile("/data/s3/bucket/.s3-tmp-1234", "x");

        S3Exception e = assertThrows(S3Exception.class, () -> store.deleteBucket("bucket"));

        assertThat(e.awsErrorDetails().errorCode(), is("BucketNotEmpty"));
    }

    @Test
    public void shouldDeleteMissingBucketQuietly() {
        store.deleteBucket("bucket");
    }

    // Helpers

    private DcacheBlobStore newStore(int maxListEntries) {
        S3User user = new S3User("AKIAEXAMPLE", subject, Restrictions.none(), ROOT);
        return new DcacheBlobStore(() -> user, pnfs, lister, reader, dcache.writer(), restorer,
              maxListEntries);
    }

    private static PutObjectRequest.Builder putRequest(String key, String data) {
        return PutObjectRequest.builder().bucket("bucket").key(key)
              .contentLength((long) data.getBytes(UTF_8).length);
    }

    private PutObjectResponse put(String key, String data) {
        return put(putRequest(key, data), data);
    }

    private PutObjectResponse put(
          PutObjectRequest.Builder request, String data) {
        return store.putBlob(request.build(), new ByteArrayInputStream(data.getBytes(UTF_8)));
    }

    private static CopyObjectRequest.Builder copy(String sourceKey, String key) {
        return CopyObjectRequest.builder().sourceBucket("bucket").sourceKey(sourceKey)
              .destinationBucket("bucket").destinationKey(key);
    }

    private static String base64(String hex) {
        return Base64.getEncoder().encodeToString(HexFormat.of().parseHex(hex));
    }

    private String contentOf(String path) {
        byte[] data = dcache.content.get(FsPath.create(path));
        return data == null ? null : new String(data, UTF_8);
    }

    /**
     * Returns the names in a directory.
     */
    private List<String> filesIn(String path) {
        FsPath directory = FsPath.create(path);
        synchronized (dcache.namespace) {
            return dcache.namespace.keySet().stream()
                  .filter(p -> !p.isRoot() && p.parent().equals(directory))
                  .map(FsPath::name)
                  .sorted()
                  .toList();
        }
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
        dcache.directory(path);
    }

    private FileAttributes givenFile(String path, String data) {
        return dcache.file(path, data);
    }

    private void givenOnTapeOnly(String path) {
        dcache.onTapeOnly(path);
    }
}

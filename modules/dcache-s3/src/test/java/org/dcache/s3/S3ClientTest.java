package org.dcache.s3;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import diskCacheV111.util.FsPath;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.security.auth.Subject;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.attributes.Restrictions;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.gaul.s3proxy.auth.AuthenticationType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.ObjectStorageClass;
import software.amazon.awssdk.services.s3.model.RestoreObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.Tier;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

/**
 * Talks to the door with the AWS SDK for Java v2: the door's Jetty handler, s3proxy, the
 * locator and the dCache blob store are real; PnfsManager, gPlazma, the pools and PinManager are
 * faked.
 */
public class S3ClientTest {

    private static final String ACCESS_KEY = "AKIAEXAMPLE";
    private static final String SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    private static final String MD5 = "5eb63bbbe01eeed093cb22bb8f5acdc3";

    private final FakeDcache dcache = new FakeDcache();
    private final AtomicBoolean restoring = new AtomicBoolean();

    private LoginStrategy loginStrategy;
    private S3ObjectRestorer restorer;
    private Server jetty;
    private URI endpoint;
    private S3Client s3;

    @Before
    public void setup() throws Exception {
        Subject subject = new Subject();
        subject.getPrincipals().add(new UidPrincipal(1000));
        loginStrategy = mock(LoginStrategy.class);
        when(loginStrategy.login(any())).thenReturn(new LoginReply(subject, Set.of()));

        restorer = mock(S3ObjectRestorer.class);
        when(restorer.isRestoring(any(), any())).thenAnswer(i -> restoring.get());
        doAnswer(i -> {
            restoring.set(true);
            return null;
        }).when(restorer).restore(any(), any(), any(), anyInt());

        jetty = startDoor();
        endpoint = endpointOf(jetty);
        s3 = client(ACCESS_KEY, SECRET);

        dcache.directory("/data/s3");
        dcache.directory("/data/s3/bucket");
        dcache.directory("/data/s3/other");
        dcache.directory("/data/s3/bucket/dir");
        dcache.directory("/data/s3/bucket/dir/sub");
        dcache.file("/data/s3/bucket/hello.txt", "hello world");
        dcache.file("/data/s3/bucket/dir/a", "a");
        dcache.file("/data/s3/bucket/dir/b", "b");
        dcache.file("/data/s3/bucket/dir/sub/c", "c");
        dcache.file("/data/s3/bucket/tape", "on tape");
        dcache.onTapeOnly("/data/s3/bucket/tape");
    }

    @After
    public void tearDown() throws Exception {
        s3.close();
        jetty.stop();
    }

    /**
     * Starts a door on the fake dCache, as another door instance would run on the same dCache.
     */
    private Server startDoor() throws Exception {
        S3KeyStore keyStore = key -> key.equals(ACCESS_KEY)
              ? Optional.of(SECRET) : Optional.empty();
        var factory = new DcacheBlobStoreFactory(dcache.pnfsHandler(), dcache.lister(),
              dcache.reader(), dcache.writer(), restorer, 1000, 7, TimeUnit.DAYS);
        var locator = new DcacheBlobStoreLocator(keyStore, loginStrategy,
              FsPath.create("/data/s3"), factory);
        var handler = new DcacheS3ProxyHandler(locator, AuthenticationType.AWS_V2_OR_V4, "",
              900);

        Server server = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        server.setHandler(new S3JettyHandler(handler));
        server.start();
        return server;
    }

    private static URI endpointOf(Server server) {
        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
        return URI.create("http://127.0.0.1:" + port);
    }

    private S3Client client(String accessKey, String secret) {
        return client(endpoint, accessKey, secret);
    }

    private static S3Client client(URI endpoint, String accessKey, String secret) {
        return S3Client.builder()
              .endpointOverride(endpoint)
              .region(Region.US_EAST_1)
              .forcePathStyle(true)
              .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secret)))
              .httpClient(UrlConnectionHttpClient.create())
              .build();
    }

    @Test
    public void shouldListBuckets() {
        assertThat(s3.listBuckets().buckets().stream().map(Bucket::name).toList(),
              contains("bucket", "other"));
    }

    @Test
    public void shouldListWithDelimiter() {
        ListObjectsV2Response page = s3.listObjectsV2(b -> b.bucket("bucket").delimiter("/"));

        assertThat(page.contents().stream().map(S3Object::key).toList(),
              contains("hello.txt", "tape"));
        assertThat(page.commonPrefixes().stream().map(CommonPrefix::prefix).toList(),
              contains("dir/"));
    }

    @Test
    public void shouldPageThroughListing() {
        var keys = new ArrayList<String>();
        s3.listObjectsV2Paginator(b -> b.bucket("bucket").maxKeys(2))
              .contents().forEach(o -> keys.add(o.key()));

        assertThat(keys, contains("dir/a", "dir/b", "dir/sub/c", "hello.txt", "tape"));
    }

    @Test
    public void shouldListWithV1() {
        var keys = new ArrayList<String>();
        String marker = null;
        ListObjectsResponse page;
        do {
            String from = marker;
            page = s3.listObjects(b -> b.bucket("bucket").prefix("dir/").maxKeys(1).marker(from));
            page.contents().forEach(o -> keys.add(o.key()));
            marker = page.nextMarker();
        } while (page.isTruncated());

        assertThat(keys, contains("dir/a", "dir/b", "dir/sub/c"));
    }

    @Test
    public void shouldReportStorageClassInListing() {
        S3Object tape = s3.listObjectsV2(b -> b.bucket("bucket").prefix("tape")).contents()
              .get(0);

        assertThat(tape.storageClass(), is(ObjectStorageClass.GLACIER));
    }

    @Test
    public void shouldNotListMissingBucket() {
        assertThrows(NoSuchBucketException.class,
              () -> s3.listObjectsV2(b -> b.bucket("missing")));
    }

    @Test
    public void shouldHeadObject() {
        HeadObjectResponse head = s3.headObject(b -> b.bucket("bucket").key("hello.txt"));

        assertThat(head.contentLength(), is(11L));
        assertThat(head.eTag(), endsWith("-1\""));
        assertThat(head.storageClass(), is(StorageClass.STANDARD));
    }

    @Test
    public void shouldNotHeadMissingObject() {
        assertThrows(NoSuchKeyException.class,
              () -> s3.headObject(b -> b.bucket("bucket").key("missing")));
    }

    @Test
    public void shouldGetObject() {
        ResponseBytes<GetObjectResponse> object = s3.getObjectAsBytes(
              b -> b.bucket("bucket").key("hello.txt"));

        assertThat(object.asUtf8String(), is("hello world"));
    }

    @Test
    public void shouldGetRange() {
        ResponseBytes<GetObjectResponse> object = s3.getObjectAsBytes(
              b -> b.bucket("bucket").key("hello.txt").range("bytes=6-"));

        assertThat(object.asUtf8String(), is("world"));
        assertThat(object.response().contentRange(), is("bytes 6-10/11"));
    }

    @Test
    public void shouldAnswerNotModified() {
        String eTag = s3.headObject(b -> b.bucket("bucket").key("hello.txt")).eTag();

        S3Exception e = assertThrows(S3Exception.class, () -> s3.getObjectAsBytes(
              b -> b.bucket("bucket").key("hello.txt").ifNoneMatch(eTag)));
        assertThat(e.statusCode(), is(304));
    }

    @Test
    public void shouldRefuseToReadObjectOnTape() {
        S3Exception e = assertThrows(S3Exception.class,
              () -> s3.getObjectAsBytes(b -> b.bucket("bucket").key("tape")));

        assertThat(e.statusCode(), is(403));
        assertThat(e.awsErrorDetails().errorCode(), is("InvalidObjectState"));
    }

    @Test
    public void shouldRestoreObjectOnTape() throws Exception {
        RestoreObjectResponse restore = s3.restoreObject(b -> b.bucket("bucket").key("tape")
              .restoreRequest(r -> r.days(3).glacierJobParameters(j -> j.tier(Tier.STANDARD))));

        assertThat(restore.sdkHttpResponse().statusCode(), is(202));
        verify(restorer).restore(any(), eq(FsPath.create("/data/s3/bucket/tape")), any(), eq(3));
        assertThat(s3.headObject(b -> b.bucket("bucket").key("tape")).restore(),
              is("ongoing-request=\"true\""));

        S3Exception again = assertThrows(S3Exception.class,
              () -> s3.restoreObject(b -> b.bucket("bucket").key("tape")
                    .restoreRequest(r -> r.days(3))));
        assertThat(again.statusCode(), is(409));
        assertThat(again.awsErrorDetails().errorCode(), is("RestoreAlreadyInProgress"));
    }

    @Test
    public void shouldRestoreForOneDayWithoutRestoreRequest() throws Exception {
        RestoreObjectResponse restore = s3.restoreObject(b -> b.bucket("bucket").key("tape"));

        assertThat(restore.sdkHttpResponse().statusCode(), is(202));
        verify(restorer).restore(any(), any(), any(), eq(1));
    }

    @Test
    public void shouldAnswerRestoreOfObjectOnDiskWithOk() {
        RestoreObjectResponse restore = s3.restoreObject(b -> b.bucket("bucket").key("hello.txt")
              .restoreRequest(r -> r.days(1)));

        assertThat(restore.sdkHttpResponse().statusCode(), is(200));
    }

    @Test
    public void shouldRefuseWrongSecret() {
        try (S3Client wrong = client(ACCESS_KEY, "not-the-secret")) {
            S3Exception e = assertThrows(S3Exception.class, wrong::listBuckets);

            assertThat(e.statusCode(), is(403));
            assertThat(e.awsErrorDetails().errorCode(), is("SignatureDoesNotMatch"));
        }
    }

    @Test
    public void shouldRefuseUnknownAccessKey() {
        try (S3Client unknown = client("AKIAUNKNOWN", SECRET)) {
            S3Exception e = assertThrows(S3Exception.class, unknown::listBuckets);

            assertThat(e.statusCode(), is(403));
            assertThat(e.awsErrorDetails().errorCode(), is("InvalidAccessKeyId"));
        }
    }

    @Test
    public void shouldNotLoginForBadSignature() throws Exception {
        try (S3Client wrong = client(ACCESS_KEY, "not-the-secret")) {
            assertThrows(S3Exception.class, wrong::listBuckets);
        }

        verify(loginStrategy, never()).login(any());
    }

    @Test
    public void shouldServePresignedUrl() throws Exception {
        try (S3Presigner presigner = S3Presigner.builder()
              .endpointOverride(endpoint)
              .region(Region.US_EAST_1)
              .serviceConfiguration(S3Configuration.builder()
                    .pathStyleAccessEnabled(true).build())
              .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(ACCESS_KEY, SECRET)))
              .build()) {
            URI url = presigner.presignGetObject(b -> b.signatureDuration(Duration.ofMinutes(5))
                  .getObjectRequest(r -> r.bucket("bucket").key("hello.txt"))).url().toURI();

            HttpResponse<String> response = HttpClient.newHttpClient().send(
                  HttpRequest.newBuilder(url).GET().build(),
                  HttpResponse.BodyHandlers.ofString(UTF_8));

            assertThat(response.statusCode(), is(200));
            assertThat(response.body(), is("hello world"));
        }
    }

    // Writes

    @Test
    public void shouldPutAndGetObject() {
        String eTag = s3.putObject(b -> b.bucket("bucket").key("new/file").contentType("text/plain")
                    .metadata(Map.of("color", "blue")),
              RequestBody.fromString("hello world")).eTag();

        assertThat(eTag, is('"' + MD5 + '"'));
        ResponseBytes<GetObjectResponse> object = s3.getObjectAsBytes(
              b -> b.bucket("bucket").key("new/file"));
        assertThat(object.asUtf8String(), is("hello world"));
        assertThat(object.response().eTag(), is(eTag));
        assertThat(object.response().contentType(), is("text/plain"));
        assertThat(object.response().metadata(), is(Map.of("color", "blue")));
    }

    /**
     * Jetty 9 passes on such a Content-Type as "text/plain; charset=UTF-8", which broke the
     * signature, and getContentType() normalises it further.
     */
    @Test
    public void shouldPutObjectWithCharset() {
        s3.putObject(b -> b.bucket("bucket").key("text").contentType("text/plain; charset=utf-8"),
              RequestBody.fromString("hello world"));

        assertThat(s3.headObject(b -> b.bucket("bucket").key("text")).contentType(),
              is("text/plain;charset=utf-8"));
    }

    @Test
    public void shouldPutObjectWithUpperCaseCharset() {
        s3.putObject(b -> b.bucket("bucket").key("text").contentType("text/plain; charset=UTF-8"),
              RequestBody.fromString("hello world"));

        assertThat(s3.headObject(b -> b.bucket("bucket").key("text")).contentLength(), is(11L));
    }

    @Test
    public void shouldPutLargeObject() {
        byte[] data = new byte[3 * 1024 * 1024 + 7];
        new Random(1).nextBytes(data);

        s3.putObject(b -> b.bucket("bucket").key("large"), RequestBody.fromBytes(data));

        assertThat(s3.getObjectAsBytes(b -> b.bucket("bucket").key("large")).asByteArray(),
              is(data));
    }

    @Test
    public void shouldReplaceObject() {
        s3.putObject(b -> b.bucket("bucket").key("hello.txt"), RequestBody.fromString("bye"));

        assertThat(s3.getObjectAsBytes(b -> b.bucket("bucket").key("hello.txt"))
              .asUtf8String(), is("bye"));
        assertThat(s3.listObjectsV2(b -> b.bucket("bucket").delimiter("/")).contents().stream()
              .map(S3Object::key).toList(), contains("hello.txt", "tape"));
    }

    @Test
    public void shouldRejectBadContentMd5() {
        S3Exception e = assertThrows(S3Exception.class, () -> s3.putObject(
              b -> b.bucket("bucket").key("hello.txt").contentMD5("XrY7u+Ae7tCTyyK7j1rNww=="),
              RequestBody.fromString("bye")));

        assertThat(e.statusCode(), is(400));
        assertThat(e.awsErrorDetails().errorCode(), is("BadDigest"));
        assertThat(s3.getObjectAsBytes(b -> b.bucket("bucket").key("hello.txt"))
              .asUtf8String(), is("hello world"));
    }

    @Test
    public void shouldNotImplementConditionalPut() {
        S3Exception e = assertThrows(S3Exception.class, () -> s3.putObject(
              b -> b.bucket("bucket").key("new").ifNoneMatch("*"),
              RequestBody.fromString("data")));

        assertThat(e.statusCode(), is(501));
    }

    @Test
    public void shouldCopyObject() {
        s3.copyObject(b -> b.sourceBucket("bucket").sourceKey("hello.txt")
              .destinationBucket("other").destinationKey("copy"));

        assertThat(s3.getObjectAsBytes(b -> b.bucket("other").key("copy")).asUtf8String(),
              is("hello world"));
    }

    @Test
    public void shouldDeleteObject() {
        s3.deleteObject(b -> b.bucket("bucket").key("hello.txt"));

        assertThrows(NoSuchKeyException.class,
              () -> s3.headObject(b -> b.bucket("bucket").key("hello.txt")));
    }

    @Test
    public void shouldDeleteObjects() {
        DeleteObjectsResponse response = s3.deleteObjects(b -> b.bucket("bucket")
              .delete(d -> d.objects(ObjectIdentifier.builder().key("dir/a").build(),
                    ObjectIdentifier.builder().key("dir/b").build(),
                    ObjectIdentifier.builder().key("missing").build())));

        assertThat(response.deleted().stream().map(DeletedObject::key).toList(),
              containsInAnyOrder("dir/a", "dir/b", "missing"));
        assertThat(response.errors(), is(empty()));
        assertThat(s3.listObjectsV2(b -> b.bucket("bucket").prefix("dir/")).contents().stream()
              .map(S3Object::key).toList(), contains("dir/sub/c"));
    }

    @Test
    public void shouldCreateAndDeleteBucket() {
        s3.createBucket(b -> b.bucket("new-bucket"));
        s3.putObject(b -> b.bucket("new-bucket").key("a/b"), RequestBody.fromString("x"));

        S3Exception e = assertThrows(S3Exception.class,
              () -> s3.deleteBucket(b -> b.bucket("new-bucket")));
        assertThat(e.statusCode(), is(409));
        assertThat(e.awsErrorDetails().errorCode(), is("BucketNotEmpty"));

        s3.deleteObject(b -> b.bucket("new-bucket").key("a/b"));
        s3.deleteBucket(b -> b.bucket("new-bucket"));

        assertThat(s3.listBuckets().buckets().stream().map(Bucket::name).toList(),
              contains("bucket", "other"));
    }

    // Multipart uploads

    @Test
    public void shouldUploadInParts() {
        byte[] first = new byte[5 * 1024 * 1024];
        new Random(1).nextBytes(first);
        byte[] second = "the end".getBytes(UTF_8);
        String id = s3.createMultipartUpload(b -> b.bucket("bucket").key("mpu/file")
              .contentType("text/plain")).uploadId();

        String eTag1 = s3.uploadPart(b -> b.bucket("bucket").key("mpu/file").uploadId(id)
              .partNumber(1), RequestBody.fromBytes(first)).eTag();
        String eTag2 = s3.uploadPart(b -> b.bucket("bucket").key("mpu/file").uploadId(id)
              .partNumber(2), RequestBody.fromBytes(second)).eTag();

        assertThat(s3.listMultipartUploads(b -> b.bucket("bucket")).uploads().stream()
              .map(u -> u.key() + " " + u.uploadId()).toList(), contains("mpu/file " + id));
        assertThat(s3.listParts(b -> b.bucket("bucket").key("mpu/file").uploadId(id)).parts()
              .stream().map(p -> p.partNumber() + " " + p.size() + " " + p.eTag()).toList(),
              contains("1 " + first.length + " " + eTag1, "2 " + second.length + " " + eTag2));

        s3.completeMultipartUpload(b -> b.bucket("bucket").key("mpu/file").uploadId(id)
              .multipartUpload(m -> m.parts(
                    CompletedPart.builder().partNumber(1).eTag(eTag1).build(),
                    CompletedPart.builder().partNumber(2).eTag(eTag2).build())));

        ResponseBytes<GetObjectResponse> object = s3.getObjectAsBytes(
              b -> b.bucket("bucket").key("mpu/file"));
        byte[] expected = new byte[first.length + second.length];
        System.arraycopy(first, 0, expected, 0, first.length);
        System.arraycopy(second, 0, expected, first.length, second.length);
        assertThat(object.asByteArray(), is(expected));
        assertThat(object.response().contentType(), is("text/plain"));
        assertThat(s3.listMultipartUploads(b -> b.bucket("bucket")).uploads(), is(empty()));
    }

    @Test
    public void shouldRefuseSmallPartsBeforeTheLast() {
        String id = s3.createMultipartUpload(b -> b.bucket("bucket").key("mpu")).uploadId();
        String eTag1 = s3.uploadPart(b -> b.bucket("bucket").key("mpu").uploadId(id)
              .partNumber(1), RequestBody.fromString("small")).eTag();
        String eTag2 = s3.uploadPart(b -> b.bucket("bucket").key("mpu").uploadId(id)
              .partNumber(2), RequestBody.fromString("last")).eTag();

        S3Exception e = assertThrows(S3Exception.class, () -> s3.completeMultipartUpload(
              b -> b.bucket("bucket").key("mpu").uploadId(id).multipartUpload(m -> m.parts(
                    CompletedPart.builder().partNumber(1).eTag(eTag1).build(),
                    CompletedPart.builder().partNumber(2).eTag(eTag2).build()))));

        assertThat(e.awsErrorDetails().errorCode(), is("EntityTooSmall"));
    }

    @Test
    public void shouldCopyPart() {
        String id = s3.createMultipartUpload(b -> b.bucket("bucket").key("mpu")).uploadId();
        String eTag = s3.uploadPartCopy(b -> b.sourceBucket("bucket").sourceKey("hello.txt")
              .destinationBucket("bucket").destinationKey("mpu").uploadId(id).partNumber(1)
              .copySourceRange("bytes=6-10")).copyPartResult().eTag();

        s3.completeMultipartUpload(b -> b.bucket("bucket").key("mpu").uploadId(id)
              .multipartUpload(m -> m.parts(
                    CompletedPart.builder().partNumber(1).eTag(eTag).build())));

        assertThat(s3.getObjectAsBytes(b -> b.bucket("bucket").key("mpu")).asUtf8String(),
              is("world"));
    }

    @Test
    public void shouldAbortUpload() {
        String id = s3.createMultipartUpload(b -> b.bucket("bucket").key("mpu")).uploadId();
        s3.uploadPart(b -> b.bucket("bucket").key("mpu").uploadId(id).partNumber(1),
              RequestBody.fromString("data"));

        s3.abortMultipartUpload(b -> b.bucket("bucket").key("mpu").uploadId(id));

        assertThat(s3.listMultipartUploads(b -> b.bucket("bucket")).uploads(), is(empty()));
        assertThrows(NoSuchUploadException.class, () -> s3.uploadPart(
              b -> b.bucket("bucket").key("mpu").uploadId(id).partNumber(1),
              RequestBody.fromString("data")));
    }

    @Test
    public void shouldUploadPartsThroughTwoDoors() throws Exception {
        Server other = startDoor();
        try (S3Client s3b = client(endpointOf(other), ACCESS_KEY, SECRET)) {
            byte[] first = new byte[5 * 1024 * 1024 + 3];
            new Random(2).nextBytes(first);
            byte[] second = new byte[1024 * 1024 + 11];
            new Random(3).nextBytes(second);
            String id = s3.createMultipartUpload(b -> b.bucket("bucket").key("two-doors"))
                  .uploadId();

            String eTag1 = s3.uploadPart(b -> b.bucket("bucket").key("two-doors").uploadId(id)
                  .partNumber(1), RequestBody.fromBytes(first)).eTag();
            String eTag2 = s3b.uploadPart(b -> b.bucket("bucket").key("two-doors").uploadId(id)
                  .partNumber(2), RequestBody.fromBytes(second)).eTag();

            assertThat(s3b.listParts(b -> b.bucket("bucket").key("two-doors").uploadId(id))
                  .parts().size(), is(2));
            s3b.completeMultipartUpload(b -> b.bucket("bucket").key("two-doors").uploadId(id)
                  .multipartUpload(m -> m.parts(
                        CompletedPart.builder().partNumber(1).eTag(eTag1).build(),
                        CompletedPart.builder().partNumber(2).eTag(eTag2).build())));

            byte[] expected = new byte[first.length + second.length];
            System.arraycopy(first, 0, expected, 0, first.length);
            System.arraycopy(second, 0, expected, first.length, second.length);
            assertThat(s3.getObjectAsBytes(b -> b.bucket("bucket").key("two-doors"))
                  .asByteArray(), is(expected));
        } finally {
            other.stop();
        }
    }

    // Versions

    @Test
    public void shouldListObjectsAsNullVersions() {
        var page = s3.listObjectVersions(b -> b.bucket("bucket").prefix("dir/").maxKeys(2));

        assertThat(page.versions().stream().map(v -> v.key() + " " + v.versionId() + " "
              + v.isLatest()).toList(), contains("dir/a null true", "dir/b null true"));
        assertThat(page.isTruncated(), is(true));
        assertThat(page.nextKeyMarker(), is("dir/b"));
        var next = s3.listObjectVersions(b -> b.bucket("bucket").prefix("dir/")
              .keyMarker(page.nextKeyMarker()).versionIdMarker(page.nextVersionIdMarker()));
        assertThat(next.versions().stream().map(v -> v.key()).toList(), contains("dir/sub/c"));
    }

    @Test
    public void shouldAcceptNullVersion() {
        assertThat(s3.getObjectAsBytes(b -> b.bucket("bucket").key("hello.txt").versionId("null"))
              .asUtf8String(), is("hello world"));

        var response = s3.deleteObjects(b -> b.bucket("bucket").delete(d -> d.objects(
              ObjectIdentifier.builder().key("dir/a").versionId("null").build())));

        assertThat(response.errors(), is(empty()));
        assertThrows(NoSuchKeyException.class,
              () -> s3.headObject(b -> b.bucket("bucket").key("dir/a")));
    }

    @Test
    public void shouldNotImplementOtherVersions() {
        S3Exception e = assertThrows(S3Exception.class, () -> s3.getObjectAsBytes(
              b -> b.bucket("bucket").key("hello.txt").versionId("3HL4kqtJlcpXroDTDmJ")));

        assertThat(e.statusCode(), is(501));
    }
}

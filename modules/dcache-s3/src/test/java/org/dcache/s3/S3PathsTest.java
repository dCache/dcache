package org.dcache.s3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import diskCacheV111.util.FsPath;
import java.util.Optional;
import org.gaul.s3proxy.blobstore.S3Exceptions;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3PathsTest {

    private static final FsPath ROOT = FsPath.create("/data/s3");
    private static final FsPath BUCKET = ROOT.child("bucket");

    @Test
    public void shouldMapBucketToDirectoryBelowRoot() {
        assertThat(S3Paths.bucket(ROOT, "bucket"), is(equalTo(BUCKET)));
    }

    @Test
    public void shouldRejectHiddenBucket() {
        assertThrows(NoSuchBucketException.class, () -> S3Paths.bucket(ROOT, ".hidden"));
    }

    @Test
    public void shouldRejectEmptyBucket() {
        assertThrows(NoSuchBucketException.class, () -> S3Paths.bucket(ROOT, ""));
    }

    @Test
    public void shouldRejectBucketWithSlash() {
        assertThrows(NoSuchBucketException.class, () -> S3Paths.bucket(ROOT, "a/b"));
    }

    @Test
    public void shouldMapKeyToPathBelowBucket() {
        assertThat(S3Paths.object(BUCKET, "a/b/c.txt"),
              is(equalTo(FsPath.create("/data/s3/bucket/a/b/c.txt"))));
    }

    @Test
    public void shouldRejectKeysWithoutPath() {
        for (String key : new String[]{"", "/a", "a//b", "a/", "a/./b", "a/../b", "..",
              ".s3-tmp-1234", "a/.s3-multipart/b"}) {
            S3Exception e = assertThrows(key, S3Exception.class, () -> S3Paths.object(BUCKET, key));
            assertThat(key, S3Exceptions.errorCode(e), is("InvalidArgument"));
        }
    }

    @Test
    public void shouldAcceptDotsInsideNames() {
        assertThat(S3Paths.object(BUCKET, "a/.b/c..d"),
              is(equalTo(FsPath.create("/data/s3/bucket/a/.b/c..d"))));
    }

    @Test
    public void shouldMapEmptyDirectoryKeyToBucket() {
        assertThat(S3Paths.directory(BUCKET, ""), is(equalTo(Optional.of(BUCKET))));
    }

    @Test
    public void shouldMapDirectoryKey() {
        assertThat(S3Paths.directory(BUCKET, "a/b/"),
              is(equalTo(Optional.of(FsPath.create("/data/s3/bucket/a/b")))));
    }

    @Test
    public void shouldNotMapDirectoryKeyWithoutPath() {
        assertThat(S3Paths.directory(BUCKET, "a//"), is(equalTo(Optional.empty())));
        assertThat(S3Paths.directory(BUCKET, "../"), is(equalTo(Optional.empty())));
        assertThat(S3Paths.directory(BUCKET, ".s3-multipart/"), is(equalTo(Optional.empty())));
    }

    @Test
    public void shouldReserveDoorNames() {
        assertThat(S3Paths.isReserved(".s3-tmp-abc"), is(true));
        assertThat(S3Paths.isReserved(".s3-multipart"), is(true));
        assertThat(S3Paths.isReserved(".s3-other"), is(false));
    }
}

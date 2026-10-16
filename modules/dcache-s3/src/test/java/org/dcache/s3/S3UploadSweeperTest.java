package org.dcache.s3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restrictions;
import org.junit.Before;
import org.junit.Test;

public class S3UploadSweeperTest {

    private static final long OLD = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(8);

    private final FakeDcache dcache = new FakeDcache();

    private S3MultipartUploads uploads;
    private S3UploadSweeper sweeper;

    @Before
    public void setup() throws Exception {
        uploads = new S3MultipartUploads(dcache.pnfsHandler(), dcache.lister(), new Subject(),
              Restrictions.none());

        sweeper = new S3UploadSweeper();
        sweeper.setPnfsHandler(dcache.pnfsHandler());
        sweeper.setLister(dcache.lister());
        sweeper.setRoot("/data/s3");
        sweeper.setMaxAge(7);
        sweeper.setMaxAgeUnit(TimeUnit.DAYS);

        dcache.directory("/data/s3/bucket");
        dcache.directory("/data/s3/other");
        dcache.directory("/data/s3/home/alice/deep");
    }

    @Test
    public void shouldRemoveOldUploadsOfBucketsBelowRoot() throws Exception {
        var old = givenUpload("/data/s3/bucket", "old", OLD);
        dcache.file(old.directory().child("1").toString(), "part");
        var recent = givenUpload("/data/s3/bucket", "recent", System.currentTimeMillis());
        var otherOld = givenUpload("/data/s3/other", "old", OLD);

        assertThat(sweeper.sweepAll(), is(2));

        assertThat(idsIn("/data/s3/bucket"), is(List.of(recent.id())));
        assertThat(idsIn("/data/s3/other"), is(List.of()));
        assertThat(dcache.namespace.containsKey(old.directory()), is(false));
        assertThat(dcache.namespace.containsKey(otherOld.directory()), is(false));
    }

    @Test
    public void shouldNotSweepBucketsFurtherDown() throws Exception {
        var deep = givenUpload("/data/s3/home/alice/deep", "old", OLD);

        assertThat(sweeper.sweepAll(), is(0));

        assertThat(idsIn("/data/s3/home/alice/deep"), is(List.of(deep.id())));
    }

    @Test
    public void shouldSweepNamedBucket() throws Exception {
        givenUpload("/data/s3/home/alice/deep", "old", OLD);

        var command = sweeper.new SweepCommand();
        command.bucket = "/data/s3/home/alice/deep";

        assertThat(command.execute(), is("Removed 1 uploads"));
        assertThat(idsIn("/data/s3/home/alice/deep"), is(List.of()));
    }

    @Test
    public void shouldListUploads() throws Exception {
        var upload = givenUpload("/data/s3/bucket", "dir/file", OLD);
        dcache.file(upload.directory().child("1").toString(), "hello");
        dcache.file(upload.directory().child("2").toString(), "world!");

        var command = sweeper.new ListCommand();
        command.bucket = "/data/s3/bucket";
        String output = command.execute();

        assertThat(output, containsString(upload.id()));
        assertThat(output, containsString("    2              11 dir/file"));
    }

    @Test
    public void shouldRemoveUpload() throws Exception {
        var upload = givenUpload("/data/s3/bucket", "file", System.currentTimeMillis());
        dcache.file(upload.directory().child("1").toString(), "part");

        var command = sweeper.new RemoveCommand();
        command.bucket = "/data/s3/bucket";
        command.id = upload.id();
        command.execute();

        assertThat(idsIn("/data/s3/bucket"), is(List.of()));
        assertThat(dcache.content.containsKey(upload.directory().child("1")), is(false));
    }

    @Test
    public void shouldRefuseToRemoveUnknownUpload() {
        var command = sweeper.new RemoveCommand();
        command.bucket = "/data/s3/bucket";
        command.id = "0b6c9a52-2a64-4dc8-9c39-7b4a5d0d3a11";

        assertThrows(FileNotFoundCacheException.class, command::execute);
    }

    private S3MultipartUploads.Upload givenUpload(String bucket, String key, long created)
          throws Exception {
        var upload = uploads.create(FsPath.create(bucket), key,
              Map.of("s3.content-type", "text/plain"));
        dcache.namespace.get(upload.directory()).setCreationTime(created);
        return upload;
    }

    private List<String> idsIn(String bucket) throws Exception {
        return uploads.list(FsPath.create(bucket)).stream()
              .map(S3MultipartUploads.Upload::id).toList();
    }
}

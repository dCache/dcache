package org.dcache.s3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class FileS3KeyStoreTest {

    private Path keyFile;
    private FileS3KeyStore keyStore;

    @Before
    public void setup() throws Exception {
        FileSystem filesystem = Jimfs.newFileSystem(Configuration.unix());
        keyFile = filesystem.getPath("/etc/dcache/s3-keys.conf");
        Files.createDirectories(keyFile.getParent());
        keyStore = new FileS3KeyStore(keyFile);
    }

    @Test
    public void shouldReturnEmptyWhenFileDoesNotExist() {
        assertFalse(keyStore.secretFor("AKIAEXAMPLE").isPresent());
    }

    @Test
    public void shouldReturnSecretForKnownKey() throws Exception {
        givenKeyFile("AKIAEXAMPLE secret");

        assertThat(keyStore.secretFor("AKIAEXAMPLE"), is(equalTo(Optional.of("secret"))));
    }

    @Test
    public void shouldReturnEmptyForUnknownKey() throws Exception {
        givenKeyFile("AKIAEXAMPLE secret");

        assertFalse(keyStore.secretFor("AKIAOTHER").isPresent());
    }

    @Test
    public void shouldRejectAllKeysWhenFileIsInvalid() throws Exception {
        givenKeyFile("AKIAEXAMPLE secret", "AKIABROKEN");

        assertFalse(keyStore.secretFor("AKIAEXAMPLE").isPresent());
    }

    @Test
    public void shouldReloadModifiedFile() throws Exception {
        givenKeyFile("AKIAEXAMPLE old-secret");
        assertThat(keyStore.secretFor("AKIAEXAMPLE"), is(equalTo(Optional.of("old-secret"))));

        givenKeyFile("AKIAEXAMPLE new-secret");
        Files.setLastModifiedTime(keyFile, FileTime.from(Instant.now().plusSeconds(10)));
        Thread.sleep(1100); // ParsableFile checks the file at most once a second

        assertThat(keyStore.secretFor("AKIAEXAMPLE"), is(equalTo(Optional.of("new-secret"))));
    }

    private void givenKeyFile(String... lines) throws Exception {
        Files.write(keyFile, List.of(lines));
    }
}

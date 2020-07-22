package diskCacheV111.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;

/**
 * Some tests for the {@link Pgpass} class
 *
 * @author Tom Dittrich
 * @version 0.2
 */
public class PgpassTest {

    File tempFile;
    Pgpass pgpass;

    @Before
    public void setUp() throws IOException {
        tempFile = File.createTempFile("pgpass", ".testfile");
        Files.writeString(tempFile.toPath(), "dbhost:5432:foo:dbuser:dbpass\nlocalhost:*:foo:dbuser:dbpass2",
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);

        Files.setPosixFilePermissions(tempFile.toPath(), Set.of(PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ));
        pgpass = new Pgpass(tempFile.getCanonicalPath());
    }

    @After
    public void tearDown() {
        tempFile.delete();
    }

    @Test
    public void whenFileNotExistThenReturnNull() {
        Pgpass pgpassWithFalsePath = new Pgpass("wrong_file_path.txt");
        assertNull(pgpassWithFalsePath.getPgpass("dummy", "dummy", "dummy", "dummy"));
    }

    @Test
    public void whenFalsePermissionsThenReturnNull() throws IOException {
        Set<PosixFilePermission> referencePermissions = PosixFilePermissions.fromString("rwx------");
        Files.setPosixFilePermissions(tempFile.toPath(), referencePermissions);

        assertNull(pgpass.getPgpass("dummy", "dummy", "dummy", "dummy"));
    }


    @Test
    public void testReadingExistingRecord() {
        String pass = pgpass.getPgpass("dbhost", "5432", "foo", "dbuser");
        assertEquals("dbpass", pass);
    }

    @Test
    public void testDbUrlWithoutParams() throws Exception {
        String pass = Pgpass.getPassword(tempFile.getCanonicalPath(),
                "jdbc:postgresql://dbhost/foo", "dbuser",
                "");
        assertEquals("dbpass", pass);
    }

    @Test
    public void testDbUrlWithParams() throws Exception {
        String pass = Pgpass.getPassword(tempFile.getCanonicalPath(),
                "jdbc:postgresql://dbhost/foo?prepareThreshold=3&targetServerType=master", "dbuser",
                "");
        assertEquals("dbpass", pass);
    }

    @Test
    public void testDbUrlWithPortAndParams() throws Exception {
        String pass = Pgpass.getPassword(tempFile.getCanonicalPath(),
                "jdbc:postgresql://dbhost:5432/foo?prepareThreshold=3&targetServerType=master", "dbuser",
                "");
        assertEquals("dbpass", pass);
    }

    @Test
    public void testDbUrlWithoutHost() throws Exception {
        String pass = Pgpass.getPassword(tempFile.getCanonicalPath(),
                "jdbc:postgresql:foo", "dbuser",
                "");
        assertEquals("dbpass2", pass);
    }
}
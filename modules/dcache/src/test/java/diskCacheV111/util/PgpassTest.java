package diskCacheV111.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static org.junit.Assert.assertNull;

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
}
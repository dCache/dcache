package diskCacheV111.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static org.junit.Assert.assertNull;

/**
 * Some tests for the {@link Pgpass} class
 *
 * @author Tom Dittrich
 * @version 0.1
 */
public class PgpassTest {

    final String FILEPATH = "src/test/resources/pgpassTestFile.txt";
    Path path;
    Pgpass pgpass;

    @Before
    public void setUp() throws IOException {
        path = Paths.get(FILEPATH);
        Files.createFile(path);

        pgpass = new Pgpass(FILEPATH);
    }

    @After
    public void tearDown() throws IOException {
        Files.delete(path);
    }

    @Test
    public void whenFileNotExistThenReturnNull() {
        Pgpass pgpassWithFalsePath = new Pgpass("wrong_file_path.txt");
        assertNull(pgpassWithFalsePath.getPgpass("dummy", "dummy", "dummy", "dummy"));
    }

    @Test
    public void whenFalsePermissionsThenReturnNull() throws IOException {
        Set<PosixFilePermission> referencePermissions = PosixFilePermissions.fromString("rwx------");
        Files.setPosixFilePermissions(path, referencePermissions);

        assertNull(pgpass.getPgpass("dummy", "dummy", "dummy", "dummy"));
    }
}
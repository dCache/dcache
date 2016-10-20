package diskCacheV111.util;

import org.junit.After;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.PatternSyntaxException;

import org.dcache.auth.Subjects;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkState;
import static org.hamcrest.Matchers.is;

public class CheckStagePermissionTests
{
    private File _file;
    private CheckStagePermission _check;
    private boolean _allowed;

    @Before
    public void setUp() throws IOException {
        _file = null;
        _check = null;
    }

    @After
    public void tearDown() {
        if (_file != null) {
            _file.delete();
        }
    }

    /*
     *  Tests that check behaviour when stage protection is disable.
     */

    @Test
    public void shouldAllowUserWithDnAndFqanToStageIfEmptyConstructed() throws Exception
    {
        givenCheckConstructedWith("");

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test user").fqan("/atlas/Role=production").fqan("/atlas"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldAllowUserWithDnToStageIfEmptyConstructed() throws Exception
    {
        givenCheckConstructedWith("");

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test user"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldAllowUserWithDnAndFqanToStageIfNullConstructed() throws Exception
    {
        givenCheckConstructedWith(null);

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test user").fqan("/atlas/Role=production").fqan("/atlas"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldAllowUserWithDnToStageIfNullConstructed() throws Exception
    {
        givenCheckConstructedWith(null);

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test user"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(true));
    }

    /*
     *  Tests that check behaviour when stage protection file is empty.
     */

    @Test()
    public void shouldDenyUserWithDnAndFqanToStageIfEmpty() throws Exception
    {
        given(file().isEmpty());

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test user").fqan("/atlas/Role=production").fqan("/atlas"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(false));
    }

    /*
     *  Tests that check behaviour when stage protection file is missing.
     */

    @Test()
    public void shouldDenyUserWithDnAndFqanToStageIfMissing() throws Exception
    {
        given(file().isMissing());

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test user").fqan("/atlas/Role=production").fqan("/atlas"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(false));
    }


    /*
     *  Tests that check behaviour when a DN is authorised to stage.
     */

    @Test
    public void shouldAllowUserWithDnToStageIfDnAuthz() throws Exception
    {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\""));

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test user"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldAllowUserWithDnAndFqanToStageIfDnAuthz() throws Exception
    {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\""));

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test user").fqan("/atlas/Role=production").fqan("/atlas"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldDenyUserWithWrongDnToStageIfDnAuthz() throws Exception
    {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\""));

        whenCheck(Subjects.of().dn("/DC=org/DC=otherExample/CN=test user"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(false));
    }

    /*
     *  Tests that check behaviour when a (DN,FQAN) pair is authorised to stage.
     */

    @Test
    public void shouldDenyUserWithDnToStageIfDnFqanAuthz() throws Exception
    {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\""));

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test user"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(false));
    }

    @Test
    public void shouldDenyUserWithDnAndWrongFqanToStageIfDnFqanAuthz() throws Exception
    {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\""));

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test user").fqan("/atlas"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(false));
    }

    @Test
    public void shouldAllowUserWithDnAndFqanToStageIfDnFqanAuthz() throws Exception
    {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\""));

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test user").fqan("/atlas/Role=production").fqan("/atlas"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldAllowUserWithDnAndNonprimaryFqanToStageIfDnFqanAuthz() throws Exception
    {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\" \"/atlas\""));

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test user").fqan("/atlas/Role=production").fqan("/atlas"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldDenyUserWithWrongDnToStageIfDnFqanAuthz() throws Exception
    {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\""));

        whenCheck(Subjects.of().dn("/DC=org/DC=anotherExample/CN=test user"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(false));
    }


    /*
     * Tests to check that updating a file is honoured.
     */

    @Test
    public void shouldAllowUserWithDnToStageIfDnAuthzAfterReload() throws Exception
    {
        given(file().isEmpty());
        givenCheckedWith(Subjects.of().dn("/DC=org/DC=example/CN=test user"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));
        // Note: filesystems have differing granularity of their timestamp: so
        //       we must make sure the mtime has increased.
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\"").withDifferentMtime());

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test user"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(true));
    }

    /*
     *  Tests that check behaviour when a pattern-based (DN,FQAN) pair is
     *  authorised to stage a specific storage class.
     */

    @Test
    public void shouldAllowUserWithDnToStageIfWildDnWildFqanAuthz() throws Exception
    {
        given(file().hasContents("\"/DC=org/DC=example/.*\" \"/atlas/Role=.*\""));

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test").fqan("/atlas/Role=production").fqan("/atlas"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldDenyUserWithWrongDnToStageIfWildDnWildFqanAuthz() throws Exception
    {
        given(file().hasContents("\"/DC=org/DC=example/.*\" \"/atlas/Role=.*\""));

        whenCheck(Subjects.of().dn("/DC=org/DC=anotherExample/CN=test").fqan("/atlas/Role=production").fqan("/atlas"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(false));
    }

    @Test
    public void shouldAllowUserWithDnFqanToStageIfAnyDnAnyFqanAnyStoreAuthz() throws Exception
    {
        given(file().hasContents("\".*\" \".*\" \".*\""));

        whenCheck(Subjects.of().dn("/DC=org/DC=anotherExample/CN=test user").fqan("/atlas/Role=production").fqan("/atlas"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(true));
    }

    /*
     *  Tests that check behaviour when a (DN,FQAN) pair is authorised to stage
     *  a specific storage class.
     */

    @Test
    public void shouldAllowUserWithDnFqanToStageStoreIfDnFqanStoreAuthz() throws Exception
    {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\" \"sql:chimera@osm\""));

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test user").fqan("/atlas/Role=production").fqan("/atlas"),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"));

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldDenyUserWithDnFqanToStageWrongStoreIfDnFqanStoreAuthz() throws Exception
    {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\" \"sql:chimera@osm\""));

        whenCheck(Subjects.of().dn("/DC=org/DC=example/CN=test user").fqan("/atlas/Role=production").fqan("/atlas"),
                FileAttributes.of().storageClass("data:chimera").hsm("osm"));

        assertThat(_allowed, is(false));
    }

    private void whenCheck(Subjects.Builder subjectBuilder,
            FileAttributes.Builder attributeBuilder) throws PatternSyntaxException, IOException
    {
        _allowed = getCheck().canPerformStaging(subjectBuilder.build(), attributeBuilder.build());
    }

    private CheckStagePermission getCheck() throws IOException
    {
        if (_check == null) {
            _check = new CheckStagePermission(_file.getCanonicalPath());
        }
        return _check;
    }

    private void givenCheckConstructedWith(String argument)
    {
        _check = new CheckStagePermission(argument);
    }

    private void givenCheckedWith(Subjects.Builder subjectBuilder,
            FileAttributes.Builder attributeBuilder) throws PatternSyntaxException, IOException
    {
        getCheck().canPerformStaging(subjectBuilder.build(), attributeBuilder.build());
    }

    private FileStateAssertion file() throws IOException
    {
        if (_file == null) {
            _file = File.createTempFile("stagePermissionFile", null, null);
            _file.deleteOnExit();
        }
        return new FileStateAssertion(_file);
    }

    private void given(FileStateAssertion assertion) throws IOException, InterruptedException
    {
        assertion.apply();
    }

    /**
     * A class to record assumptions about the file's state and a method to
     * enact those assumptions.
     */
    public static class FileStateAssertion
    {
        private final File _file;

        private String _contents;
        private boolean _deleteFile;
        private long _mtime;

        public FileStateAssertion(File file)
        {
            _file = file;
        }

        public FileStateAssertion hasContents(String contents) throws IOException
        {
            _contents = contents.endsWith("\n") ? contents : (contents + '\n');
            return this;
        }

        public FileStateAssertion isEmpty() throws IOException
        {
            _contents = "";
            return this;
        }

        public FileStateAssertion isMissing()
        {
            _deleteFile = true;
            return this;
        }

        public FileStateAssertion withDifferentMtime() throws InterruptedException
        {
            _mtime = _file.lastModified();
            return this;
        }

        public void apply() throws IOException, InterruptedException
        {
            checkState(_deleteFile || _contents != null);
            checkState(!_deleteFile || _mtime == 0);

            if (_deleteFile) {
                _file.delete();
            } else {
                updateContent();
                while (_mtime != 0 && _mtime == _file.lastModified()) {
                    Thread.sleep(10);
                    updateContent();
                }
            }
        }

        private void updateContent() throws IOException
        {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(_file))) {
                writer.write(_contents);
            }
        }
    }
}

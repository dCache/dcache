package diskCacheV111.util;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import diskCacheV111.vehicles.ProtocolInfo;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.EnumSet;
import java.util.function.Function;
import java.util.regex.PatternSyntaxException;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Activity;
import org.dcache.auth.attributes.MultiTargetedRestriction;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.util.StorageInfoBuilder;
import org.dcache.vehicles.FileAttributes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CheckStagePermissionTests {
    /*
     * definition is taken verbatim from NFS4ProtocolInfo
     * (NFS4) and major=4 seem to be redundant
     */

    public static final ProtocolInfo NFS4_PROTOCOL_INFO = new ProtocolInfo() {
        private static final String _protocolName = "NFS4";
        private static final int _minor = 1;
        private static final int _major = 4;

        @Override
        public String getProtocol() {
            return _protocolName;
        }

        @Override
        public int getMinorVersion() {
            return _minor;
        }

        @Override
        public int getMajorVersion() {
            return _major;
        }

        @Override
        public String getVersionString() {
            return _protocolName + "-" + _major + "." + _minor;
        }
    };


    public static final ProtocolInfo XROOTD_PROTOCOL_INFO = new ProtocolInfo() {
        private static final String _protocolName = "Xrootd";
        private static final int _minor = 2;
        private static final int _major = 7;

        @Override
        public String getProtocol() {
            return _protocolName;
        }

        @Override
        public int getMinorVersion() {
            return _minor;
        }

        @Override
        public int getMajorVersion() {
            return _major;
        }

        @Override
        public String getVersionString() {
            return _protocolName + "-" + _major + "." + _minor;
        }
    };

    private File _file;
    private CheckStagePermission _check;
    private boolean _allowed;
    private boolean _anonymousAllowed = true;

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
     *  Tests that check behaviour when stage protection is disabled.
     */

    @Test
    public void shouldAllowUserWithDnAndFqanToStageIfEmptyConstructed() throws Exception {
        givenCheckConstructedWith("");

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                    .dn("/DC=org/DC=example/CN=test user")
                    .fqan("/atlas/Role=production").fqan("/atlas"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldAllowUserWithDnToStageIfEmptyConstructed() throws Exception {
        givenCheckConstructedWith("");

        whenCheck(Subjects.of().uid(100).gid(100).username("testuser")
                    .dn("/DC=org/DC=example/CN=test user"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldAllowUserWithDnAndFqanToStageIfNullConstructed() throws Exception {
        givenCheckConstructedWith(null);

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                    .dn("/DC=org/DC=example/CN=test user").fqan("/atlas/Role=production").fqan("/atlas"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldAllowUserWithDnToStageIfNullConstructed() throws Exception {
        givenCheckConstructedWith(null);

        whenCheck(Subjects.of().uid(100).gid(100).username("testuser")
                    .dn("/DC=org/DC=example/CN=test user"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(true));
    }

    /*
     *  Tests that check behaviour when stage protection file is empty.
     */

    @Test()
    public void shouldDenyUserWithDnAndFqanToStageIfEmpty() throws Exception {
        given(file().isEmpty());

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                    .dn("/DC=org/DC=example/CN=test user")
                    .fqan("/atlas/Role=production").fqan("/atlas"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(false));
    }

    /*
     *  Tests that check behaviour when stage protection file is missing.
     */

    @Test()
    public void shouldDenyUserWithDnAndFqanToStageIfMissing() throws Exception {
        given(file().isMissing());

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                    .dn("/DC=org/DC=example/CN=test user")
                    .fqan("/atlas/Role=production").fqan("/atlas"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(false));
    }


    /*
     *  Tests that check behaviour when a DN is authorised to stage.
     */

    @Test
    public void shouldAllowUserWithDnToStageIfDnAuthz() throws Exception {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("testuser")
                    .dn("/DC=org/DC=example/CN=test user"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldAllowUserWithDnAndFqanToStageIfDnAuthz() throws Exception {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                    .dn("/DC=org/DC=example/CN=test user")
                    .fqan("/atlas/Role=production").fqan("/atlas"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldDenyUserWithWrongDnToStageIfDnAuthz() throws Exception {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("testuser")
                    .dn("/DC=org/DC=otherExample/CN=test user"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(false));
    }

    /*
     *  Tests that check behaviour when a (DN,FQAN) pair is authorised to stage.
     */

    @Test
    public void shouldDenyUserWithDnToStageIfDnFqanAuthz() throws Exception {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("testuser")
                    .dn("/DC=org/DC=example/CN=test user"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(false));
    }

    @Test
    public void shouldDenyUserWithDnAndWrongFqanToStageIfDnFqanAuthz() throws Exception {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("atlas")
                    .dn("/DC=org/DC=example/CN=test user").fqan("/atlas"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(false));
    }

    @Test
    public void shouldAllowUserWithDnAndFqanToStageIfDnFqanAuthz() throws Exception {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("testuser")
                    .dn("/DC=org/DC=example/CN=test user")
                    .fqan("/atlas/Role=production").fqan("/atlas"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldAllowUserWithDnAndNonprimaryFqanToStageIfDnFqanAuthz() throws Exception {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\" \"/atlas\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                    .dn("/DC=org/DC=example/CN=test user")
                    .fqan("/atlas/Role=production").fqan("/atlas"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldDenyUserWithWrongDnToStageIfDnFqanAuthz() throws Exception {
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("testuser")
                    .dn("/DC=org/DC=anotherExample/CN=test user"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(false));
    }


    /*
     * Tests to check that updating a file is honoured.
     */

    @Test
    public void shouldAllowUserWithDnToStageIfDnAuthzAfterReload() throws Exception {
        given(file().isEmpty());
        givenCheckedWith(Subjects.of().uid(100).gid(100).dn("/DC=org/DC=example/CN=test user"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"),
              NFS4_PROTOCOL_INFO);
        // Note: filesystems have differing granularity of their timestamp: so
        //       we must make sure the mtime has increased.
        given(file().hasContents("\"/DC=org/DC=example/CN=test user\"").withDifferentMtime());

        whenCheck(Subjects.of().uid(100).gid(100).username("testuser")
                    .dn("/DC=org/DC=example/CN=test user"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"),
              NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(true));
    }

    /*
     *  Tests that check behaviour when a pattern-based (DN,FQAN) pair is
     *  authorised to stage a specific storage class.
     */

    @Test
    public void shouldAllowUserWithDnToStageIfWildDnWildFqanAuthz() throws Exception {
        given(file().hasContents("\"/DC=org/DC=example/.*\" \"/atlas/Role=.*\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                    .dn("/DC=org/DC=example/CN=test")
                    .fqan("/atlas/Role=production").fqan("/atlas"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldDenyUserWithWrongDnToStageIfWildDnWildFqanAuthz() throws Exception {
        given(file().hasContents("\"/DC=org/DC=example/.*\" \"/atlas/Role=.*\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                    .dn("/DC=org/DC=anotherExample/CN=test")
                    .fqan("/atlas/Role=production").fqan("/atlas"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(false));
    }

    @Test
    public void shouldAllowUserWithDnFqanToStageIfAnyDnAnyFqanAnyStoreAuthz() throws Exception {
        given(file().hasContents("\".*\" \".*\" \".*\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                    .dn("/DC=org/DC=anotherExample/CN=test user")
                    .fqan("/atlas/Role=production").fqan("/atlas"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(true));
    }

    /*
     *  Tests that check behaviour when a (DN,FQAN) pair is authorised to stage
     *  a specific storage class.
     */

    @Test
    public void shouldAllowUserWithDnFqanToStageStoreIfDnFqanStoreAuthz() throws Exception {
        given(file().hasContents(
              "\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\" \"sql:chimera@osm\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                    .dn("/DC=org/DC=example/CN=test user")
                    .fqan("/atlas/Role=production").fqan("/atlas"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldDenyUserWithDnFqanToStageWrongStoreIfDnFqanStoreAuthz() throws Exception {
        given(file().hasContents(
              "\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\" \"sql:chimera@osm\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                    .dn("/DC=org/DC=example/CN=test user")
                    .fqan("/atlas/Role=production").fqan("/atlas"),
              FileAttributes.of().storageClass("data:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(false));
    }

    /*
     *  Tests that check behaviour when a (DN,FQAN,StorageUnit) triplet is authorised to stage
     *  a specific protocol
     */

    @Test
    public void shouldAllowUserWithDnFqanStoreToStageIfDnFqanStoreProtocolAuthz() throws Exception {
        given(file().hasContents(
              "\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\" \"sql:chimera@osm\" \"NFS4/4\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                    .dn("/DC=org/DC=example/CN=test user")
                    .fqan("/atlas/Role=production").fqan("/atlas"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldDenyUserWithDnFqanStoreWrongProtocoIfDnFqanStoreAuthz() throws Exception {
        given(file().hasContents(
              "\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\" \"sql:chimera@osm\" \"DCap/3\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                    .dn("/DC=org/DC=example/CN=test user")
                    .fqan("/atlas/Role=production").fqan("/atlas"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(false));
    }

    @Test
    public void shouldDenyUserWithDnFqanStoreProtocoIfDnFqanStoreAuthzButPtocolIsBanned()
          throws Exception {
        given(file().hasContents(
              "\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\" \"sql:chimera@osm\" \"!NFS4/4\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                    .dn("/DC=org/DC=example/CN=test user")
                    .fqan("/atlas/Role=production").fqan("/atlas"),
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(false));
    }

    @Test
    public void shouldAllowAnonymousUserIfStoreIsNotBlackListedProtocolNotListed()
          throws Exception {
        given(file().hasContents("\"\" \"\" \"!sql:chimera@osm\""));

        whenCheck(Subjects.NOBODY,
              FileAttributes.of().storageClass("sql:chimera").hsm("enstore"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldAllowAnonymousUserStoreIfProtocolNotBlackListed() throws Exception {
        given(file().hasContents("\"\" \"\" \"sql:chimera@osm\" \"!NFS.*\""));

        whenCheck(Subjects.NOBODY,
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), XROOTD_PROTOCOL_INFO);

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldNotAllowAnonymousUserIfAnonymousDisallowedAndNoFile() throws Exception {
        given(file().isMissing());
        givenAllowAnonymousAccess(false);

        whenCheck(Subjects.NOBODY,
              FileAttributes.of().storageClass("sql:chimera").hsm("enstore"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(false));
    }

    @Test
    public void shouldNotAllowAnonymousUserIfAnonymousDisallowedAndStoreIsNotBlackListedProtocolNotListed()
          throws Exception {
        given(file().hasContents("\"\" \"\" \"!sql:chimera@osm\""));
        givenAllowAnonymousAccess(false);

        whenCheck(Subjects.NOBODY,
              FileAttributes.of().storageClass("sql:chimera").hsm("enstore"), NFS4_PROTOCOL_INFO);

        assertThat(_allowed, is(false));
    }

    @Test
    public void shouldNotAllowAnonymousUserStoreIfAnonymousDisallowedAndProtocolNotBlackListed()
          throws Exception {
        given(file().hasContents("\"\" \"\" \"sql:chimera@osm\" \"!NFS.*\""));
        givenAllowAnonymousAccess(false);

        whenCheck(Subjects.NOBODY,
              FileAttributes.of().storageClass("sql:chimera").hsm("osm"), XROOTD_PROTOCOL_INFO);

        assertThat(_allowed, is(false));
    }

    @Test
    public void shouldAllowAnonymousAccessWithNullRestriction() throws Exception {
        given(file().hasContents("\"\" \"\" \"sql:chimera@osm\" \"!NFS.*\""));
        givenAllowAnonymousAccess(true);

        whenCheck(Subjects.NOBODY,
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"), XROOTD_PROTOCOL_INFO,
                null);

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldAllowAnonymousAccessWithNoRestrictions() throws Exception {
        given(file().hasContents("\"\" \"\" \"sql:chimera@osm\" \"!NFS.*\""));
        givenAllowAnonymousAccess(true);

        whenCheck(Subjects.NOBODY,
                FileAttributes.of().storageClass("sql:chimera").hsm("osm"), XROOTD_PROTOCOL_INFO,
                Restrictions.none());

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldAllowAccessWithRestrictions() throws Exception {
        given(file().hasContents(
                "\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\" \"sql:chimera@osm\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                        .dn("/DC=org/DC=example/CN=test user")
                        .fqan("/atlas/Role=production").fqan("/atlas").build(),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm").storageInfo((new StorageInfoBuilder()).withKey("path", "/pnfs/user/my/file.dat").build()),
                NFS4_PROTOCOL_INFO,
                new MultiTargetedRestriction(singleton(new MultiTargetedRestriction.Authorisation(EnumSet.of(Activity.STAGE), FsPath.create("/pnfs/user/my")))));

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldDenyAccessForRestrictedPath() throws Exception {
        given(file().hasContents(
                "\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\" \"sql:chimera@osm\""));

        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                        .dn("/DC=org/DC=example/CN=test user")
                        .fqan("/atlas/Role=production").fqan("/atlas").build(),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm").storageInfo((new StorageInfoBuilder()).withKey("path", "/pnfs/user/his/file.dat").build()),
                NFS4_PROTOCOL_INFO,
                new MultiTargetedRestriction(singleton(new MultiTargetedRestriction.Authorisation(EnumSet.of(Activity.STAGE), FsPath.create("/pnfs/user/my")))));

        assertThat(_allowed, is(false));
    }

    @Test
    public void shouldAllowAccessToRestrictedLogicalPath() throws Exception {
        given(file().hasContents(
                "\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\" \"sql:chimera@osm\""));
        Function<FsPath, FsPath> fakeSymlinkResolver = (FsPath p) -> {
            return FsPath.create(p.toString().replaceAll("^/pnfs/user/my(/|$)", "/pnfs/user/his$1"));
        };
        Restriction r = new MultiTargetedRestriction(singleton(new MultiTargetedRestriction.Authorisation(EnumSet.of(Activity.STAGE), FsPath.create("/pnfs/user/my"))));
        r.setPathResolver(fakeSymlinkResolver);
        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                .dn("/DC=org/DC=example/CN=test user")
                .fqan("/atlas/Role=production").fqan("/atlas").build(),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm").storageInfo((new StorageInfoBuilder()).withKey("path", "/pnfs/user/his/file.dat").build()),
                NFS4_PROTOCOL_INFO, r);

        assertThat(_allowed, is(true));
    }

    @Test
    public void shouldAllowLogicalAccessToRestrictedPath() throws Exception {
        given(file().hasContents(
                "\"/DC=org/DC=example/CN=test user\" \"/atlas/Role=production\" \"sql:chimera@osm\""));
        Function<FsPath, FsPath> fakeSymlinkResolver = (FsPath p) -> {
            return FsPath.create(p.toString().replaceAll("^/pnfs/user/his(/|$)", "/pnfs/user/my$1"));
           };
        Restriction r = new MultiTargetedRestriction(singleton(new MultiTargetedRestriction.Authorisation(EnumSet.of(Activity.STAGE), FsPath.create("/pnfs/user/my"))));
        r.setPathResolver(fakeSymlinkResolver);
        whenCheck(Subjects.of().uid(100).gid(100).username("atlasprod")
                        .dn("/DC=org/DC=example/CN=test user")
                        .fqan("/atlas/Role=production").fqan("/atlas").build(),
                FileAttributes.of().storageClass("sql:chimera").hsm("osm").storageInfo((new StorageInfoBuilder()).withKey("path", "/pnfs/user/his/file.dat").build()),
                NFS4_PROTOCOL_INFO, r);

        assertThat(_allowed, is(true));
    }

    private void whenCheck(Subjects.Builder subjectBuilder,
                           FileAttributes.Builder attributeBuilder,
                           ProtocolInfo protocolInfo) throws PatternSyntaxException, IOException {
        whenCheck(subjectBuilder.build(), attributeBuilder, protocolInfo);
    }

    private void whenCheck(Subject subject,
                           FileAttributes.Builder attributeBuilder,
                           ProtocolInfo protocolInfo) throws PatternSyntaxException, IOException {
        _allowed = getCheck().canPerformStaging(subject, attributeBuilder.build(), protocolInfo);
    }

    private void whenCheck(Subject subject,
          FileAttributes.Builder attributeBuilder,
          ProtocolInfo protocolInfo,
          Restriction restriction) throws PatternSyntaxException, IOException {
        _allowed = getCheck().canPerformStaging(subject, attributeBuilder.build(), protocolInfo, restriction);
    }

    private CheckStagePermission getCheck() throws IOException {
        if (_check == null) {
            _check = new CheckStagePermission(_file.getCanonicalPath());
            _check.setAllowAnonymousStaging(_anonymousAllowed);
        }
        return _check;
    }

    private void givenCheckConstructedWith(String argument) {
        _check = new CheckStagePermission(argument);
    }

    private void givenCheckedWith(Subjects.Builder subjectBuilder,
          FileAttributes.Builder attributeBuilder,
          ProtocolInfo protocolInfo) throws PatternSyntaxException, IOException {
        getCheck().canPerformStaging(subjectBuilder.build(), attributeBuilder.build(),
              protocolInfo);
    }

    private void givenAllowAnonymousAccess(boolean allowed) {
        _anonymousAllowed = allowed;
    }

    private FileStateAssertion file() throws IOException {
        if (_file == null) {
            _file = File.createTempFile("stagePermissionFile", null, null);
            _file.deleteOnExit();
        }
        return new FileStateAssertion(_file);
    }

    private void given(FileStateAssertion assertion) throws IOException, InterruptedException {
        assertion.apply();
    }

    /**
     * A class to record assumptions about the file's state and a method to enact those
     * assumptions.
     */
    public static class FileStateAssertion {

        private final File _file;

        private String _contents;
        private boolean _deleteFile;
        private long _mtime;

        public FileStateAssertion(File file) {
            _file = file;
        }

        public FileStateAssertion hasContents(String contents) throws IOException {
            _contents = contents.endsWith("\n") ? contents : (contents + '\n');
            return this;
        }

        public FileStateAssertion isEmpty() throws IOException {
            _contents = "";
            return this;
        }

        public FileStateAssertion isMissing() {
            _deleteFile = true;
            return this;
        }

        public FileStateAssertion withDifferentMtime() throws InterruptedException {
            // CheckStagePermission uses current time (rather than file's mtime)
            // when checking if the file has been modified.  Not only is this
            // wrong (racy), it also means the testing code must also use
            // currentTimeMillis to avoid race-conditions when testing.
            _mtime = System.currentTimeMillis();
            return this;
        }

        public void apply() throws IOException, InterruptedException {
            checkState(_deleteFile || _contents != null);
            checkState(!_deleteFile || _mtime == 0);

            if (_deleteFile) {
                _file.delete();
            } else {
                updateContent();
                while (_mtime != 0 && _mtime >= _file.lastModified()) {
                    Thread.sleep(10);
                    updateContent();
                }
            }
        }

        private void updateContent() throws IOException {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(_file))) {
                writer.write(_contents);
            }
        }
    }
}

package org.dcache.chimera;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.dcache.chimera.FileSystemProvider.SetXattrMode;
import static org.dcache.chimera.FileSystemProvider.StatCacheOption.NO_STAT;
import static org.dcache.chimera.FileSystemProvider.StatCacheOption.STAT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.dcache.acl.ACE;
import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.AceType;
import org.dcache.acl.enums.RsType;
import org.dcache.acl.enums.Who;
import org.dcache.chimera.posix.Stat;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

public class JdbcFsTest extends ChimeraTestCaseHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcFsTest.class);

    @Test
    public void testLevelRemoveOnDelete() throws Exception {
        final int level = 1;
        FsInode inode = _rootInode.create("testLevelRemoveOnDelete", 0, 0, 0644);
        _fs.createFileLevel(inode, level);
        FsInode levelInode = new FsInode(_fs, inode.ino(), level);
        assertTrue(levelInode.exists());

        _fs.remove(_rootInode, "testLevelRemoveOnDelete", inode);
        levelInode = new FsInode(_fs, inode.ino(), level);
        assertFalse(levelInode.exists());
    }

    @Test
    public void testLs() throws Exception {

        long count = DirectoryStreamHelper.streamOf(_rootInode).count();
        assertTrue("Root dir can't be empty", count > 0L);
    }

    @Test
    public void testMkDir() throws Exception {

        Stat stat = _rootInode.stat();
        FsInode newDir = _rootInode.mkdir("junit");

        assertEquals("mkdir have to incrise parent's nlink count by one",
              _rootInode.stat().getNlink(), stat.getNlink() + 1);
        assertTrue("mkdir have to update parent's mtime",
              _rootInode.stat().getMTime() > stat.getMTime());
        assertEquals("new dir should have link count equal to two", newDir.stat().getNlink(), 2);
        assertTrue("change count is not updated",
              stat.getGeneration() != _rootInode.stat().getGeneration());
    }

    @Test
    public void testMkDirByPath() throws Exception {

        Stat stat = _rootInode.stat();
        FsInode newDir = _fs.mkdir("/junit");

        assertEquals("mkdir has to increase parent's nlink count by one",
              stat.getNlink() + 1, _rootInode.stat().getNlink());
        assertTrue("mkdir has to update parent's mtime",
              _rootInode.stat().getMTime() > stat.getMTime());
        assertEquals("new dir should have link count equal to two", 2, newDir.stat().getNlink());
        assertTrue("change count is not updated",
              stat.getGeneration() != _rootInode.stat().getGeneration());
    }

    @Test
    public void testMkDirInSetGidDir() throws Exception {

        FsInode dir1 = _rootInode.mkdir("junit", 1, 2, 02755);
        FsInode dir2 = dir1.mkdir("test", 1, 3, 0755);

        assertEquals("owner is not respected", dir2.stat().getUid(), 1);
        assertEquals("setgid is not respected", dir2.stat().getGid(), 2);
        assertEquals("setgid is not respected",
              dir2.stat().getMode() & UnixPermission.S_PERMS, 02755);
    }

    @Test
    public void testMkDirWithTags() throws Exception {
        byte[] bytes = "value".getBytes();
        FsInode dir1 = _fs.mkdir(_rootInode, "junit", 1, 2, 02755, Collections.emptyList(),
              ImmutableMap.of("tag", bytes));
        assertThat(_fs.getAllTags(dir1), hasEntry("tag", bytes));
    }

    @Test
    public void testCreateFile() throws Exception {
        FsInode base = _rootInode.mkdir("junit");
        Stat stat = base.stat();

        Thread.sleep(1); // required to ensure file created in distinct millisecond

        FsInode newFile = base.create("testCreateFile", 0, 0, 0644);

        assertEquals("file creation shouldn't change parent's nlink count",
              base.stat().getNlink(), stat.getNlink());

        assertTrue("file creation has to update parent's mtime",
              base.stat().getMTime() > stat.getMTime());
        assertEquals("new file should have link count equal to one", newFile.stat().getNlink(), 1);
        assertTrue("change count is not updated",
              stat.getGeneration() != base.stat().getGeneration());
    }

    @Test
    public void testNlinkCountOfNewDir() throws Exception {

        int parentNlink = _rootInode.stat().getNlink();
        FsInode newDir = _rootInode.mkdir("junit", 1, 2, 0755);
        assertEquals(parentNlink + 1, _rootInode.stat().getNlink());
        assertEquals(2, newDir.stat().getNlink());
    }

    @Test
    public void testCreateFilePermission() throws Exception {

        FsInode base = _rootInode.mkdir("junit");
        int mode1 = 0644;
        int mode2 = 0755;
        FsInode file1 = base.create("testCreateFilePermission1", 0, 0, mode1);
        FsInode file2 = base.create("testCreateFilePermission2", 0, 0, mode2);

        assertEquals("creare pemissions are not respected",
              file1.stat().getMode() & UnixPermission.S_PERMS, mode1);
        assertEquals("creare pemissions are not respected",
              file2.stat().getMode() & UnixPermission.S_PERMS, mode2);

    }

    @Test
    public void testCreateFilePermissionAndOwnerOnSetGidDirectory() throws Exception {
        FsInode base = _rootInode.mkdir("junit", 1, 2, 02775);
        int mode1 = 0644;
        int mode2 = 0755;
        FsInode file1 = base.create("testCreateFilePermission1", 0, 0, mode1);
        FsInode file2 = base.create("testCreateFilePermission2", 0, 0, mode2);

        assertEquals("owner is not respected", file1.stat().getUid(), 0);
        assertEquals("setgid is not respected", file1.stat().getGid(), 2);
        assertEquals("create pemissions are not respected",
              file1.stat().getMode() & UnixPermission.S_PERMS, mode1);

        assertEquals("owner is not respected", file2.stat().getUid(), 0);
        assertEquals("setgid is not respected", file2.stat().getGid(), 2);
        assertEquals("create pemissions are not respected",
              file2.stat().getMode() & UnixPermission.S_PERMS, mode2);
    }

    @Test // (expected=FileExistsChimeraFsException.class)
    public void testCreateFileDup() throws Exception {

        FsInode base = _rootInode.mkdir("junit");

        try {
            base.create("testCreateFile", 0, 0, 0644);
            base.create("testCreateFile", 0, 0, 0644);

            fail("you can't create a file twice");

        } catch (FileExistsChimeraFsException fee) {
            // OK
        }
    }

    @Test // (expected=FileExistsChimeraFsException.class)
    public void testCreateDirDup() throws Exception {

        FsInode base = _rootInode.mkdir("junit");

        try {
            base.mkdir("testCreateDir");
            base.mkdir("testCreateDir");

            fail("you can't create a directory twice");

        } catch (FileExistsChimeraFsException fee) {
            // OK
        }
    }

    @Test(expected = DirNotEmptyChimeraFsException.class)
    public void testDeleteNonEmptyDir() throws Exception {

        FsInode base = _rootInode.mkdir("junit");

        base.create("testCreateFile", 0, 0, 0644);
        _rootInode.remove("junit");

    }

    @Test
    public void testDeleteFile() throws Exception {

        FsInode base = _rootInode.mkdir("junit");

        base.create("testCreateFile", 0, 0, 0644);
        Stat stat = base.stat();

        Thread.sleep(1); // ensure updated directory mtime is distinct from creation mtime

        base.remove("testCreateFile");

        assertEquals("remove should not decrement parents link count", base.stat().getNlink(), stat.getNlink());
        assertFalse("remove have to update parent's mtime",
              stat.getMTime() == base.stat().getMTime());

    }

    @Test
    public void testDeleteDir() throws Exception {

        FsInode base = _rootInode.mkdir("junit");

        base.mkdir("testCreateDir", 0, 0, 0644);
        Stat stat = base.stat();

        Thread.sleep(1); // ensure updated directory mtime is distinct from creation mtime

        base.remove("testCreateDir");

        assertEquals("remove have to decrease parents link count", base.stat().getNlink(),
              stat.getNlink() - 1);
        assertFalse("remove have to update parent's mtime",
              stat.getMTime() == base.stat().getMTime());

    }

    @Test(expected = FileNotFoundChimeraFsException.class)
    public void testDeleteNonExistingFile() throws Exception {
        _rootInode.remove("testCreateFile");
    }

    @Test(expected = FileNotFoundChimeraFsException.class)
    public void testCreateInNonExistingDir() throws Exception {
        FsInode missingDir = new FsInode(_fs, Long.MAX_VALUE);
        _fs.createFile(missingDir, "aFile");
    }

    @Test
    public void testDeleteInFile() throws Exception {

        FsInode fileInode = _rootInode.create("testCreateFile", 0, 0, 0644);
        try {
            fileInode.remove("anObject");
            fail("you can't remove an  object in file Inode");
        } catch (ChimeraFsException ioe) {
            // OK
        }
    }

    @Test
    public void testCreateInFile() throws Exception {

        FsInode fileInode = _rootInode.create("testCreateFile", 0, 0, 0644);
        try {
            fileInode.create("anObject", 0, 0, 0644);
            fail("you can't create an  object in file Inode");
        } catch (NotDirChimeraException nde) {
            // OK
        }
    }

    @Test
    public void testHardLink() throws Exception {

        FsInode base = _rootInode.mkdir("junit");

        FsInode fileInode = base.create("hardLinkTestSourceFile", 0, 0, 0644);

        Stat stat = fileInode.stat();

        FsInode hardLinkInode = _fs.createHLink(base, fileInode, "hardLinkTestDestinationFile");

        assertEquals("hard link's  have to increase link count by one", stat.getNlink() + 1,
              hardLinkInode.stat().getNlink());

        _fs.remove(base, "hardLinkTestDestinationFile", hardLinkInode);
        assertTrue("removing of hard link have to decrease link count by one",
              1 == fileInode.stat().getNlink());

    }

    @Test
    public void testRemoveLinkToDir() throws Exception {

        FsInode base = _rootInode.mkdir("junit");

        _fs.createLink(base, "aLink", "/junit");
    }

    @Test
    public void testRemoveLinkToSomewhare() throws Exception {

        FsInode linkBase = _rootInode.mkdir("links");

        _fs.createLink(linkBase, "file123", 0, 0, 0644, "/files/file123".getBytes(UTF_8));
        _fs.remove("/links/file123");
    }

    @Test
    public void testRemoveLinkToFile() throws Exception {

        FsInode fileBase = _rootInode.mkdir("files");
        FsInode linkBase = _rootInode.mkdir("links");
        FsInode fileInode = fileBase.create("file123", 0, 0, 0644);

        _fs.createLink(linkBase, "file123", 0, 0, 0644, "/files/file123".getBytes(UTF_8));
        _fs.remove("/links/file123");

        assertTrue("original file is gone!", fileInode.exists());
    }

    @Test(expected = FileNotFoundChimeraFsException.class)
    public void testHardLinkOfMissingParent() throws Exception {

        FsInode dir = new FsInode(_fs, 1111);

        _fs.createHLink(dir, _rootInode, "directoryAsHardLink");
    }

    @Test(expected = FileNotFoundChimeraFsException.class)
    public void testHardLinkOfMissingInode() throws Exception {

        FsInode dir = new FsInode(_fs, 1111);

        _fs.createHLink(_rootInode, dir, "directoryAsHardLink");
    }

    @Test(expected = PermissionDeniedChimeraFsException.class)
    public void testDirHardLink() throws Exception {

        FsInode base = _rootInode.mkdir("junit");

        FsInode dirInode = base.mkdir("dirHadrLinkTestSrouceDir");

        FsInode hardLinkInode = _fs.createHLink(base, dirInode, "dirHadrLinkTestDestinationDir");
    }

    @Test
    public void testSymLink() throws Exception {

        FsInode base = _rootInode.mkdir("junit");
        FsInode fileInode = base.create("testCreateFile", 0, 0, 0644);

        try {
            fileInode.createLink("aLink", 0, 0, 0644, "../junit".getBytes());
            fail("can't create a link in non directory inode");
        } catch (NotDirChimeraException e) {
            // OK
        }
    }

    @Test
    public void testRenameNonExistSameDir() throws Exception {

        FsInode base = _rootInode.mkdir("junit");
        FsInode fileInode = base.create("testCreateFile", 0, 0, 0644);

        Stat preStatBase = base.stat();
        Stat preStatFile = fileInode.stat();

        boolean ok = _fs.rename(fileInode, base, "testCreateFile", base, "testCreateFile2");

        assertTrue("can't move", ok);
        assertEquals("link count of base directory should not be modified in case of rename",
              preStatBase.getNlink(), base.stat().getNlink());
        assertEquals("link count of file shold not be modified in case of rename",
              preStatFile.getNlink(), fileInode.stat().getNlink());
        try {
            base.inodeOf("testCreateFile", NO_STAT);
            fail("Some inode exists under source name");
        } catch (FileNotFoundChimeraFsException e) {
            // Success.
        }
        assertEquals("should resolve same inode under target name", fileInode,
                base.inodeOf("testCreateFile2", NO_STAT));
    }

    @Test
    public void testRenameExistSameDir() throws Exception {

        FsInode base = _rootInode.mkdir("junit");
        FsInode fileInode = base.create("testCreateFile", 0, 0, 0644);
        FsInode file2Inode = base.create("testCreateFile2", 0, 0, 0644);

        Stat preStatBase = base.stat();
        file2Inode.setStatCache(null);

        boolean ok = _fs.rename(fileInode, base, "testCreateFile", base, "testCreateFile2");

        assertTrue("can't move", ok);
        assertEquals("link count of base directory shouldn't change", preStatBase.getNlink(), base.stat().getNlink());
        try {
            base.inodeOf("testCreateFile", NO_STAT);
            fail("Some inode exists under source name");
        } catch (FileNotFoundChimeraFsException e) {
            // Success.
        }
        assertEquals("should resolve same inode under target name", fileInode,
                base.inodeOf("testCreateFile2", NO_STAT));

        assertFalse("ghost file", file2Inode.exists());

    }

    @Test
    public void testRenameNonExistNotSameDir() throws Exception {

        FsInode base = _rootInode.mkdir("junit");
        FsInode base2 = _rootInode.mkdir("junit2");
        FsInode fileInode = base.create("testCreateFile", 0, 0, 0644);

        Stat preStatBase = base.stat();
        Stat preStatBase2 = base2.stat();
        Stat preStatFile = fileInode.stat();

        boolean ok = _fs.rename(fileInode, base, "testCreateFile", base2, "testCreateFile2");

        assertTrue("can't move", ok);
        assertEquals("link count of source directory shouldn't change on move out", preStatBase.getNlink(),
              base.stat().getNlink());
        assertEquals("link count of destination directory should change on move in", preStatBase2.getNlink(), base2.stat().getNlink());
        assertEquals("link count of file should not be modified on move", preStatFile.getNlink(),
              fileInode.stat().getNlink());
        try {
            base.inodeOf("testCreateFile", NO_STAT);
            fail("Some inode exists under source name in source directory");
        } catch (FileNotFoundChimeraFsException e) {
            // Success.
        }
        try {
            base2.inodeOf("testCreateFile", NO_STAT);
            fail("Some inode exists under source name in target directory.");
        } catch (FileNotFoundChimeraFsException e) {
            // Success.
        }
        try {
            base.inodeOf("testCreateFile2", NO_STAT);
            fail("Some inode exists under target name in source directory");
        } catch (FileNotFoundChimeraFsException e) {
            // Success.
        }
        assertEquals("should resolve same inode under target name in target directory",
                fileInode, base2.inodeOf("testCreateFile2", NO_STAT));
    }

    @Test
    public void testRenameExistNotSameDir() throws Exception {

        FsInode base = _rootInode.mkdir("junit");
        FsInode base2 = _rootInode.mkdir("junit2");
        FsInode fileInode = base.create("testCreateFile", 0, 0, 0644);
        FsInode fileInode2 = base2.create("testCreateFile2", 0, 0, 0644);

        Stat preStatBase = base.stat();
        Stat preStatBase2 = base2.stat();
        Stat preStatFile = fileInode.stat();
        fileInode2.setStatCache(null);

        boolean ok = _fs.rename(fileInode, base, "testCreateFile", base2, "testCreateFile2");

        assertTrue("can't move", ok);
        assertEquals("link count of source directory should should not be modified", preStatBase.getNlink(),
              base.stat().getNlink());
        assertEquals("link count of destination directory should not be modified on replace", preStatBase2.getNlink(), base2.stat().getNlink());
        assertEquals("link count of file shold not be modified on move", preStatFile.getNlink(), fileInode.stat().getNlink());

        assertFalse("ghost file", fileInode2.exists());
        try {
            base.inodeOf("testCreateFile", NO_STAT);
            fail("Some inode exists under source name in source directory");
        } catch (FileNotFoundChimeraFsException e) {
            // Success.
        }
        try {
            base2.inodeOf("testCreateFile", NO_STAT);
            fail("Some inode exists under source name in target directory.");
        } catch (FileNotFoundChimeraFsException e) {
            // Success.
        }
        try {
            base.inodeOf("testCreateFile2", NO_STAT);
            fail("Some inode exists under target name in source directory");
        } catch (FileNotFoundChimeraFsException e) {
            // Success.
        }
        assertEquals("should resolve same inode under target name in target directory",
                fileInode, base2.inodeOf("testCreateFile2", NO_STAT));
    }

    @Test
    public void testRenameHardLinkToItselfSameDir() throws Exception {

        FsInode base = _rootInode.mkdir("junit");
        FsInode fileInode = base.create("testCreateFile", 0, 0, 0644);
        FsInode linkInode = _fs.createHLink(base, fileInode, "testCreateFile2");

        Stat preStatBase = base.stat();
        Stat preStatFile = fileInode.stat();

        boolean ok = _fs.rename(fileInode, base, "testCreateFile", base, "testCreateFile2");

        assertFalse("rename of hardlink to itself should do nothing", ok);
        assertEquals("link count of base directory should not be modified in case of rename",
              preStatBase.getNlink(), base.stat().getNlink());
        assertEquals("link count of file should not be modified in case of rename",
              preStatFile.getNlink(),
              fileInode.stat().getNlink());
        assertEquals("should resolve same inode under target name in target directory",
                fileInode, base.inodeOf("testCreateFile", NO_STAT));
        assertEquals("should resolve same inode under target name in target directory",
                linkInode, base.inodeOf("testCreateFile2", NO_STAT));
    }

    @Test
    public void testRenameHardLinkToItselfNotSameDir() throws Exception {

        FsInode base = _rootInode.mkdir("junit");
        FsInode base2 = _rootInode.mkdir("junit2");
        FsInode fileInode = base.create("testCreateFile", 0, 0, 0644);
        FsInode linkInode = _fs.createHLink(base2, fileInode, "testCreateFile2");

        Stat preStatBase = base.stat();
        Stat preStatBase2 = base2.stat();
        Stat preStatFile = fileInode.stat();

        boolean ok = _fs.rename(fileInode, base, "testCreateFile", base2, "testCreateFile2");

        assertFalse("rename of hardlink to itself should do nothing", ok);
        assertEquals("link count of source directory should not be modified in case of rename",
              preStatBase.getNlink(),
              base.stat().getNlink());
        assertEquals("link count of destination directory should not be modified in case of rename",
              preStatBase2.getNlink(), base2.stat().getNlink());
        assertEquals("link count of file should not be modified in case of rename",
              preStatFile.getNlink(), fileInode.stat().getNlink());
        assertEquals("should resolve same inode under target name in target directory",
                fileInode, base.inodeOf("testCreateFile", NO_STAT));
        try {
            base.inodeOf("testCreateFile2", NO_STAT);
            fail("Some inode exists under destination name in source directory");
        } catch (FileNotFoundChimeraFsException e) {
            // Success.
        }
        try {
            base2.inodeOf("testCreateFile", NO_STAT);
            fail("Some inode exists under source name in target directory");
        } catch (FileNotFoundChimeraFsException e) {
            // Success.
        }
        assertEquals("should resolve same inode under target name in target directory",
                linkInode, base2.inodeOf("testCreateFile2", NO_STAT));
    }

    @Test
    public void testRemoveFileById() throws Exception {
        long n = getDirEntryCount(_rootInode);
        FsInode file = _rootInode.create("foo", 0, 0, 0644);
        _fs.remove(file);
        assertEquals(n, getDirEntryCount(_rootInode));
    }

    @Test
    public void testRemoveSeveralHardlinksById() throws Exception {
        long n = getDirEntryCount(_rootInode);
        FsInode file = _rootInode.create("foo", 0, 0, 0644);
        _fs.createHLink(_rootInode, file, "bar");
        _fs.remove(file);
        assertEquals(n, getDirEntryCount(_rootInode));
        assertEquals(n, _rootInode.stat().getNlink());
    }

    @Test
    public void testRemoveDirById() throws Exception {
        long n = getDirEntryCount(_rootInode);
        FsInode foo = _rootInode.mkdir("foo");
        _fs.remove(foo);
        assertEquals(n, getDirEntryCount(_rootInode));
    }

    @Test(expected = DirNotEmptyChimeraFsException.class)
    public void testRemoveNonEmptyDirById() throws Exception {
        FsInode foo = _rootInode.mkdir("foo");
        FsInode bar = foo.mkdir("bar");
        _fs.remove(foo);
    }

    @Test(expected = InvalidArgumentChimeraException.class)
    public void testRemoveRootById() throws Exception {
        _fs.remove(_rootInode);
    }

    @Test(expected = FileNotFoundChimeraFsException.class)
    public void testRemoveNonexistgById() throws Exception {
        FsInode inode = new FsInode(_fs, Long.MAX_VALUE);
        _fs.remove(inode);
    }

    @Test(expected = FileNotFoundChimeraFsException.class)
    public void testRemoveNonexistgByPath() throws Exception {
        FsInode base = _rootInode.mkdir("junit");
        base.remove("notexist");
    }

    @Test
    public void testRemoveFileByPath() throws Exception {
        long n = getDirEntryCount(_rootInode);
        FsInode file = _rootInode.create("foo", 0, 0, 0644);
        _fs.remove("/foo");
        assertEquals(n, getDirEntryCount(_rootInode));
    }

    @Test
    public void testRemoveDirByPath() throws Exception {
        long n = getDirEntryCount(_rootInode);
        FsInode foo = _rootInode.mkdir("foo");
        _fs.remove("/foo");
        assertEquals(n, getDirEntryCount(_rootInode));
    }

    @Test(expected = DirNotEmptyChimeraFsException.class)
    public void testRemoveNonEmptyDirByPath() throws Exception {
        FsInode foo = _rootInode.mkdir("foo");
        FsInode bar = foo.mkdir("bar");
        _fs.remove("/foo");
    }

    @Test(expected = InvalidArgumentChimeraException.class)
    public void testRemoveRootByPath() throws Exception {
        _fs.remove("/");
    }

    @Test
    public void testAddLocationForNonexistong() throws Exception {
        FsInode inode = new FsInode(_fs, Long.MAX_VALUE);
        try {
            _fs.addInodeLocation(inode, StorageGenericLocation.DISK, "/dev/null");
            fail("was able to add cache location for non existing file");
        } catch (FileNotFoundChimeraFsException e) {
            // OK
        }
    }

    @Test
    public void testDupAddLocation() throws Exception {

        FsInode base = _rootInode.mkdir("junit");

        FsInode fileInode = base.create("testCreateFile", 0, 0, 0644);
        _fs.addInodeLocation(fileInode, StorageGenericLocation.DISK, "/dev/null");
        _fs.addInodeLocation(fileInode, StorageGenericLocation.DISK, "/dev/null");
    }

    @Test(expected = FileNotFoundChimeraFsException.class)
    public void testSetSizeNotExist() throws Exception {

        FsInode inode = new FsInode(_fs, Long.MAX_VALUE);
        Stat stat = new Stat();
        stat.setSize(1);

        _fs.setInodeAttributes(inode, 0, stat);
    }

    @Test(expected = FileNotFoundChimeraFsException.class)
    public void testChowneNotExist() throws Exception {

        FsInode inode = new FsInode(_fs, Long.MAX_VALUE);
        Stat stat = new Stat();
        stat.setUid(3750);
        _fs.setInodeAttributes(inode, 0, stat);
    }

    @Test
    public void testUpdateLevelNotExist() throws Exception {
        FsInode inode = new FsInode(_fs, Long.MAX_VALUE);
        try {
            byte[] data = "bla".getBytes();
            _fs.write(inode, 1, 0, data, 0, data.length);
            fail("was able to update level of non existing file");
        } catch (FileNotFoundChimeraFsException e) {
            // OK
        }
    }

    @Test
    public void testUpdateChecksumNotExist() throws Exception {
        FsInode inode = new FsInode(_fs, Long.MAX_VALUE);
        try {
            _fs.setInodeChecksum(inode, 1, "asum");
            fail("was able to update checksum of non existing file");
        } catch (FileNotFoundChimeraFsException e) {
            // OK
        }
    }

    @Test
    public void testUpdateChecksum() throws Exception {
        String sum = "abc";

        FsInode base = _rootInode.mkdir("junit");
        FsInode fileInode = base.create("testCreateFile", 0, 0, 0644);
        _fs.setInodeChecksum(fileInode, 1, sum);
        assertHasChecksum(new Checksum(ChecksumType.getChecksumType(1), sum), fileInode);
    }

    @Test
    public void testCtimeOnUpdateChecksum() throws Exception {
        String sum = "abc";

        FsInode base = _rootInode.mkdir("junit");
        FsInode fileInode = base.create("testCreateFile", 0, 0, 0644);
        Stat stat = fileInode.stat();
        long before = stat.getCTime();
        long genBefore = stat.getGeneration();
        Thread.sleep(100);
        _fs.setInodeChecksum(fileInode, 1, sum);
        stat = fileInode.stat();
        long after = stat.getCTime();
        long genAfter = stat.getGeneration();
        assertNotEquals("ctime was not updated", before, after);
        assertNotEquals("generation was not updated", genBefore, genAfter);
    }

    @Test
    public void testCtimeOnRemoveChecksum() throws Exception {
        String sum = "abc";

        FsInode base = _rootInode.mkdir("junit");
        FsInode fileInode = base.create("testCreateFile", 0, 0, 0644);
        Stat stat = fileInode.stat();
        long before = stat.getCTime();
        long genBefore = stat.getGeneration();
        Thread.sleep(100);
        _fs.removeInodeChecksum(fileInode, 1);
        stat = fileInode.stat();
        long after = stat.getCTime();
        long genAfter = stat.getGeneration();
        assertNotEquals("ctime was not updated", before, after);
        assertNotEquals("generation was not updated", genBefore, genAfter);
    }

    @Ignore("Functionality not yet written, but desired")
    @Test
    public void testUpdateChecksumDifferTypes() throws Exception {
        String sum1 = "abc1";
        String sum2 = "abc2";

        FsInode base = _rootInode.mkdir("junit");
        FsInode fileInode = base.create("testCreateFile", 0, 0, 0644);
        _fs.setInodeChecksum(fileInode, 1, sum1);
        _fs.setInodeChecksum(fileInode, 2, sum2);
        assertHasChecksum(new Checksum(ChecksumType.getChecksumType(1), sum1), fileInode);
        assertHasChecksum(new Checksum(ChecksumType.getChecksumType(2), sum2), fileInode);
    }

    @Test
    public void testResolveLinkOnPathToId() throws Exception {

        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);
        FsInode linkInode = _rootInode.createLink("aLink", 0, 0, 055, "testDir".getBytes());

        FsInode inode = _fs.path2inode("aLink", _rootInode);
        assertEquals("Link resolution did not work", dirInode, inode);

    }

    @Test
    public void testResolveLinkOnPathToIds() throws Exception {
        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);
        FsInode linkInode = _rootInode.createLink("aLink", 0, 0, 055, "testDir".getBytes());

        List<FsInode> inodes = _fs.path2inodes("aLink", _rootInode);
        assertEquals("Link resolution did not work",
              Lists.newArrayList(_rootInode, linkInode, dirInode),
              inodes);
    }

    @Test
    public void testResolveLinkOnPathToIdRelative() throws Exception {

        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);
        FsInode linkInode = _rootInode.createLink("aLink", 0, 0, 055, "../testDir".getBytes());

        FsInode inode = _fs.path2inode("aLink", _rootInode);
        assertEquals("Link resolution did not work", dirInode, inode);

    }

    @Test
    public void testResolveLinkOnPathToIdsRelative() throws Exception {
        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);
        FsInode linkInode = _rootInode.createLink("aLink", 0, 0, 055, "../testDir".getBytes());

        List<FsInode> inodes = _fs.path2inodes("aLink", _rootInode);
        assertEquals("Link resolution did not work",
              Lists.newArrayList(_rootInode, linkInode, _rootInode, dirInode),
              inodes);
    }

    @Test(expected = FileExistsChimeraFsException.class)
    public void testLinkWithExistingName() throws Exception {

        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);
        _rootInode.create("aLink", 0, 0, 055);
        _rootInode.createLink("aLink", 0, 0, 055, "../testDir".getBytes());
    }

    @Test
    public void testResolveLinkOnPathToIdAbsolute() throws Exception {

        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);
        FsInode subdirInode = dirInode.mkdir("testDir2", 0, 0, 0755);
        FsInode linkInode = dirInode.createLink("aLink", 0, 0, 055, "/testDir/testDir2".getBytes());

        FsInode inode = _fs.path2inode("aLink", dirInode);
        assertEquals("Link resolution did not work", subdirInode, inode);
    }

    @Test
    public void testResolveLinkOnPathToIdsAbsolute() throws Exception {
        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);
        FsInode subdirInode = dirInode.mkdir("testDir2", 0, 0, 0755);
        FsInode linkInode = dirInode.createLink("aLink", 0, 0, 055, "/testDir/testDir2".getBytes());

        List<FsInode> inodes = _fs.path2inodes("aLink", dirInode);
        assertEquals("Link resolution did not work",
              Lists.newArrayList(dirInode, linkInode, _rootInode, dirInode, subdirInode),
              inodes);
    }

    @Test
    public void testUpdateCtimeOnSetOwner() throws Exception {
        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);
        long oldCtime = dirInode.stat().getCTime();

        Stat stat = new Stat();
        stat.setUid(3750);
        dirInode.setStat(stat);
        assertTrue("The ctime is not updated", dirInode.stat().getCTime() >= oldCtime);
    }

    @Test
    public void testUpdateCtimeOnSetGroup() throws Exception {
        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);
        long oldCtime = dirInode.stat().getCTime();
        long oldChage = dirInode.stat().getGeneration();

        Stat stat = new Stat();
        stat.setGid(3750);
        dirInode.setStat(stat);
        assertTrue("The ctime is not updated", dirInode.stat().getCTime() >= oldCtime);
        assertTrue("change count is not updated", dirInode.stat().getGeneration() != oldChage);
    }

    @Test
    public void testUpdateCtimeOnSetMode() throws Exception {
        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);
        long oldCtime = dirInode.stat().getCTime();
        long oldChage = dirInode.stat().getGeneration();

        Stat stat = new Stat();
        stat.setMode(0700);
        dirInode.setStat(stat);
        assertTrue("The ctime is not updated", dirInode.stat().getCTime() >= oldCtime);
        assertTrue("change count is not updated", dirInode.stat().getGeneration() != oldChage);
    }

    @Test
    public void testUpdateMtimeOnSetSize() throws Exception {
        FsInode inode = _rootInode.create("file", 0, 0, 0755);
        long oldMtime = inode.stat().getMTime();
        long oldChage = inode.stat().getGeneration();

        Stat stat = new Stat();
        stat.setSize(17);
        inode.setStat(stat);
        assertTrue("The mtime is not updated", inode.stat().getMTime() >= oldMtime);
        assertTrue("change count is not updated", inode.stat().getGeneration() != oldChage);
    }

    @Test
    public void testSetAcl() throws Exception {
        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<>();

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0, AccessMask.ADD_SUBDIRECTORY.getValue(),
              Who.USER, 1001));

        aces.add(
              new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0, AccessMask.ADD_FILE.getValue(), Who.USER,
                    1001));

        _fs.setACL(dirInode, aces);
        List<ACE> l2 = _fs.getACL(dirInode);
        assertEquals(aces, l2);
    }

    @Test
    public void testReSetAcl() throws Exception {
        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<>();

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0,
              AccessMask.ADD_SUBDIRECTORY.getValue(), Who.USER, 1001));

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
              AccessMask.ADD_FILE.getValue(), Who.USER, 1001));

        _fs.setACL(dirInode, aces);
        _fs.setACL(dirInode, new ArrayList<ACE>());
        assertTrue(_fs.getACL(dirInode).isEmpty());
    }

    @Test(expected = FileNotFoundChimeraFsException.class)
    public void testGetInodeByPathNotExist() throws Exception {
        _fs.path2inode("/some/nonexisting/path");
        fail("Expected exception not thrown");
    }

    @Test
    public void testMoveSubdirectory() throws Exception {
        FsInode dir01 = _rootInode.mkdir("dir01", 0, 0, 0755);
        FsInode dir02 = dir01.mkdir("dir02", 0, 0, 0755);
        FsInode dir03 = dir02.mkdir("dir03", 0, 0, 0755);

        FsInode dir11 = _rootInode.mkdir("dir11", 0, 0, 0755);
        FsInode dir12 = dir11.mkdir("dir12", 0, 0, 0755);
        FsInode dir13 = dir12.mkdir("dir13", 0, 0, 0755);

        _fs.rename(dir02, dir01, "dir02", dir13, "dir14");

        FsInode newInode = _fs.inodeOf(dir13, "dir14", NO_STAT);
        assertEquals("Invalid parent", dir13, newInode.inodeOf("..", NO_STAT));
    }

    @Test(expected = NotDirChimeraException.class)
    public void testMoveIntoFile() throws Exception {

        FsInode src = _rootInode.create("testMoveIntoFile1", 0, 0, 0644);
        FsInode dest = _rootInode.create("testMoveIntoFile2", 0, 0, 0644);
        _fs.rename(src, _rootInode, "testMoveIntoFile1", dest, "testMoveIntoFile3");
    }

    @Test(expected = FileExistsChimeraFsException.class)
    public void testMoveIntoDir() throws Exception {

        FsInode src = _rootInode.create("testMoveIntoDir", 0, 0, 0644);
        FsInode dir = _rootInode.mkdir("dir", 0, 0, 0755);
        _fs.rename(src, _rootInode, "testMoveIntoDir", _rootInode, "dir");
    }

    @Test(expected = FileNotFoundChimeraFsException.class)
    public void testMoveNotExists() throws Exception {
        _fs.rename(_rootInode, _rootInode, "foo", _rootInode, "bar");
    }

    @Test(expected = DirNotEmptyChimeraFsException.class)
    public void testMoveNotEmptyDir() throws Exception {

        FsInode dir1 = _rootInode.mkdir("dir1", 0, 0, 0755);
        FsInode dir2 = _rootInode.mkdir("dir2", 0, 0, 0755);
        FsInode src = dir2.create("testMoveIntoDir", 0, 0, 0644);
        _fs.rename(dir1, _rootInode, "dir1", _rootInode, "dir2");
    }

    @Test
    public void testMoveExistingWithLevel() throws Exception {

        FsInode base = _rootInode.mkdir("junit");
        FsInode inode1 = base.create("testCreateFile1", 0, 0, 0644);
        FsInode inode2 = base.create("testCreateFile2", 0, 0, 0644);

        FsInode level1of1 = new FsInode(_fs, inode1.ino(), 1);

        byte[] data = "hello".getBytes();
        level1of1.write(0, data, 0, data.length);
        assertTrue(_fs.rename(inode2, base, "testCreateFile2", base, "testCreateFile1"));

    }

    @Test(expected = InvalidNameChimeraException.class)
    public void testNameTooDirLong() throws Exception {
        String tooLongName = Strings.repeat("a", 257);
        FsInode base = _rootInode.mkdir(tooLongName);
    }

    @Test(expected = InvalidNameChimeraException.class)
    public void testNameTooFileLong() throws Exception {
        String tooLongName = Strings.repeat("a", 257);
        FsInode base = _rootInode.create(tooLongName, 0, 0, 0644);
    }

    @Test(expected = InvalidNameChimeraException.class)
    public void testNameTooMoveLong() throws Exception {
        String tooLongName = Strings.repeat("a", 257);
        FsInode inode = _rootInode.mkdir("testNameTooMoveLong");
        _fs.rename(inode, _rootInode, "testNameTooMoveLong", _rootInode, tooLongName);
    }

    @Test
    public void testTagPropagation() throws ChimeraFsException {

        var tagName = "aTag";

        _fs.createTag(_rootInode, tagName);
        FsInode tagInode = new FsInode_TAG(_fs, _rootInode.ino(), tagName);
        byte[] data = "data".getBytes(UTF_8);
        tagInode.write(0, data, 0, data.length);

        var dir = _fs.mkdir(_rootInode, "dir.0", 0, 0, 0755);
        _fs.statTag(dir, tagName);
    }

    @Test(expected = FileNotFoundChimeraFsException.class)
    public void testStatMissingTag() throws ChimeraFsException {
        var dir = _fs.mkdir(_rootInode, "dir.0", 0, 0, 0755);
        _fs.statTag(dir, "aTag");
    }

    @Test
    public void testTagDisposal() throws ChimeraFsException, SQLException {

        var tagName = "aTag";

        var dir = _fs.mkdir(_rootInode, "dir.0", 0, 0, 0755);
        _fs.createTag(dir, tagName);

        FsInode tagInode = new FsInode_TAG(_fs, dir.ino(), tagName);
        byte[] data = "data".getBytes(UTF_8);
        tagInode.write(0, data, 0, data.length);

        long tagId = tagInode.stat().getIno();

        try (var conn = _dataSource.getConnection()) {
            var found = conn.createStatement().executeQuery("SELECT * FROM t_tags_inodes WHERE itagid="+tagId).next();
            assertTrue("tag inodes is not populated with a new entry", found);
        }

        _fs.remove(_rootInode, "dir.0", dir);

        try (var conn = _dataSource.getConnection()) {
            var found = conn.createStatement().executeQuery("SELECT * FROM t_tags_inodes WHERE itagid="+tagId).next();
            assertFalse("tag is not disposed on last reference removal", found);
        }
    }

    @Test
    public void testChangeTagOwner() throws Exception {

        final String tagName = "myTag";
        FsInode base = _rootInode.mkdir("junit");
        _fs.createTag(base, tagName);
        FsInode tagInode = new FsInode_TAG(_fs, base.ino(), tagName);
        Stat stat = new Stat();
        stat.setUid(1);
        tagInode.setStat(stat);

        assertEquals(1, tagInode.stat().getUid());
    }

    @Test
    public void testChangeTagOwnerGroup() throws Exception {

        final String tagName = "myTag";
        FsInode base = _rootInode.mkdir("junit");
        _fs.createTag(base, tagName);
        FsInode tagInode = new FsInode_TAG(_fs, base.ino(), tagName);
        Stat stat = new Stat();
        stat.setGid(1);
        tagInode.setStat(stat);

        assertEquals(1, tagInode.stat().getGid());
    }

    @Test
    public void testChangeTagMode() throws Exception {

        final String tagName = "myTag";
        FsInode base = _rootInode.mkdir("junit");
        _fs.createTag(base, tagName);
        FsInode tagInode = new FsInode_TAG(_fs, base.ino(), tagName);
        Stat stat = new Stat();
        stat.setMode(0007);
        tagInode.setStat(stat);

        assertEquals(0007 | UnixPermission.S_IFREG, tagInode.stat().getMode());
    }

    @Test
    public void testUpdateTagMtimeOnWrite() throws Exception {

        final String tagName = "myTag";
        final byte[] data1 = "some data".getBytes();
        final byte[] data2 = "some other data".getBytes();

        FsInode base = _rootInode.mkdir("junit");
        _fs.createTag(base, tagName);
        FsInode tagInode = new FsInode_TAG(_fs, base.ino(), tagName);

        tagInode.write(0, data1, 0, data1.length);
        Stat statBefore = tagInode.stat();

        tagInode.write(0, data2, 0, data2.length);
        Stat statAfter = tagInode.stat();

        assertTrue(statAfter.getMTime() >= statBefore.getMTime());
    }

    @Test
    public void testSetAttribitesOnTag() throws Exception {
        final String tagName = "myTag";

        FsInode base = _rootInode.mkdir("junit");
        _fs.createTag(base, tagName);
        FsInode tagInode = new FsInode_TAG(_fs, base.ino(), tagName);

        Stat stat = tagInode.stat();
        Stat baseStat = base.stat();

        stat.setUid(123);
        _fs.setInodeAttributes(tagInode, 0, stat);

        assertEquals(baseStat, base.stat());
    }

    @Test(expected = FileNotFoundChimeraFsException.class)
    public void testGetParentOnRoot() throws Exception {
        String id = _rootInode.statCache().getId();
        _rootInode.inodeOf(".(parent)(" + id + ")", NO_STAT);
    }

    @Test
    @Ignore("See https://github.com/dCache/dcache/issues/7487")
    public void testCreateFileDotUseLevel0ForExistingFile() throws Exception {
        FsInode file = _rootInode.create("normal-file", 1, 2, 0644);

        FsInode level0 = _fs.createFile(_rootInode, ".(use)(0)(normal-file)",
                3, 4, 0755);

        assertThat(level0, notNullValue());
        assertThat(level0, equalTo(file));
        assertThat(level0.getLevel(), equalTo(0));
        assertThat(level0.type(), equalTo(FsInodeType.INODE));

        var fileStat = file.getStatCache();
        var stat = level0.getStatCache();

        assertThat(stat.getUid(), equalTo(1));
        assertThat(stat.getGid(), equalTo(2));
        assertThat(stat.getMode(), equalTo(0644 | UnixPermission.S_IFREG));
        assertThat(stat.getIno(), equalTo(fileStat.getIno()));
        assertThat(stat.getId(), equalTo(fileStat.getId()));
        assertThat(stat.getGeneration(), equalTo(0L));
        assertThat(stat.getSize(), equalTo(0L));
        assertThat(stat.getNlink(), equalTo(1));
        assertThat(stat.getDev(), equalTo(17));
        assertThat(stat.getRdev(), equalTo(13));
    }

    @Test(expected=FileNotFoundChimeraFsException.class)
    public void testCreateFileDotUseLevel0ForMissingFile() throws Exception {
        _fs.createFile(_rootInode, ".(use)(0)(no-such-file)", 0, 0, 0644);
    }

    @Test
    public void testCreateFileDotUseLevel1ForExistingFile() throws Exception {
        FsInode file = _rootInode.create("normal-file", 2, 3, 0644);

        FsInode level0 = _fs.createFile(_rootInode, ".(use)(1)(normal-file)",
                4, 5, 0755);

        assertThat(level0, notNullValue());
        assertThat(level0, not(equalTo(file)));
        assertThat(level0.getLevel(), equalTo(1));
        assertThat(level0.type(), equalTo(FsInodeType.INODE));

        var fileStat = file.getStatCache();
        var stat = level0.getStatCache();

        assertThat(stat.getUid(), equalTo(2));
        assertThat(stat.getGid(), equalTo(3));
        assertThat(stat.getMode(), equalTo(0644 | UnixPermission.S_IFREG));
        assertThat(stat.getIno(), equalTo(fileStat.getIno()));
        assertThat(stat.getGeneration(), equalTo(0L));
        assertThat(stat.getSize(), equalTo(0L));
        assertThat(stat.getNlink(), equalTo(1));
        assertThat(stat.getDev(), equalTo(17));
        assertThat(stat.getRdev(), equalTo(13));
    }

    @Test(expected=FileNotFoundChimeraFsException.class)
    public void testCreateFileDotUseLevel1ForMissingFile() throws Exception {
        _fs.createFile(_rootInode, ".(use)(1)(no-such-file)", 0, 0, 0644);
    }

    @Test
    public void testInodeOfDotIdExistingFile() throws Exception {
        FsInode file = _rootInode.create("normal-file", 1, 2, 0644);

        FsInode id = _fs.inodeOf(_rootInode, ".(id)(normal-file)", STAT);

        assertThat(id, notNullValue());
        assertThat(id, not(equalTo(file)));
        assertThat(id.getLevel(), equalTo(0));
        assertThat(id.type(), equalTo(FsInodeType.ID));

        assertContents(id, file.getId() + "\n");
    }

    @Test(expected=FileNotFoundChimeraFsException.class)
    public void testInodeOfDotIdFileNotExisting() throws Exception {
        _fs.inodeOf(_rootInode, ".(id)(normal-file)", STAT);
    }

    @Test
    public void testInodeOfDotUseLevel0ExistingFile() throws Exception {
        FsInode file = _rootInode.create("normal-file", 1, 2, 0644);

        FsInode use = _fs.inodeOf(_rootInode, ".(use)(0)(normal-file)", STAT);

        assertThat(use, notNullValue());
        assertThat(use, equalTo(file));
        assertThat(use.getLevel(), equalTo(0));
        assertThat(use.type(), equalTo(FsInodeType.INODE));

        var fileStat = file.statCache();
        var stat = use.statCache();

        assertThat(stat.getUid(), equalTo(1));
        assertThat(stat.getGid(), equalTo(2));
        assertThat(stat.getMode(), equalTo(0644 | UnixPermission.S_IFREG));
        assertThat(stat.getIno(), equalTo(fileStat.getIno()));
        assertThat(stat.getId(), equalTo(fileStat.getId()));
        assertThat(stat.getGeneration(), equalTo(0L));
        assertThat(stat.getSize(), equalTo(0L));
        assertThat(stat.getNlink(), equalTo(1));
        assertThat(stat.getDev(), equalTo(17));
        assertThat(stat.getRdev(), equalTo(13));
    }

    @Test(expected=FileNotFoundChimeraFsException.class)
    public void testInodeOfDotUseLevel0NonExistingFile() throws Exception {
        _fs.inodeOf(_rootInode, ".(use)(0)(normal-file)", STAT);
    }

    @Test
    public void testInodeOfDotSuriExistingFile() throws Exception {
        FsInode file = _rootInode.create("normal-file", 1, 2, 0644);
        String location = "hsm://somesystem/target";
        _fs.addInodeLocation(file, StorageGenericLocation.TAPE, location);

        FsInode suri = _fs.inodeOf(_rootInode, ".(suri)(normal-file)", STAT);

        assertThat(suri, notNullValue());
        assertThat(suri, not(equalTo(file)));
        assertThat(suri.getLevel(), equalTo(0));
        assertThat(suri.type(), equalTo(FsInodeType.SURI));

        var fileStat = file.statCache();
        var stat = suri.statCache();

        assertThat(stat.getUid(), equalTo(1));
        assertThat(stat.getGid(), equalTo(2));
        assertThat(stat.getMode(), equalTo(0644 | UnixPermission.S_IFREG));
        assertThat(stat.getIno(), equalTo(fileStat.getIno()));
        assertThat(stat.getId(), equalTo(fileStat.getId()));
        assertThat(stat.getGeneration(), equalTo(fileStat.getGeneration() +1)); // work-around for NFS.
        assertThat(stat.getNlink(), equalTo(1));
        assertThat(stat.getDev(), equalTo(17));
        assertThat(stat.getRdev(), equalTo(13));

        assertContents(suri, location + "\n");
    }

    @Test(expected=FileNotFoundChimeraFsException.class)
    public void testInodeOfDotSuriNonExistingFile() throws Exception {
        _fs.inodeOf(_rootInode, ".(suri)(normal-file)", STAT);
    }

    @Test
    public void testInodeOfDotGetChecksumExistingFileWithNoChecksums() throws Exception {
        FsInode file = _rootInode.create("normal-file", 1, 2, 0644);

        FsInode checksums = _fs.inodeOf(_rootInode, ".(get)(normal-file)(checksums)", STAT);

        assertThat(checksums, notNullValue());
        assertThat(checksums, not(equalTo(file)));
        assertThat(checksums.getLevel(), equalTo(0));
        assertThat(checksums.type(), equalTo(FsInodeType.PCRC));

        var fileStat = file.statCache();
        var stat = checksums.statCache();

        assertThat(stat.getUid(), equalTo(1));
        assertThat(stat.getGid(), equalTo(2));
        assertThat(stat.getMode(), equalTo(0644 | UnixPermission.S_IFREG));
        assertThat(stat.getIno(), equalTo(fileStat.getIno()));
        assertThat(stat.getId(), equalTo(fileStat.getId()));
        assertThat(stat.getGeneration(), equalTo(fileStat.getGeneration()));
        assertThat(stat.getNlink(), equalTo(1));
        assertThat(stat.getDev(), equalTo(17));
        assertThat(stat.getRdev(), equalTo(13));

        assertContents(checksums, "\n\r");
    }

    @Test(expected=FileNotFoundChimeraFsException.class)
    public void testInodeOfDotGetChecksumNonExistingFile() throws Exception {
        _fs.inodeOf(_rootInode, ".(get)(normal-file)(checksums)", STAT);
    }

    @Test
    public void testInodeOfDotConfigExistingFile() throws Exception {
        buildWormhole();
        FsInode worm = _fs.inodeOf(_rootInode, ".(config)", STAT);
        FsInode origCfgFile = worm.create("test.config", 1, 2, 0644);

        FsInode cfgFile = _fs.inodeOf(_rootInode, ".(config)(test.config)", STAT);

        assertThat(cfgFile, notNullValue());
        assertThat(cfgFile, equalTo(origCfgFile));

        var origCfgStat = origCfgFile.statCache();
        var stat = cfgFile.statCache();

        assertThat(stat.getUid(), equalTo(1));
        assertThat(stat.getGid(), equalTo(2));
        assertThat(stat.getMode(), equalTo(0644 | UnixPermission.S_IFREG));
        assertThat(stat.getIno(), equalTo(origCfgStat.getIno()));
        assertThat(stat.getId(), equalTo(origCfgStat.getId()));
        assertThat(stat.getGeneration(), equalTo(origCfgStat.getGeneration()));
        assertThat(stat.getNlink(), equalTo(1));
        assertThat(stat.getDev(), equalTo(17));
        assertThat(stat.getRdev(), equalTo(13));
    }

    @Test(expected=FileNotFoundChimeraFsException.class)
    public void testInodeOfDotConfigNotExistingFile() throws Exception {
        buildWormhole();

        _fs.inodeOf(_rootInode, ".(config)(no-such-file.config)", STAT);
    }

    @Test
    public void testInodeOfDotFsetChecksum() throws Exception {
        FsInode file = _rootInode.create("normal-file", 1, 2, 0644);

        FsInode fset = _fs.inodeOf(_rootInode,
                ".(fset)(normal-file)(checksum)(MD5)(d41d8cd98f00b204e9800998ecf8427e)",
                STAT);

        // simulate the 'touch' command.
        var touchStat = new Stat();
        touchStat.setMTime(System.currentTimeMillis());
        fset.setStat(touchStat);

        var checksums = _fs.getInodeChecksums(file);
        assertThat(checksums, hasSize(1));
        Checksum actual = checksums.iterator().next();
        Checksum expected = new Checksum(ChecksumType.MD5_TYPE,
                "d41d8cd98f00b204e9800998ecf8427e");

        assertThat(actual, equalTo(expected));
    }

    @Test(expected=FileNotFoundChimeraFsException.class)
    public void testInodeOfDotFsetChecksumMissingFile() throws Exception {
        _fs.inodeOf(_rootInode, ".(fset)(missing-file)(checksum)(MD5)(d41d8cd98f00b204e9800998ecf8427e)", STAT);
    }

    @Test
    public void testInodeOfNormalFile() throws Exception {
        FsInode file = _rootInode.create("normal-file", 1, 2, 0644);

        FsInode result = _fs.inodeOf(_rootInode, "normal-file", STAT);

        assertThat(result, notNullValue());
        assertThat(result, equalTo(file));

        var fileStat = file.statCache();
        var stat = result.statCache();

        assertThat(stat.getUid(), equalTo(1));
        assertThat(stat.getGid(), equalTo(2));
        assertThat(stat.getMode(), equalTo(0644 | UnixPermission.S_IFREG));
        assertThat(stat.getIno(), equalTo(fileStat.getIno()));
        assertThat(stat.getId(), equalTo(fileStat.getId()));
        assertThat(stat.getGeneration(), equalTo(fileStat.getGeneration()));
        assertThat(stat.getNlink(), equalTo(1));
        assertThat(stat.getDev(), equalTo(17));
        assertThat(stat.getRdev(), equalTo(13));
    }

    @Test(expected=FileNotFoundChimeraFsException.class)
    public void testInodeOfMissingFile() throws Exception {
        _fs.inodeOf(_rootInode, "missing-file", STAT);
    }

    @Test
    public void testGenerationOnReaddir() throws Exception {
        FsInode inode = _rootInode.mkdir("junit");
        Stat stat = new Stat();
        inode.setStat(stat); // to bump generation
        try (DirectoryStreamB<ChimeraDirectoryEntry> dirStream = _fs.newDirectoryStream(
              _rootInode)) {

            for (ChimeraDirectoryEntry entry : dirStream) {
                if (entry.getName().equals("junit") && entry.getStat().getGeneration() == 1) {
                    return;
                }
            }

            fail();
        }
    }

    private void assertHasChecksum(Checksum expectedChecksum, FsInode inode) throws Exception {
        for (Checksum checksum : _fs.getInodeChecksums(inode)) {
            if (checksum.equals(expectedChecksum)) {
                return;
            }
        }
        fail("No checksums");
    }

    @Test(expected = InvalidNameChimeraException.class)
    public void testDeleteDot() throws Exception {

        FsInode base = _rootInode.mkdir("dir1");
        base.remove(".");
    }

    @Test(expected = InvalidNameChimeraException.class)
    public void testDeleteDotAtEnd() throws Exception {

        FsInode base = _rootInode.mkdir("dir1");
        _fs.remove("/dir1/.");
    }

    @Test(expected = InvalidNameChimeraException.class)
    public void testDeleteDotDot() throws Exception {

        FsInode base = _rootInode.mkdir("dir1");
        base.remove("..");
    }

    @Test(expected = IsDirChimeraException.class)
    public void testSetSizeOnDir() throws Exception {
        FsInode dir = _rootInode.mkdir("dir1");
        Stat stat = new Stat();
        stat.setSize(1);
        dir.setStat(stat);
    }

    @Test(expected = InvalidArgumentChimeraException.class)
    public void testSetSizeOnNonFile() throws Exception {
        FsInode dir = _rootInode.mkdir("dir1");
        FsInode link = _rootInode.createLink("link1", 1, 1, 0777, "dir1".getBytes(UTF_8));
        Stat stat = new Stat();
        stat.setSize(1);
        link.setStat(stat);
    }

    @Test(expected = FileExistsChimeraFsException.class)
    public void testCreateDuplicateTag() throws Exception {
        FsInode dir = _rootInode.mkdir("dir1");
        _fs.createTag(dir, "aTag", 0, 0, 0644);
        _fs.createTag(dir, "aTag", 0, 0, 0644);
    }

    @Test
    public void testPathofUnicodePath() throws ChimeraFsException {
        FsInode file = _rootInode.create("JC385 - 1300C 12h 15X - \uc624\uc5fc.jpg", 0, 0, 0644);
        FsInode_PATHOF pathof = new FsInode_PATHOF(_fs, file.ino());
        byte[] buffer = new byte[64];
        int len = pathof.read(0, buffer, 0, 64);
        assertThat(len, is(equalTo(36)));
    }

    @Test
    public void testPathofOnPartOfUnicodePath() throws ChimeraFsException {
        FsInode file = _rootInode.create("JC385 - 1300C 12h 15X - \uc624\uc5fc.jpg", 0, 0, 0644);
        FsInode_PATHOF pathof = new FsInode_PATHOF(_fs, file.ino());
        byte[] buffer = new byte[12];
        int len = pathof.read(23, buffer, 0, 8);
        assertThat(len, is(8));
        assertArrayEquals(buffer, new byte[]{45, 32, -20, -104, -92, -20, -105, -68, 0, 0, 0, 0});
    }

    @Test
    public void testNameofUnicodePath() throws ChimeraFsException {
        FsInode file = _rootInode.create("JC385 - 1300C 12h 15X - \uc624\uc5fc.jpg", 0, 0, 0644);
        FsInode_NAMEOF nameof = new FsInode_NAMEOF(_fs, file.ino());
        byte[] buffer = new byte[64];
        int len = nameof.read(0, buffer, 0, 64);
        assertThat(len, is(35));
    }

    @Test
    public void testNameofOnPartOfUnicodePath() throws ChimeraFsException {
        FsInode file = _rootInode.create("JC385 - 1300C 12h 15X - \uc624\uc5fc.jpg", 0, 0, 0644);
        FsInode_NAMEOF nameof = new FsInode_NAMEOF(_fs, file.ino());
        byte[] buffer = new byte[12];
        int len = nameof.read(23, buffer, 0, 8);
        assertThat(len, is(8));
        assertArrayEquals(buffer, new byte[]{32, -20, -104, -92, -20, -105, -68, 46, 0, 0, 0, 0});
    }

    @Test
    public void testCreateBlockDev() throws ChimeraFsException {
        _fs.createFile(_rootInode, "aBlockDev", 0, 0, 0644 | UnixPermission.S_IFBLK,
              UnixPermission.S_IFBLK);
    }

    @Test
    public void testCreateCharDev() throws ChimeraFsException {
        _fs.createFile(_rootInode, "aCharDev", 0, 0, 0644 | UnixPermission.S_IFCHR,
              UnixPermission.S_IFCHR);
    }

    @Test
    public void testCreateSocketDev() throws ChimeraFsException {
        _fs.createFile(_rootInode, "aSocket", 0, 0, 0644 | UnixPermission.S_IFSOCK,
              UnixPermission.S_IFSOCK);
    }

    @Test
    public void testCreateFifoDev() throws ChimeraFsException {
        _fs.createFile(_rootInode, "aFifo", 0, 0, 0644 | UnixPermission.S_IFIFO,
              UnixPermission.S_IFIFO);
    }

    @Test
    public void testCreateSymLink() throws ChimeraFsException {
        _fs.createFile(_rootInode, "aSymlink", 0, 0, 0644 | UnixPermission.S_IFLNK,
              UnixPermission.S_IFLNK);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateDir() throws ChimeraFsException {
        _fs.createFile(_rootInode, "aDir", 0, 0, 0755 | UnixPermission.S_IFDIR,
              UnixPermission.S_IFDIR);
    }

    @Test
    public void testLevelCreation() throws ChimeraFsException {

        FsInode file = _fs.createFile(_rootInode, "aFile", 0, 0, 0755 | UnixPermission.S_IFREG,
              UnixPermission.S_IFREG);
        FsInode level = _fs.createFileLevel(file, 2);

        byte[] data = "some random data".getBytes(UTF_8);
        int n = level.write(0, data, 0, data.length);
        assertEquals("incorrect number of bytes", data.length, n);

        byte[] moreData = "some more random data".getBytes(UTF_8);
        n = level.write(0, moreData, 0, moreData.length);
        assertEquals("incorrect number of bytes", moreData.length, n);

    }

    @Test
    public void testTagDeletionOnDirectoryRemove() throws ChimeraFsException, SQLException {

        FsInode top = _rootInode.mkdir("top");
        _fs.createTag(top, "aTag");

        FsInode tagInode = new FsInode_TAG(_fs, top.ino(), "aTag");
        byte[] data = "data".getBytes(UTF_8);
        tagInode.write(0, data, 0, data.length);

        long tagid = tagInode.stat().getIno();
        assertEquals("Tag ref count mismatch", 1, tagInode.stat().getNlink());

        FsInode sub = top.mkdir("sub");
        _fs.remove(top, "sub", sub);
        _fs.remove(_rootInode, "top", top);

        // as we don't have a way to access tags, use direct SQL
        try (Connection c = _dataSource.getConnection()) {
            ResultSet rs = c.createStatement()
                  .executeQuery("SELECT * from t_tags where itagid=" + tagid);
            // on last remove tag must be gone
            assertFalse("Tag is not garbage collected on last remove", rs.next());
        }
    }

    @Test
    public void testPushTag() throws Exception {

        FsInode top = _rootInode.mkdir("top");
        String tagName = "aTag";

        FsInode d = top;
        for (int i = 0; i < 10; i++) {
            d = d.mkdir(Integer.toString(i));
        }

        byte[] tagData = "some random tag".getBytes(UTF_8);
        _fs.createTag(top, tagName);
        _fs.setTag(top, tagName, tagData, 0, tagData.length);

        assertArrayEquals(new String[0], _fs.tags(d));

        _fs.pushTag(top, tagName);
        assertArrayEquals(new String[]{tagName}, _fs.tags(d));
    }

    @Test
    public void testTashTimestampOnRemove() throws Exception {
        final String name = "testTashTimestampOnRemove";
        FsInode inode = _rootInode.create(name, 0, 0, 0644);
        String id = inode.getId();

        // ensure location to get entry in the trash table
        _fs.addInodeLocation(inode, 1, "aPool");
        JdbcTemplate jdbc = new JdbcTemplate(_dataSource);

        // wind back timestamp
        Instant day0 = Instant.parse("2000-09-16T09:00:00.00Z"); // dCache birth day
        jdbc.update("update t_locationinfo set ictime=?",
              ps -> ps.setTimestamp(1, Timestamp.from(day0)));

        _fs.remove(_rootInode, name, inode);

        Timestamp ctime = jdbc.query(
              "SELECT * from t_locationinfo_trash where ipnfsid=? and itype=1",
              ps -> ps.setString(1, id),
              rs -> rs.next() ? rs.getTimestamp("ictime") : null);

        assertNotNull("No entries in trash table", ctime);

        // as timestamp depends on thread execution and, and, and... differce is
        // couple of minutes.

        Instant removeTime = ctime.toInstant();
        Duration diff = Duration.between(removeTime, Instant.now()).abs();
        assertTrue(diff.toMinutes() < 2);
    }

    @Test
    public void testOverflowOnJomboFiles() throws ChimeraFsException {

        Stat stat = new Stat();
        stat.setSize(Long.MAX_VALUE);

        FsInode file1 = _fs.createFile(_rootInode, "file1", 0, 0, 0644);
        file1.setStat(stat);
        FsInode file2 = _fs.createFile(_rootInode, "file2", 0, 0, 0644);
        file2.setStat(stat);

        FsStat fsStat = _fs.getFsStat();
        assertThat(fsStat.getUsedFiles(), greaterThan(0L));
    }

    @Test
    public void testFsStat() throws ChimeraFsException {

        FsStat fsStat = _fs.getFsStat();
        assertThat(fsStat.getUsedSpace(), is(1048576L));
        assertThat(fsStat.getUsedFiles(), is(1048576L));
    }

    @Test
    public void testSetGetXattr() throws Exception {

        FsInode dir = _fs.mkdir("/test");
        FsInode inode = _fs.createFile(dir, "aFile");

        String key = "attr1";
        byte[] value = "cat".getBytes(UTF_8);
        Stat s0 = _fs.stat(inode);
        _fs.setXattr(inode, key, value, SetXattrMode.CREATE);
        assertThat("inode generate must be update on xattr create",
              _fs.stat(inode).getGeneration(), greaterThan(s0.getGeneration()));
        byte[] result = _fs.getXattr(inode, key);

        assertArrayEquals("Get xattr returns unexpected value", value, result);
    }


    @Test
    public void testGenerationVirtualdir() throws Exception {
        FsInode dir = _rootInode.mkdir("parent");
        String parentDirName = _fs.inode2path(dir);

        String labelname = "cat";

        FsInode inodeA = _fs.createFile(dir, "aFile");
        FsInode inodeB = _fs.createFile(dir, "bFile");
        FsInode inodeC = _fs.createFile(dir, "cFile");

        String labelnameCat = "cat";
        String labelnameDog = "dog";

        _fs.addLabel(inodeA, labelnameCat);
        _fs.addLabel(inodeB, labelnameCat);
        _fs.addLabel(inodeC, labelnameDog);

        List<String> fileNames = new ArrayList<>();

        try (DirectoryStreamB<ChimeraDirectoryEntry> dirStream = _rootInode.virtualDirectoryStream(
              labelname)) {

            for (ChimeraDirectoryEntry entry : dirStream) {
                fileNames.add(entry.getInode().getId());
            }
        }
        assertTrue(fileNames.containsAll(Lists.newArrayList(inodeA.getId(), inodeB.getId())));
    }

    @Test
    public void testLabelDoesNotExist() throws Exception {

        FsInode dir = _fs.mkdir("/test");

        FsInode inodeA = _fs.createFile(dir, "aFile");
        FsInode inodeB = _fs.createFile(dir, "bFile");
        FsInode inodeC = _fs.createFile(dir, "cFile");

        String labelnameCat = "cat";
        String labelnameDog = "dog";

        _fs.addLabel(inodeA, labelnameCat);
        _fs.addLabel(inodeB, labelnameCat);
        _fs.addLabel(inodeC, labelnameDog);

        DirectoryStreamB<ChimeraDirectoryEntry> dirStream = _rootInode.virtualDirectoryStream(
              "yellow");

        assertEquals("Unexpected number of labels", 0, dirStream.stream().count());

    }


    @Test
    public void testRemoveLabel() throws Exception {

        FsInode dir = _fs.mkdir("/test");

        FsInode inodeA = _fs.createFile(dir, "aFile");
        FsInode inodeB = _fs.createFile(dir, "bFile");
        FsInode inodeC = _fs.createFile(dir, "cFile");

        String labelnameCat = "cat";
        String labelnameDog = "dog";

        _fs.addLabel(inodeA, labelnameCat);
        _fs.addLabel(inodeB, labelnameCat);
        _fs.addLabel(inodeC, labelnameCat);
        _fs.addLabel(inodeC, labelnameDog);

        _fs.removeLabel(inodeC, labelnameCat);

        DirectoryStreamB<ChimeraDirectoryEntry> dirStream = _rootInode.virtualDirectoryStream(
              labelnameCat);

        assertEquals("Unexpected number of labels", 2, dirStream.stream().count());

    }


    @Test
    public void testRemoveLabelsFromAllFiles() throws Exception {

        FsInode dir = _fs.mkdir("/test");

        FsInode inodeA = _fs.createFile(dir, "aFile");
        FsInode inodeB = _fs.createFile(dir, "bFile");
        FsInode inodeC = _fs.createFile(dir, "cFile");

        String labelnameCat = "cat";
        String labelnameDog = "dog";

        _fs.addLabel(inodeA, labelnameCat);
        _fs.addLabel(inodeB, labelnameCat);
        _fs.addLabel(inodeC, labelnameCat);
        _fs.addLabel(inodeC, labelnameDog);

        _fs.removeLabel(inodeC, labelnameCat);
        _fs.removeLabel(inodeB, labelnameCat);
        _fs.removeLabel(inodeA, labelnameCat);

        DirectoryStreamB<ChimeraDirectoryEntry> dirStream = _rootInode.virtualDirectoryStream(
              labelnameCat);

        assertEquals("Unexpected number of labels", 0, dirStream.stream().count());
        assertTrue("Unexpected label", _fs.getLabels(inodeC).iterator().next().equals("dog"));


    }

    @Test
    public void testGetLabels() throws Exception {

        FsInode dir = _fs.mkdir("/test");
        FsInode inode = _fs.createFile(dir, "aFile");

        String[] labels = {"cat", "dog", "yellow", "green"};

        for (String labelName : labels) {
            _fs.addLabel(inode, labelName);
        }

        Collection<String> labelsSet = _fs.getLabels(inode);

        assertEquals("Unexpected number of attributes", 4, labelsSet.size());
        assertThat("List labels without order", labelsSet, containsInAnyOrder(labels));


    }

    @Test
    public void testaddLabelsExist() throws Exception {

        FsInode dir = _fs.mkdir("/test");

        FsInode inodeA = _fs.createFile(dir, "aFile");
        FsInode inodeB = _fs.createFile(dir, "bFile");

        String labelnameCat = "cat";

        _fs.addLabel(inodeA, labelnameCat);
        _fs.addLabel(inodeB, labelnameCat);
        _fs.addLabel(inodeA, labelnameCat);

        DirectoryStreamB<ChimeraDirectoryEntry> dirStream = _rootInode.virtualDirectoryStream(
              labelnameCat);

        assertEquals("Unexpected number of labels", 2, dirStream.stream().count());
    }

    @Test
    public void testAddLabels() throws Exception {

        FsInode dir = _fs.mkdir("/test");

        FsInode inodeA = _fs.createFile(dir, "aFile");
        FsInode inodeB = _fs.createFile(dir, "bFile");
        FsInode inodeC = _fs.createFile(dir, "cFile");

        String labelnameCat = "cat";
        String labelnameDog = "dog";

        _fs.addLabel(inodeA, labelnameCat);
        _fs.addLabel(inodeB, labelnameCat);
        _fs.addLabel(inodeC, labelnameDog);

        DirectoryStreamB<ChimeraDirectoryEntry> dirStream = _rootInode.virtualDirectoryStream(
              labelnameCat);

        assertEquals("Unexpected number of labels", 2, dirStream.stream().count());
        assertTrue("Unexpected labels",
              _fs.getLabels(inodeA).contains("cat") && _fs.getLabels(inodeB).contains("cat"));

    }

    @Test
    public void testAddLabelsSameFileDiffLabels() throws Exception {

        FsInode dir = _fs.mkdir("/test");

        FsInode inodeA = _fs.createFile(dir, "aFile");
        FsInode inodeB = _fs.createFile(dir, "bFile");
        FsInode inodeC = _fs.createFile(dir, "cFile");

        String labelnameCat = "cat";
        String labelnameDog = "dog";

        _fs.addLabel(inodeA, labelnameCat);
        _fs.addLabel(inodeB, labelnameCat);
        _fs.addLabel(inodeC, labelnameDog);
        _fs.addLabel(inodeA, labelnameDog);


        DirectoryStreamB<ChimeraDirectoryEntry> dirStream = _rootInode.virtualDirectoryStream(
              labelnameDog);

        assertEquals("Unexpected number of labels", 2, dirStream.stream().count());
        assertTrue("Unexpected labels",
              _fs.getLabels(inodeA).contains("cat") && _fs.getLabels(inodeA).contains("dog"));

    }

    @Test(expected = FileExistsChimeraFsException.class)
    public void testExclusiveCreateXattr() throws Exception {

        FsInode dir = _fs.mkdir("/test");
        FsInode inode = _fs.createFile(dir, "aFile");

        String key = "attr1";
        byte[] value = "cat".getBytes(UTF_8);
        _fs.setXattr(inode, key, value, SetXattrMode.CREATE);
        _fs.setXattr(inode, key, value, SetXattrMode.CREATE);
    }

    @Test(expected = NoXdataChimeraException.class)
    public void testUpdateOnlyXattr() throws Exception {

        FsInode dir = _fs.mkdir("/test");
        FsInode inode = _fs.createFile(dir, "aFile");

        String key = "attr1";
        byte[] value = "cat".getBytes(UTF_8);
        _fs.setXattr(inode, key, value, SetXattrMode.REPLACE);
    }

    @Test
    public void testUpdateExistingXattr() throws Exception {

        FsInode dir = _fs.mkdir("/test");
        FsInode inode = _fs.createFile(dir, "aFile");

        String key = "attr1";
        byte[] value1 = "cat".getBytes(UTF_8);
        byte[] value2 = "cat2".getBytes(UTF_8);
        _fs.setXattr(inode, key, value1, SetXattrMode.CREATE);
        Stat s0 = _fs.stat(inode);
        _fs.setXattr(inode, key, value2, SetXattrMode.REPLACE);
        assertThat("inode generation must be update on xattr replace",
              _fs.stat(inode).getGeneration(), greaterThan(s0.getGeneration()));

        byte[] result = _fs.getXattr(inode, key);

        assertArrayEquals("Get xattr returns unexpected value", value2, result);
    }

    @Test
    public void testUpdateOrCreateXattr() throws Exception {

        FsInode dir = _fs.mkdir("/test");
        FsInode inode = _fs.createFile(dir, "aFile");

        String key = "attr1";
        byte[] value1 = "cat".getBytes(UTF_8);
        byte[] value2 = "cat2".getBytes(UTF_8);
        _fs.setXattr(inode, key, value1, SetXattrMode.EITHER);
        byte[] result = _fs.getXattr(inode, key);

        assertArrayEquals("Get xattr returns unexpected value", value1, result);

        Stat s0 = _fs.stat(inode);
        _fs.setXattr(inode, key, value2, SetXattrMode.EITHER);
        assertThat("inode generation must be update on xattr create/replace",
              _fs.stat(inode).getGeneration(), greaterThan(s0.getGeneration()));

        result = _fs.getXattr(inode, key);

        assertArrayEquals("Get xattr returns unexpected value", value2, result);
    }

    @Test(expected = NoXdataChimeraException.class)
    public void testGetXattrNoSet() throws Exception {

        FsInode dir = _fs.mkdir("/test");
        FsInode inode = _fs.createFile(dir, "aFile");

        String key = "attr1";
        _fs.getXattr(inode, key);
    }

    @Test
    public void testListXattrNoAttrs() throws Exception {

        FsInode dir = _fs.mkdir("/test");
        FsInode inode = _fs.createFile(dir, "aFile");

        Collection<String> xattrs = _fs.listXattrs(inode);
        assertTrue("Unexpected attributed by newly created file", xattrs.isEmpty());
    }

    @Test
    public void testListXattrAfterSet() throws Exception {

        FsInode dir = _fs.mkdir("/test");
        FsInode inode = _fs.createFile(dir, "aFile");

        String key = "attr1";
        byte[] value = "cat".getBytes(UTF_8);
        _fs.setXattr(inode, key, value, SetXattrMode.CREATE);

        Collection<String> xattrs = _fs.listXattrs(inode);

        assertEquals("Unexpected number of attributes", 1, xattrs.size());
    }

    @Test(expected = NoXdataChimeraException.class)
    public void testRemoveXattrNoAttrs() throws Exception {

        FsInode dir = _fs.mkdir("/test");
        FsInode inode = _fs.createFile(dir, "aFile");
        String key = "attr1";
        _fs.removeXattr(inode, key);
    }

    @Test
    public void testRemoveXattrAfterSet() throws Exception {

        FsInode dir = _fs.mkdir("/test");
        FsInode inode = _fs.createFile(dir, "aFile");
        String key = "attr1";
        byte[] value = "cat".getBytes(UTF_8);
        _fs.setXattr(inode, key, value, SetXattrMode.CREATE);
        Stat s0 = _fs.stat(inode);
        _fs.removeXattr(inode, key);
        assertThat("inode generation must be update on xattr remote",
              _fs.stat(inode).getGeneration(), greaterThan(s0.getGeneration()));
    }

    private long getDirEntryCount(FsInode dir) throws IOException {
        try (var s = _fs.newDirectoryStream(dir)) {
            return s.stream().count();
        }
    }

    private void buildWormhole() throws ChimeraFsException {
        _fs.mkdir("/admin");
        _fs.mkdir("/admin/etc");
        _fs.mkdir("/admin/etc/config");
    }

    private void assertContents(FsInode inode, String expectedContents)
            throws ChimeraFsException {
        var stat = inode.statCache();

        long expectedSize = expectedContents.getBytes(UTF_8).length;

        assertThat(stat.getSize(), equalTo(expectedSize));

        int requestedReadSize = (int)expectedSize;
        byte[] data = new byte[requestedReadSize];
        int actualSize = inode.read(0, data, 0, requestedReadSize);
        assertThat(actualSize, equalTo(requestedReadSize));
        String actualContents = new String(data, UTF_8);
        assertThat(actualContents, equalTo(expectedContents));
    }
}

package org.dcache.chimera;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;

import org.dcache.acl.ACE;
import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.AceType;
import org.dcache.acl.enums.RsType;
import org.dcache.acl.enums.Who;
import org.dcache.chimera.posix.Stat;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class BasicTest extends ChimeraTestCaseHelper {

    @Test
    public void testLevelRemoveOnDelete() throws Exception {
        final int level = 1;
        FsInode inode = _rootInode.create("testLevelRemoveOnDelete", 0, 0, 0644);
        _fs.createFileLevel(inode, level);
        FsInode levelInode = new FsInode(_fs, inode.toString(), level);
        assertTrue(levelInode.exists());

        _fs.remove(_rootInode, "testLevelRemoveOnDelete");
        levelInode = new FsInode(_fs, inode.toString(), level);
        assertFalse(levelInode.exists());
    }

    @Test
    public void testLs() throws Exception {

        List<HimeraDirectoryEntry> list = DirectoryStreamHelper.listOf(_rootInode);
        assertTrue("Root Dir cant be empty", list.size() > 0);

    }

    @Test
    public void testMkDir() throws Exception {

        Stat stat = _rootInode.stat();
        FsInode newDir = _rootInode.mkdir("junit");

        assertEquals("mkdir have to incrise parent's nlink count by one",
                _rootInode.stat().getNlink(), stat.getNlink() + 1);
        assertTrue("mkdir have to update parent's mtime", _rootInode.stat().getMTime() > stat.getMTime());
        assertEquals("new dir should have link count equal to two", newDir.stat().getNlink(), 2);
        assertTrue("change count is not updated", stat.getGeneration() != _rootInode.stat().getGeneration());
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
        FsInode dir1 = _fs.mkdir(_rootInode, "junit", 1, 2, 02755, ImmutableMap.of("tag", bytes));
        assertThat(_fs.getAllTags(dir1), hasEntry("tag", bytes));
    }

    @Test
    public void testCreateFile() throws Exception {
        FsInode base = _rootInode.mkdir("junit");
        Stat stat = base.stat();

        Thread.sleep(1); // required to ensure file created in distinct millisecond

        FsInode newFile = base.create("testCreateFile", 0, 0, 0644);

        assertEquals("file creation has to increase parent's nlink count by one",
                base.stat().getNlink(), stat.getNlink() + 1);
        assertTrue("file creation has to update parent's mtime", base.stat().getMTime() > stat.getMTime());
        assertEquals("new file should have link count equal to one", newFile.stat().getNlink(), 1);
        assertTrue("change count is not updated", stat.getGeneration() != base.stat().getGeneration());
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

    @Test(expected=DirNotEmptyHimeraFsException.class)
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

        base.remove("testCreateFile");

        assertEquals("remove have to decrease parents link count", base.stat().getNlink(), stat.getNlink() - 1);
        assertFalse("remove have to update parent's mtime", stat.getMTime() == base.stat().getMTime());

    }

    @Test
    public void testDeleteDir() throws Exception {

        FsInode base = _rootInode.mkdir("junit");

        base.create("testCreateDir", 0, 0, 0644);
        Stat stat = base.stat();

        base.remove("testCreateDir");

        assertEquals("remove have to decrease parents link count", base.stat().getNlink(), stat.getNlink() - 1);
        assertFalse("remove have to update parent's mtime", stat.getMTime() == base.stat().getMTime());

    }

    @Test(expected=FileNotFoundHimeraFsException.class)
    public void testDeleteNonExistingFile() throws Exception {

        _rootInode.remove("testCreateFile");
    }

    @Test(expected = FileNotFoundHimeraFsException.class)
    public void testDeleteNonExistingDir() throws Exception {
        FsInode missingDir = new FsInode(_fs);
        _fs.remove(missingDir, "aFile");
    }

    @Test(expected = FileNotFoundHimeraFsException.class)
    public void testCreateInNonExistingDir() throws Exception {
        FsInode missingDir = new FsInode(_fs);
        _fs.createFile(missingDir, "aFile");
    }

    @Test
    public void testDeleteInFile() throws Exception {

        FsInode fileInode = _rootInode.create("testCreateFile", 0, 0, 0644);
        try {
            fileInode.remove("anObject");
            fail("you can't remove an  object in file Inode");
        } catch (IOHimeraFsException ioe) {
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
    private final static int PARALLEL_THREADS_COUNT = 10;

    /**
     *
     * Helper class.
     *
     * the idea behind is to run a test in parallel threads.
     * number of running thread defined by <i>PARALLEL_THREADS_COUNT</i>
     * constant.
     *
     * The concept of a parallel test is following:
     *
     * a test have two <i>CountDownLatch</i> - readyToStart and testsReady.
     * <i>readyToStart</i> used to synchronize tests
     * <i>testsReady</i> used to notify test that all test are done;
     *
     */
    private static class ParallelCreateTestRunnerThread extends Thread {

        /**
         * tests root directory
         */
        private final FsInode _testRoot;
        /**
         * have to be counted down as soon as thread is done it's job
         */
        private final CountDownLatch _ready;
        /**
         * wait for 'Go'
         */
        private final CountDownLatch _waitingToStart;

        /**
         *
         * @param name of the thread
         * @param testRoot root directory of the test
         * @param ready tests ready count down
         * @param waitingToStart wait for 'Go'
         */
        public ParallelCreateTestRunnerThread(String name, FsInode testRoot, CountDownLatch ready, CountDownLatch waitingToStart) {
            super(name);
            _testRoot = testRoot;
            _ready = ready;
            _waitingToStart = waitingToStart;
        }

        @Override
        public void run() {
            try {
                _waitingToStart.await();
                _testRoot.create(Thread.currentThread().getName(), 0, 0, 0644);
            } catch (Exception hfe) {
                // FIXME
            } finally {
                _ready.countDown();
            }
        }
    }

    @Test
    public void testParallelCreate() throws Exception {

        FsInode base = _rootInode.mkdir("junit");
        Stat stat = base.stat();

        CountDownLatch readyToStart = new CountDownLatch(PARALLEL_THREADS_COUNT);
        CountDownLatch testsReady = new CountDownLatch(PARALLEL_THREADS_COUNT);

        for (int i = 0; i < PARALLEL_THREADS_COUNT; i++) {

            new ParallelCreateTestRunnerThread("TestRunner" + i, base, testsReady, readyToStart).start();
            readyToStart.countDown();
        }

        testsReady.await();
        assertEquals("new dir should have link count equal to two", base.stat().getNlink(), stat.getNlink() + PARALLEL_THREADS_COUNT);

    }

    @Test
    public void testHardLink() throws Exception {

        FsInode base = _rootInode.mkdir("junit");

        FsInode fileInode = base.create("hardLinkTestSourceFile", 0, 0, 0644);

        Stat stat = fileInode.stat();

        FsInode hardLinkInode = _fs.createHLink(base, fileInode, "hardLinkTestDestinationFile");

        assertEquals("hard link's  have to increase link count by one", stat.getNlink() + 1, hardLinkInode.stat().getNlink());

        _fs.remove(base, "hardLinkTestDestinationFile");
        assertTrue("removing of hard link have to decrease link count by one", 1 == fileInode.stat().getNlink());

    }

    @Test
    public void testRemoveLinkToDir() throws Exception {

        FsInode base = _rootInode.mkdir("junit");

        _fs.createLink(base, "aLink", "/junit");
    }

    @Test
    public void testRemoveLinkToSomewhare() throws Exception {

        FsInode linkBase = _rootInode.mkdir("links");

        _fs.createLink(linkBase, "file123", 0, 0, 0644, "/files/file123".getBytes(Charsets.UTF_8));
        _fs.remove("/links/file123");
    }

    @Test
    public void testRemoveLinkToFile() throws Exception {

        FsInode fileBase = _rootInode.mkdir("files");
        FsInode linkBase = _rootInode.mkdir("links");
        FsInode fileInode = fileBase.create("file123", 0, 0, 0644);

        _fs.createLink(linkBase, "file123", 0, 0, 0644, "/files/file123".getBytes(Charsets.UTF_8));
        _fs.remove("/links/file123");

        assertTrue("original file is gone!", fileInode.exists());
    }

    @Ignore("broken test, normal filesystems do not allow directory hard-links. Why does chimera?")
    @Test
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

        boolean ok = _fs.move(base, "testCreateFile", base, "testCreateFile2");



        assertTrue("can't move", ok);
        assertEquals("link count of base directory should not be modified in case of rename", preStatBase.getNlink(), base.stat().getNlink());
        assertEquals("link count of file shold not be modified in case of rename", preStatFile.getNlink(), fileInode.stat().getNlink());

    }

    @Test
    public void testRenameExistSameDir() throws Exception {

        FsInode base = _rootInode.mkdir("junit");
        FsInode fileInode = base.create("testCreateFile", 0, 0, 0644);
        FsInode file2Inode = base.create("testCreateFile2", 0, 0, 0644);

        Stat preStatBase = base.stat();

        boolean ok = _fs.move(base, "testCreateFile", base, "testCreateFile2");

        assertTrue("can't move", ok);
        assertEquals("link count of base directory should decrease by one", preStatBase.getNlink() - 1, base.stat().getNlink());

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

        boolean ok = _fs.move(base, "testCreateFile", base2, "testCreateFile2");

        assertTrue("can't move", ok);
        assertEquals("link count of source directory should decrese on move out", preStatBase.getNlink() - 1, base.stat().getNlink());
        assertEquals("link count of destination directory should increase on move in", preStatBase2.getNlink() + 1, base2.stat().getNlink());
        assertEquals("link count of file shold not be modified on move", preStatFile.getNlink(), fileInode.stat().getNlink());

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

        boolean ok = _fs.move(base, "testCreateFile", base2, "testCreateFile2");

        assertTrue("can't move", ok);
        assertEquals("link count of source directory should decrese on move out", preStatBase.getNlink() - 1, base.stat().getNlink());
        assertEquals("link count of destination directory should not be modified on replace", preStatBase2.getNlink(), base2.stat().getNlink());
        assertEquals("link count of file shold not be modified on move", preStatFile.getNlink(), fileInode.stat().getNlink());

        assertFalse("ghost file", fileInode2.exists());
    }

    @Test
    public void testRenameHardLinkToItselfSameDir() throws Exception {


        FsInode base = _rootInode.mkdir("junit");
        FsInode fileInode = base.create("testCreateFile", 0, 0, 0644);
        FsInode linkInode = _fs.createHLink(base, fileInode, "testCreateFile2");

        Stat preStatBase = base.stat();
        Stat preStatFile = fileInode.stat();

        boolean ok = _fs.move(base, "testCreateFile", base, "testCreateFile2");

        assertFalse("rename of hardlink to itself should do nothing", ok);
        assertEquals("link count of base directory should not be modified in case of rename", preStatBase.getNlink(), base.stat().getNlink());
        assertEquals("link count of file should not be modified in case of rename", preStatFile.getNlink(), fileInode.stat().getNlink());

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

        boolean ok = _fs.move(base, "testCreateFile", base2, "testCreateFile2");

        assertFalse("rename of hardlink to itself should do nothing", ok);
        assertEquals("link count of source directory should not be modified in case of rename", preStatBase.getNlink(), base.stat().getNlink());
        assertEquals("link count of destination directory should not be modified in case of rename", preStatBase2.getNlink(), base2.stat().getNlink());
        assertEquals("link count of file should not be modified in case of rename", preStatFile.getNlink(), fileInode.stat().getNlink());

    }

    @Test(expected=FileNotFoundHimeraFsException.class)
    public void testRemoveNonexistgById() throws Exception {

        FsInode inode = new FsInode(_fs, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
        _fs.remove(inode);
    }

    @Test(expected=FileNotFoundHimeraFsException.class)
    public void testRemoveNonexistgByPath() throws Exception {
        FsInode base = _rootInode.mkdir("junit");
        _fs.remove(base, "notexist");
    }

    @Test
    public void testAddLocationForNonexistong() throws Exception {
        FsInode inode = new FsInode(_fs, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
        try {
            _fs.addInodeLocation(inode, StorageGenericLocation.DISK, "/dev/null");
            fail("was able to add cache location for non existing file");
        } catch (FileNotFoundHimeraFsException e) {
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

    @Ignore("Functionality not yet written, but desired")
    @Test
    public void testSetSizeNotExist() throws Exception {

        FsInode inode = new FsInode(_fs, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
        try {
            _fs.setFileSize(inode, 0);
            fail("was able set size for non existing file");
        } catch (FileNotFoundHimeraFsException e) {
            // OK
        }
    }

    @Ignore("Functionality not yet written, but desired")
    @Test
    public void testChowneNotExist() throws Exception {

        FsInode inode = new FsInode(_fs, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
        try {
            _fs.setFileOwner(inode, 3750);
            fail("was able set owner for non existing file");
        } catch (FileNotFoundHimeraFsException e) {
            // OK
        }
    }

    @Test
    public void testUpdateLevelNotExist() throws Exception {
        FsInode inode = new FsInode(_fs, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
        try {
            byte[] data = "bla".getBytes();
            _fs.write(inode, 1, 0, data, 0, data.length);
            fail("was able to update level of non existing file");
        } catch (FileNotFoundHimeraFsException e) {
            // OK
        }
    }

    @Test
    public void testUpdateChecksumNotExist() throws Exception {
        FsInode inode = new FsInode(_fs, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
        try {
            _fs.setInodeChecksum(inode, 1, "asum");
            fail("was able to update checksum of non existing file");
        } catch (FileNotFoundHimeraFsException e) {
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
    public void testUpdateALNonExist() throws Exception {
        FsInode inode = new FsInode(_fs, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
        try {
            _fs.setAccessLatency(inode, AccessLatency.ONLINE);
            fail("was able to update access latency of non existing file");
        } catch (FileNotFoundHimeraFsException e) {
            // OK
        }
    }

    @Test
    public void testUpdateALExist() throws Exception {
        FsInode base = _rootInode.mkdir("junit");
        FsInode inode = base.create("testCreateFile", 0, 0, 0644);

        _fs.setAccessLatency(inode, AccessLatency.ONLINE);
    }

    @Test
    public void testChangeALExist() throws Exception {
        FsInode base = _rootInode.mkdir("junit");
        FsInode inode = base.create("testCreateFile", 0, 0, 0644);

        _fs.setAccessLatency(inode, AccessLatency.ONLINE);
        _fs.setAccessLatency(inode, AccessLatency.NEARLINE);
    }

    @Test
    public void testUpdateRPNonExist() throws Exception {
        FsInode inode = new FsInode(_fs, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
        try {
            _fs.setRetentionPolicy(inode, RetentionPolicy.CUSTODIAL);
            fail("was able to update retention policy of non existing file");
        } catch (FileNotFoundHimeraFsException e) {
            // OK
        }
    }

    @Test
    public void testUpdateRPExist() throws Exception {
        FsInode base = _rootInode.mkdir("junit");
        FsInode inode = base.create("testCreateFile", 0, 0, 0644);

        _fs.setRetentionPolicy(inode, RetentionPolicy.CUSTODIAL);
    }

    @Test
    public void testChangeRPExist() throws Exception {
        FsInode base = _rootInode.mkdir("junit");
        FsInode inode = base.create("testCreateFile", 0, 0, 0644);

        _fs.setRetentionPolicy(inode, RetentionPolicy.CUSTODIAL);
        _fs.setRetentionPolicy(inode, RetentionPolicy.OUTPUT);
    }

    @Test
    public void testResolveLinkOnPathToId() throws Exception {

        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);
        FsInode linkInode = _rootInode.createLink("aLink", 0, 0, 055, "testDir".getBytes());

        FsInode inode = _fs.path2inode("aLink", _rootInode);
        assertEquals("Link resolution did not work", dirInode, inode);

    }

    @Test
    public void testResolveLinkOnPathToIds() throws Exception
    {
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
    public void testResolveLinkOnPathToIdsRelative() throws Exception
    {
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
    public void testResolveLinkOnPathToIdsAbsolute() throws Exception
    {
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

        TimeUnit.MILLISECONDS.sleep(2);
        dirInode.setUID(3750);
        assertTrue("The ctime is not updated", dirInode.stat().getCTime() > oldCtime);
    }

    @Test
    public void testUpdateCtimeOnSetGroup() throws Exception {
        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);
        long oldCtime = dirInode.stat().getCTime();
        long oldChage = dirInode.stat().getGeneration();

        TimeUnit.MILLISECONDS.sleep(2);
        dirInode.setGID(3750);
        assertTrue("The ctime is not updated", dirInode.stat().getCTime() > oldCtime);
        assertTrue("change count is not updated", dirInode.stat().getGeneration() != oldChage);
    }

    @Test
    public void testUpdateCtimeOnSetMode() throws Exception {
        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);
        long oldCtime = dirInode.stat().getCTime();
        long oldChage = dirInode.stat().getGeneration();

        TimeUnit.MILLISECONDS.sleep(2);
        dirInode.setMode(0700);
        assertTrue("The ctime is not updated", dirInode.stat().getCTime() > oldCtime);
        assertTrue("change count is not updated", dirInode.stat().getGeneration() != oldChage);
    }

    @Test
    public void testUpdateMtimeOnSetSize() throws Exception {
        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);
        long oldMtime = dirInode.stat().getMTime();
        long oldChage = dirInode.stat().getGeneration();

        TimeUnit.MILLISECONDS.sleep(2);
        dirInode.setSize(17);
        assertTrue("The mtime is not updated", dirInode.stat().getMTime() > oldMtime);
        assertTrue("change count is not updated", dirInode.stat().getGeneration() != oldChage);
    }

    @Test
    public void testSetAcl() throws Exception {
        FsInode dirInode = _rootInode.mkdir("testDir", 0, 0, 0755);


        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<>();

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0,
                AccessMask.ADD_SUBDIRECTORY.getValue(), Who.USER, 1001,
                ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.ADD_FILE.getValue(), Who.USER, 1001,
                ACE.DEFAULT_ADDRESS_MSK));

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
                AccessMask.ADD_SUBDIRECTORY.getValue(), Who.USER, 1001,
                ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.ADD_FILE.getValue(), Who.USER, 1001,
                ACE.DEFAULT_ADDRESS_MSK));

        _fs.setACL(dirInode, aces);
        _fs.setACL(dirInode, new ArrayList<ACE>() );
        assertTrue(_fs.getACL(dirInode).isEmpty());
    }

    @Test(expected=FileNotFoundHimeraFsException.class)
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

        _fs.move(dir01, "dir02", dir13, "dir14");

        FsInode newInode = _fs.inodeOf(dir13, "dir14");
        assertEquals("Invalid parent", dir13, newInode.inodeOf(".."));
    }

    @Test(expected = NotDirChimeraException.class)
    public void testMoveIntoFile() throws Exception {

	FsInode src = _rootInode.create("testMoveIntoFile1", 0, 0, 0644);
	FsInode dest = _rootInode.create("testMoveIntoFile2", 0, 0, 0644);
	_fs.move(_rootInode, "testMoveIntoFile1", dest, "testMoveIntoFile3");
    }

    @Test(expected = FileExistsChimeraFsException.class)
    public void testMoveIntoDir() throws Exception {

	FsInode src = _rootInode.create("testMoveIntoDir", 0, 0, 0644);
	FsInode dir = _rootInode.mkdir("dir", 0, 0, 0755);
	_fs.move(_rootInode, "testMoveIntoDir", _rootInode, "dir");
    }

    @Test(expected = FileNotFoundHimeraFsException.class)
    public void testMoveNotExists() throws Exception {
        _fs.move(_rootInode, "foo", _rootInode, "bar");
    }

    @Test(expected = DirNotEmptyHimeraFsException.class)
    public void testMoveNotEmptyDir() throws Exception {

	FsInode dir1 = _rootInode.mkdir("dir1", 0, 0, 0755);
	FsInode dir2 = _rootInode.mkdir("dir2", 0, 0, 0755);
	FsInode src = dir2.create("testMoveIntoDir", 0, 0, 0644);
	_fs.move(_rootInode, "dir1", _rootInode, "dir2");
    }

    @Test
    public void testMoveExistingWithLevel() throws Exception {

        FsInode base = _rootInode.mkdir("junit");
        FsInode inode1 = base.create("testCreateFile1", 0, 0, 0644);
        FsInode inode2 = base.create("testCreateFile2", 0, 0, 0644);

        FsInode level1of1 = new FsInode(_fs, inode1.toString(), 1);

        byte[] data = "hello".getBytes();
        level1of1.write(0, data, 0, data.length);
        assertTrue(_fs.move(base, "testCreateFile2", base, "testCreateFile1"));

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
        _rootInode.mkdir("testNameTooMoveLong");
        _fs.move(_rootInode, "testNameTooMoveLong", _rootInode, tooLongName);
    }

    @Test
    public void testChangeTagOwner() throws Exception {

        final String tagName = "myTag";
        FsInode base = _rootInode.mkdir("junit");
        _fs.createTag(base, tagName);
        FsInode tagInode = new FsInode_TAG(_fs, base.toString(), tagName);
        tagInode.setUID(1);

        assertEquals(1, tagInode.stat().getUid());
    }

    @Test
    public void testChangeTagOwnerGroup() throws Exception {

        final String tagName = "myTag";
        FsInode base = _rootInode.mkdir("junit");
        _fs.createTag(base, tagName);
        FsInode tagInode = new FsInode_TAG(_fs, base.toString(), tagName);
        tagInode.setGID(1);

        assertEquals(1, tagInode.stat().getGid());
    }

    @Test
    public void testChangeTagMode() throws Exception {

        final String tagName = "myTag";
        FsInode base = _rootInode.mkdir("junit");
        _fs.createTag(base, tagName);
        FsInode tagInode = new FsInode_TAG(_fs, base.toString(), tagName);
        tagInode.setMode(0007);

        assertEquals(0007 | UnixPermission.S_IFREG, tagInode.stat().getMode());
    }

    @Test
    public void testUpdateTagMtimeOnWrite() throws Exception {

        final String tagName = "myTag";
        final byte[] data1 = "some data".getBytes();
        final byte[] data2 = "some other data".getBytes();

        FsInode base = _rootInode.mkdir("junit");
        _fs.createTag(base, tagName);
        FsInode tagInode = new FsInode_TAG(_fs, base.toString(), tagName);

        tagInode.write(0, data1, 0, data1.length);
        Stat statBefore = tagInode.stat();

        // unshure that time in millis is changed
        TimeUnit.MICROSECONDS.sleep(2);

        tagInode.write(0, data2, 0, data2.length);
        Stat statAfter = tagInode.stat();

        assertTrue(statBefore.getMTime() != statAfter.getMTime());
    }

    @Test
    public void testSetAttribitesOnTag() throws Exception {
        final String tagName = "myTag";

        FsInode base = _rootInode.mkdir("junit");
        _fs.createTag(base, tagName);
        FsInode tagInode = new FsInode_TAG(_fs, base.toString(), tagName);

        Stat stat = tagInode.stat();
        Stat baseStat = base.stat();

        stat.setUid(123);
        _fs.setInodeAttributes(tagInode, 0, stat);

        assertEquals(baseStat, base.stat());
    }

    @Test
    public void testBackwardCompatibility() throws Exception {

        byte[] oldId = "0:TAG:0000DA875B38D9E0461F9ADFEA7C7422A956:somelongtagname".getBytes(Charsets.UTF_8);
        final FsInode inodeWithOldId = _fs.inodeFromBytes(oldId);
        byte[] newId = inodeWithOldId.getIdentifier();
        final FsInode inodeWithNewId = _fs.inodeFromBytes(newId);

        assertTrue(newId.length < oldId.length);
        assertEquals(inodeWithOldId, inodeWithNewId);
    }

    private void assertHasChecksum(Checksum expectedChecksum, FsInode inode) throws Exception {
        for(Checksum checksum: _fs.getInodeChecksums(inode)) {
            if (checksum.equals(expectedChecksum)) {
                return;
            }
        }
        fail("No checksums");
    }
}

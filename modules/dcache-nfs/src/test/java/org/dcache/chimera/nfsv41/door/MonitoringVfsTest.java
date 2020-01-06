/*
 * Copyright (c) 2018 - 2020 Deutsches Elektronen-Synchroton,
 * Member of the Helmholtz Association, (DESY), HAMBURG, GERMANY
 *
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera.nfsv41.door;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import javax.security.auth.Subject;

import java.io.IOException;
import java.util.Collections;

import diskCacheV111.namespace.EventReceiver;
import diskCacheV111.util.PnfsId;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.Link;
import org.dcache.namespace.FileType;
import org.dcache.namespace.events.EventType;
import org.dcache.nfs.status.BadHandleException;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.DirectoryStream;
import org.dcache.nfs.vfs.FileHandle.FileHandleBuilder;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.Stat.Type;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.nfs.vfs.VirtualFileSystem.StabilityLevel;
import org.dcache.nfs.vfs.VirtualFileSystem.WriteResult;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.dcache.nfs.vfs.Stat.S_TYPE;
import static org.dcache.nfs.vfs.Stat.Type.REGULAR;
import static org.dcache.nfs.vfs.VirtualFileSystem.StabilityLevel.UNSTABLE;
import static org.dcache.util.PrincipalSetMaker.aSetOfPrincipals;
import static org.junit.Assert.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.ArgumentMatchers.*;
import static org.hamcrest.CoreMatchers.*;
import static org.mockito.Mockito.*;

public class MonitoringVfsTest
{
    private static final Subject TEST_USER;

    static {
        TEST_USER = new Subject();
        TEST_USER.getPrincipals().addAll(aSetOfPrincipals()
                .withUsername("test-user")
                .withUid(1000)
                .withGid(1000)
                .build());
        TEST_USER.setReadOnly();
    }


    private class InodeBuilder
    {
        private long id = -1;
        private PnfsId pnfsId;
        private Link link;
        private Stat stat;

        public InodeBuilder withId(long id)
        {
            checkArgument(id >= 0);
            this.id = id;
            return this;
        }

        public InodeBuilder withPnfsId(String id)
        {
            pnfsId = new PnfsId(id);
            return this;
        }

        public InodeBuilder withPnfsId(PnfsId id)
        {
            pnfsId = id;
            return this;
        }

        public InodeBuilder withLink(Inode parent, String name)
        {
            try {
                FsInode parentInode = ChimeraVfs.inodeFromBytes(fsp, parent.getFileId());
                link = new Link(parentInode, name);
                return this;
            } catch (BadHandleException e) {
                throw new RuntimeException(e);
            }
        }

        public InodeBuilder withStat(StatBuilder statBuilder)
        {
            this.stat = statBuilder.build();
            return this;
        }

        public Inode build()
        {
            checkState(id >= 0, "Missing withId");

            FsInode inode = new FsInode(fsp, id);
            if (pnfsId != null) {
                try {
                    given(fsp.inode2id(eq(inode))).willReturn(pnfsId.toString());
                } catch (ChimeraFsException e) {
                    throw new RuntimeException(e);
                }
            }
            if (link != null) {
                try {
                    given(fsp.find(inode)).willReturn(Collections.singleton(link));
                } catch (ChimeraFsException e) {
                    throw new RuntimeException(e);
                }
            }

            Inode nfsInode = new Inode(new FileHandleBuilder().build(inode.getIdentifier()));

            if (stat != null) {
                try {
                    given(inner.getattr(nfsInode)).willReturn(stat);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            return nfsInode;
        }
    }

    private static class StatBuilder
    {
        private final Stat stat = new Stat();

        public StatBuilder withAtime(long atime)
        {
            stat.setATime(atime);
            return this;
        }

        public StatBuilder withMode(int mode)
        {
            stat.setMode(mode);
            return this;
        }

        public StatBuilder withType(Type type)
        {
            stat.setMode(stat.getMode() & ~S_TYPE | type.toMode());
            return this;
        }

        public StatBuilder withGeneration(long generation)
        {
            stat.setGeneration(generation);
            return this;
        }

        public Stat build()
        {
            return stat.clone();
        }
    }

    private static class WriteResultBuilder
    {
        private int bytesWritten;
        private StabilityLevel stabilityLevel;

        WriteResultBuilder withBytesWritten(int count)
        {
            bytesWritten = count;
            return this;
        }

        WriteResultBuilder withStabilityLevel(StabilityLevel level)
        {
            stabilityLevel = level;
            return this;
        }

        public WriteResult build()
        {
            return new WriteResult(stabilityLevel, bytesWritten);
        }
    }


    VirtualFileSystem inner;
    EventReceiver receiver;
    FileSystemProvider fsp;
    MonitoringVfs monitor;

    private InodeBuilder anInode()
    {
        return new InodeBuilder();
    }

    private static WriteResultBuilder aWriteResult()
    {
        return new WriteResultBuilder();
    }

    private static StatBuilder aStat()
    {
        return new StatBuilder();
    }

    @Before
    public void setup()
    {
        inner = mock(VirtualFileSystem.class);
        receiver = mock(EventReceiver.class);
        fsp = mock(FileSystemProvider.class);

        monitor = new MonitoringVfs();
        monitor.setEventReceiver(receiver);
        monitor.setInner(inner);
        monitor.setFileSystemProvider(fsp);
    }

    @Test
    public void shouldNotifyOnSuccessfulMkdir() throws Exception
    {
        PnfsId parentId = new PnfsId("000000000000000000000000000000000001");
        Inode parent = anInode().withId(1L).withPnfsId(parentId).build();
        Inode newDir = anInode().withId(2L).build();
        given(inner.mkdir(eq(parent), eq("foo"), any(), eq(0755))).willReturn(newDir);

        Inode result = monitor.mkdir(parent, "foo", TEST_USER, 0755);

        assertThat(result, is(equalTo(newDir)));
        verify(inner).mkdir(parent, "foo", TEST_USER, 0755);
        verify(receiver).notifyChildEvent(EventType.IN_CREATE, parentId, "foo", FileType.DIR);
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnFailedMkdir() throws Exception
    {
        Inode parent = anInode().withId(1L).withPnfsId("000000000000000000000000000000000001").build();
        given(inner.mkdir(eq(parent), eq("foo"), any(), eq(0755))).willThrow(IOException.class);

        try {
            monitor.mkdir(parent, "foo", TEST_USER, 0755);

            fail("mkdir operation unexpectedly succeeded");
        } catch (IOException e) {
        }
        verify(inner).mkdir(parent, "foo", TEST_USER, 0755);
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnSuccessfulCreate() throws Exception
    {
        PnfsId parentId = new PnfsId("000000000000000000000000000000000001");
        Inode parent = anInode().withId(1L).withPnfsId(parentId).build();
        Inode newFile = anInode().withId(2L).build();
        given(inner.create(eq(parent), any(), eq("foo"), any(), eq(0755))).willReturn(newFile);

        Inode result = monitor.create(parent, REGULAR, "foo", TEST_USER, 0755);

        assertThat(result, is(equalTo(newFile)));
        verify(inner).create(parent, REGULAR, "foo", TEST_USER, 0755);
        verify(receiver).notifyChildEvent(EventType.IN_CREATE, parentId, "foo", FileType.REGULAR);
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnFailedCreate() throws Exception
    {
        Inode parent = anInode().withId(1L).withPnfsId("000000000000000000000000000000000001").build();
        given(inner.create(eq(parent), any(), eq("foo"), any(), eq(0755))).willThrow(IOException.class);

        try {
            monitor.create(parent, REGULAR, "foo", TEST_USER, 0755);

            fail("create operation unexpectedly succeeded");
        } catch (IOException e) {
        }
        verify(inner).create(parent, REGULAR, "foo", TEST_USER, 0755);
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnLink() throws Exception
    {
        PnfsId parentId = new PnfsId("000000000000000000000000000000000001");
        Inode parent = anInode().withId(1L).withPnfsId(parentId).build();
        Inode existingFile = anInode().withId(2L).build();
        Inode newLink = anInode().withId(3L).build();
        given(inner.link(eq(parent), eq(existingFile), eq("new-link"), any())).willReturn(newLink);

        Inode result = monitor.link(parent, existingFile, "new-link", TEST_USER);

        assertThat(result, is(equalTo(newLink)));
        verify(inner).link(parent, existingFile, "new-link", TEST_USER);
        verify(receiver).notifyChildEvent(EventType.IN_CREATE, parentId, "new-link", FileType.REGULAR);
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulLink() throws Exception
    {
        Inode parent = anInode().withId(1L).withPnfsId("000000000000000000000000000000000001").build();
        Inode existingFile = anInode().withId(2L).build();
        given(inner.link(eq(parent), eq(existingFile), eq("new-link"), any())).willThrow(IOException.class);

        try {
            monitor.link(parent, existingFile, "new-link", TEST_USER);

            fail("link unexpectedly succeeded");
        } catch (IOException e) {
        }
        verify(inner).link(parent, existingFile, "new-link", TEST_USER);
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnSymlink() throws Exception
    {
        PnfsId parentId = new PnfsId("000000000000000000000000000000000001");
        Inode parent = anInode().withId(1L).withPnfsId(parentId).build();
        Inode newSymLink = anInode().withId(2L).build();
        given(inner.symlink(eq(parent), eq("new-symlink"), eq("target"), any(), eq(0644)))
                .willReturn(newSymLink);

        Inode result = monitor.symlink(parent, "new-symlink", "target", TEST_USER, 0644);

        assertThat(result, is(equalTo(newSymLink)));
        verify(inner).symlink(parent, "new-symlink", "target", TEST_USER, 0644);
        verify(receiver).notifyChildEvent(EventType.IN_CREATE, parentId, "new-symlink", FileType.LINK);
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulSymlink() throws Exception
    {
        PnfsId parentId = new PnfsId("000000000000000000000000000000000001");
        Inode parent = anInode().withId(1L).withPnfsId(parentId).build();
        given(inner.symlink(eq(parent), eq("new-symlink"), eq("target"), any(), eq(0644)))
                .willThrow(IOException.class);

        try {
            monitor.symlink(parent, "new-symlink", "target", TEST_USER, 0644);

            fail("symlink unexpectedly succeeded");
        } catch (IOException e) {
        }
        verify(inner).symlink(parent, "new-symlink", "target", TEST_USER, 0644);
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnSetattr() throws Exception
    {
        PnfsId parentId = new PnfsId("000000000000000000000000000000000001");
        PnfsId targetId = new PnfsId("000000000000000000000000000000000002");
        Inode parent = anInode().withId(1L).withPnfsId(parentId).build();
        Inode target = anInode().withId(2L).withPnfsId(targetId).withLink(parent, "target")
                .withStat(aStat().withMode(0644).withType(REGULAR)).build();
        Stat stat = aStat().withAtime(42L).build();

        monitor.setattr(target, stat);

        verify(inner).setattr(target, stat);
        verify(receiver).notifyChildEvent(EventType.IN_ATTRIB, parentId, "target", FileType.REGULAR);
        verify(receiver).notifySelfEvent(EventType.IN_ATTRIB, targetId, FileType.REGULAR);
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulSetattr() throws Exception
    {
        PnfsId parentId = new PnfsId("000000000000000000000000000000000001");
        PnfsId targetId = new PnfsId("000000000000000000000000000000000002");
        Inode parent = anInode().withId(1L).withPnfsId(parentId).build();
        Inode target = anInode().withId(2L).withPnfsId(targetId).withLink(parent, "target")
                .withStat(aStat().withMode(0644).withType(REGULAR)).build();
        Stat stat = aStat().withAtime(42L).build();
        willThrow(IOException.class).given(inner).setattr(any(), any());

        try {
            monitor.setattr(target, stat);

            fail("setattr unexpectedly succeeded");
        } catch (IOException e) {
        }
        verify(inner).setattr(target, stat);
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnSetAcl() throws Exception
    {
        PnfsId parentId = new PnfsId("000000000000000000000000000000000001");
        PnfsId targetId = new PnfsId("000000000000000000000000000000000002");
        Inode parent = anInode().withId(1L).withPnfsId(parentId).build();
        Inode target = anInode().withId(2L).withPnfsId(targetId).withLink(parent, "target")
                .withStat(aStat().withMode(0644).withType(REGULAR)).build();
        nfsace4[] acl = {};

        monitor.setAcl(target, acl);

        verify(inner).setAcl(target, acl);
        verify(receiver).notifyChildEvent(EventType.IN_ATTRIB, parentId, "target", FileType.REGULAR);
        verify(receiver).notifySelfEvent(EventType.IN_ATTRIB, targetId, FileType.REGULAR);
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulSetAcl() throws Exception
    {
        PnfsId parentId = new PnfsId("000000000000000000000000000000000001");
        PnfsId targetId = new PnfsId("000000000000000000000000000000000002");
        Inode parent = anInode().withId(1L).withPnfsId(parentId).build();
        Inode target = anInode().withId(2L).withPnfsId(targetId).withLink(parent, "target")
                .withStat(aStat().withMode(0644).withType(REGULAR)).build();
        nfsace4[] acl = {};
        willThrow(IOException.class).given(inner).setAcl(any(), any());

        try {
            monitor.setAcl(target, acl);

            fail("setAcl unexpectedly succeeded");
        } catch (IOException e) {
        }
        verify(inner).setAcl(target, acl);
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnRead() throws Exception
    {
        PnfsId parentId = new PnfsId("000000000000000000000000000000000001");
        PnfsId targetId = new PnfsId("000000000000000000000000000000000002");
        Inode parent = anInode().withId(1L).withPnfsId(parentId).build();
        Inode target = anInode().withId(2L).withPnfsId(targetId).withLink(parent, "target").build();
        given(inner.read(eq(target), any(), anyLong(), anyInt())).willReturn(100);
        byte[] data = new byte[1024];

        int result = monitor.read(target, data, 0, 1024);

        assertThat(result, is(equalTo(100)));
        verify(inner).read(target, data, 0, 1024);
        InOrder childEvents = inOrder(receiver);
        childEvents.verify(receiver).notifyChildEvent(EventType.IN_OPEN, parentId, "target", FileType.REGULAR);
        childEvents.verify(receiver).notifyChildEvent(EventType.IN_ACCESS, parentId, "target", FileType.REGULAR);
        childEvents.verify(receiver).notifyChildEvent(EventType.IN_CLOSE_NOWRITE, parentId, "target", FileType.REGULAR);
        InOrder targetEvents = inOrder(receiver);
        targetEvents.verify(receiver).notifySelfEvent(EventType.IN_OPEN, targetId, FileType.REGULAR);
        targetEvents.verify(receiver).notifySelfEvent(EventType.IN_ACCESS, targetId, FileType.REGULAR);
        targetEvents.verify(receiver).notifySelfEvent(EventType.IN_CLOSE_NOWRITE, targetId, FileType.REGULAR);
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulRead() throws Exception
    {
        PnfsId parentId = new PnfsId("000000000000000000000000000000000001");
        PnfsId targetId = new PnfsId("000000000000000000000000000000000002");
        Inode parent = anInode().withId(1L).withPnfsId(parentId).build();
        Inode target = anInode().withId(2L).withPnfsId(targetId).withLink(parent, "target").build();
        willThrow(IOException.class).given(inner).read(any(), any(), anyLong(), anyInt());
        byte[] data = new byte[1024];

        try {
            monitor.read(target, data, 0, 1024);

            fail("read unexpectedly succeeded");
        } catch (IOException e) {
        }
        verify(inner).read(target, data, 0, 1024);
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnWrite() throws Exception
    {
        PnfsId parentId = new PnfsId("000000000000000000000000000000000001");
        PnfsId targetId = new PnfsId("000000000000000000000000000000000002");
        Inode parent = anInode().withId(1L).withPnfsId(parentId).build();
        Inode target = anInode().withId(2L).withPnfsId(targetId).withLink(parent, "target").build();
        given(inner.write(eq(target), any(), anyLong(), anyInt(), any()))
                .willReturn(aWriteResult().withBytesWritten(100).withStabilityLevel(UNSTABLE).build());
        byte[] data = new byte[1024];

        WriteResult result = monitor.write(target, data, 0, 1024, UNSTABLE);

        verify(inner).write(target, data, 0, 1024, UNSTABLE);
        assertThat(result.getBytesWritten(), is(equalTo(100)));
        assertThat(result.getStabilityLevel(), is(equalTo(UNSTABLE)));
        InOrder childEvents = inOrder(receiver);
        childEvents.verify(receiver).notifyChildEvent(EventType.IN_OPEN, parentId, "target", FileType.REGULAR);
        childEvents.verify(receiver).notifyChildEvent(EventType.IN_MODIFY, parentId, "target", FileType.REGULAR);
        childEvents.verify(receiver).notifyChildEvent(EventType.IN_CLOSE_WRITE, parentId, "target", FileType.REGULAR);
        InOrder targetEvents = inOrder(receiver);
        targetEvents.verify(receiver).notifySelfEvent(EventType.IN_OPEN, targetId, FileType.REGULAR);
        targetEvents.verify(receiver).notifySelfEvent(EventType.IN_MODIFY, targetId, FileType.REGULAR);
        targetEvents.verify(receiver).notifySelfEvent(EventType.IN_CLOSE_WRITE, targetId, FileType.REGULAR);
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulWrite() throws Exception
    {
        Inode parent = anInode().withId(1L).withPnfsId("000000000000000000000000000000000001").build();
        Inode target = anInode().withId(2L).withPnfsId("000000000000000000000000000000000002")
                .withLink(parent, "target").build();
        byte[] data = new byte[1024];
        given(inner.write(eq(target), any(), anyLong(), anyInt(), any()))
                .willThrow(IOException.class);

        try {
            monitor.write(target, data, 0, 1024, UNSTABLE);

            fail("write unexpectedly succeeded");
        } catch (IOException e) {
        }

        verify(inner).write(target, data, 0, 1024, UNSTABLE);
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnRemove() throws Exception
    {
        PnfsId parentId = new PnfsId("000000000000000000000000000000000001");
        PnfsId targetId = new PnfsId("000000000000000000000000000000000002");
        Inode parent = anInode().withId(1L).withPnfsId(parentId).build();
        Inode target = anInode().withId(2L).withPnfsId(targetId).withLink(parent, "target")
                .withStat(aStat().withMode(0644).withType(REGULAR)).build();
        given(inner.lookup(parent, "target")).willReturn(target); // REVISIT move this into InodeBuilder?

        monitor.remove(parent, "target");

        verify(inner).remove(parent, "target");
        verify(receiver).notifyChildEvent(EventType.IN_DELETE, parentId, "target", FileType.REGULAR);
        verify(receiver).notifySelfEvent(EventType.IN_DELETE_SELF, targetId, FileType.REGULAR);
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulRemove() throws Exception
    {
        PnfsId parentId = new PnfsId("000000000000000000000000000000000001");
        PnfsId targetId = new PnfsId("000000000000000000000000000000000002");
        Inode parent = anInode().withId(1L).withPnfsId(parentId).build();
        Inode target = anInode().withId(2L).withPnfsId(targetId).withLink(parent, "target")
                .withStat(aStat().withMode(0644).withType(REGULAR)).build();
        given(inner.lookup(parent, "target")).willReturn(target);
        willThrow(IOException.class).given(inner).remove(any(), any());

        try {
            monitor.remove(parent, "target");

            fail("remove unexpectedly succeeded");
        } catch (IOException e) {
        }
        verify(inner).remove(parent, "target");
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnMove() throws Exception
    {
        PnfsId sourceDirId = new PnfsId("000000000000000000000000000000000001");
        PnfsId destinationDirId = new PnfsId("000000000000000000000000000000000002");
        PnfsId targetId = new PnfsId("000000000000000000000000000000000003");
        Inode sourceDir = anInode().withId(1L).withPnfsId(sourceDirId).build();
        Inode destinationDir = anInode().withId(2L).withPnfsId(destinationDirId)
                .withStat(aStat().withGeneration(1)).build();
        Inode target = anInode().withId(3L).withPnfsId(targetId).withLink(sourceDir, "target")
                .withStat(aStat().withMode(0644).withType(REGULAR)).build();
        willReturn(true).given(inner).move(sourceDir, "target", destinationDir, "new-name");
        given(inner.lookup(sourceDir, "target")).willReturn(target);
        given(inner.lookup(destinationDir, "new-name")).willReturn(target);

        boolean result = monitor.move(sourceDir, "target", destinationDir, "new-name");

        assertThat(result, is(equalTo(true)));
        verify(inner).move(sourceDir, "target", destinationDir, "new-name");
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver).notifySelfEvent(EventType.IN_MOVE_SELF, targetId, FileType.REGULAR);
        ArgumentCaptor<String> fromCookie = ArgumentCaptor.forClass(String.class);
        verify(receiver).notifyMovedEvent(eq(EventType.IN_MOVED_FROM), eq(sourceDirId), eq("target"), fromCookie.capture(), eq(FileType.REGULAR));
        ArgumentCaptor<String> toCookie = ArgumentCaptor.forClass(String.class);
        verify(receiver).notifyMovedEvent(eq(EventType.IN_MOVED_TO), eq(destinationDirId), eq("new-name"), toCookie.capture(), eq(FileType.REGULAR));
        assertThat("cookie mismatch", fromCookie.getValue(), is(equalTo(toCookie.getValue())));
    }

    @Test
    public void shouldNotNotifyOnNoopMove() throws Exception
    {
        PnfsId sourceDirId = new PnfsId("000000000000000000000000000000000001");
        PnfsId destinationDirId = new PnfsId("000000000000000000000000000000000002");
        PnfsId targetId = new PnfsId("000000000000000000000000000000000003");
        Inode sourceDir = anInode().withId(1L).withPnfsId(sourceDirId).build();
        Inode destinationDir = anInode().withId(2L).withPnfsId(destinationDirId)
                .withStat(aStat().withGeneration(1)).build();
        Inode target = anInode().withId(3L).withPnfsId(targetId).withLink(sourceDir, "target")
                .withStat(aStat().withMode(0644).withType(REGULAR)).build();
        willReturn(false).given(inner).move(sourceDir, "target", destinationDir, "new-name");
        given(inner.lookup(sourceDir, "target")).willReturn(target);
        given(inner.lookup(destinationDir, "new-name")).willReturn(target);

        boolean result = monitor.move(sourceDir, "target", destinationDir, "new-name");

        assertThat(result, is(equalTo(false)));
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulMove() throws Exception
    {
        PnfsId sourceDirId = new PnfsId("000000000000000000000000000000000001");
        PnfsId destinationDirId = new PnfsId("000000000000000000000000000000000002");
        PnfsId targetId = new PnfsId("000000000000000000000000000000000003");
        Inode sourceDir = anInode().withId(1L).withPnfsId(sourceDirId).build();
        Inode destinationDir = anInode().withId(2L).withPnfsId(destinationDirId)
                .withStat(aStat().withGeneration(1)).build();
        Inode target = anInode().withId(3L).withPnfsId(targetId).withLink(sourceDir, "target")
                .withStat(aStat().withMode(0644).withType(REGULAR)).build();
        willThrow(IOException.class).given(inner).move(sourceDir, "target", destinationDir, "new-name");
        given(inner.lookup(sourceDir, "target")).willReturn(target);
        given(inner.lookup(destinationDir, "new-name")).willReturn(target);

        try {
            monitor.move(sourceDir, "target", destinationDir, "new-name");

            fail("move unexpectedly succeeded");
        } catch (IOException e) {
        }
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnList() throws Exception
    {
        PnfsId parentDirId = new PnfsId("000000000000000000000000000000000001");
        PnfsId targetDirId = new PnfsId("000000000000000000000000000000000002");
        Inode parentDir = anInode().withId(1L).withPnfsId(parentDirId).build();
        Inode targetDir = anInode().withId(2L).withPnfsId(targetDirId)
                .withLink(parentDir, "target").build();
        byte[] verifier = {};
        DirectoryStream listing = mock(DirectoryStream.class);
        given(inner.list(targetDir, verifier, 0)).willReturn(listing);

        DirectoryStream result = monitor.list(targetDir, verifier, 0);

        assertThat(result, is(equalTo(listing)));
        verify(inner).list(targetDir, verifier, 0);
        InOrder parentEvents = inOrder(receiver);
        parentEvents.verify(receiver).notifyChildEvent(EventType.IN_OPEN, parentDirId, "target", FileType.DIR);
        parentEvents.verify(receiver).notifyChildEvent(EventType.IN_CLOSE_NOWRITE, parentDirId, "target", FileType.DIR);
        InOrder targetEvents = inOrder(receiver);
        targetEvents.verify(receiver).notifySelfEvent(EventType.IN_OPEN, targetDirId, FileType.DIR);
        targetEvents.verify(receiver).notifySelfEvent(EventType.IN_CLOSE_NOWRITE, targetDirId, FileType.DIR);
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulList() throws Exception
    {
        PnfsId parentDirId = new PnfsId("000000000000000000000000000000000001");
        PnfsId targetDirId = new PnfsId("000000000000000000000000000000000002");
        Inode parentDir = anInode().withId(1L).withPnfsId(parentDirId).build();
        Inode targetDir = anInode().withId(2L).withPnfsId(targetDirId)
                .withLink(parentDir, "target").build();
        byte[] verifier = {};
        given(inner.list(targetDir, verifier, 0)).willThrow(IOException.class);

        try {
            monitor.list(targetDir, verifier, 0);

            fail("list unexpectedly succeeded");
        } catch (IOException e) {
        }
        verify(inner).list(targetDir, verifier, 0);
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }
}

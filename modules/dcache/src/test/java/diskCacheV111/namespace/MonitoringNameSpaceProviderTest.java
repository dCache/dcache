/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 - 2020 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package diskCacheV111.namespace;

import com.google.common.collect.Range;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import javax.security.auth.Subject;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import diskCacheV111.namespace.NameSpaceProvider.Link;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;

import org.dcache.namespace.CreateOption;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.ListHandler;
import org.dcache.namespace.events.EventType;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.util.PrincipalSetMaker.aSetOfPrincipals;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.hamcrest.text.IsEmptyString.emptyOrNullString;
import static org.mockito.hamcrest.MockitoHamcrest.*;

public class MonitoringNameSpaceProviderTest
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

    MonitoringNameSpaceProvider monitor;
    NameSpaceProvider inner;
    EventReceiver receiver;

    @Before
    public void setup()
    {
        inner = mock(NameSpaceProvider.class);
        receiver = mock(EventReceiver.class);

        monitor = new MonitoringNameSpaceProvider();
        monitor.setNameSpaceProvider(inner);
        monitor.setEventReceiver(receiver);
    }

    @Test
    public void shouldNotifyOnCreateDirectory() throws Exception
    {
        PnfsId existingDir = new PnfsId("000000000000000000000000000000000001");
        PnfsId newDir = new PnfsId("000000000000000000000000000000000002");
        FileAttributes newDirAttr = new FileAttributes();
        given(inner.createDirectory(any(), eq("/foo/bar"), any())).willReturn(newDir);
        given(inner.find(any(), eq(newDir))).willReturn(singleLink(existingDir, "bar"));

        PnfsId result = monitor.createDirectory(TEST_USER, "/foo/bar", newDirAttr);

        assertThat(result, is(equalTo(newDir)));
        verify(inner).createDirectory(TEST_USER, "/foo/bar", newDirAttr);
        verify(receiver).notifyChildEvent(EventType.IN_CREATE, existingDir, "bar",
                FileType.DIR);
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnFailedCreateDirectory() throws Exception
    {
        given(inner.createDirectory(any(), eq("/foo/bar"), any())).willThrow(CacheException.class);

        try {
            monitor.createDirectory(TEST_USER, "/foo/bar", new FileAttributes());

            fail("createDirectory succeeded when inner class failed");
        } catch (CacheException e) {
        }
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnSetFileAttributes() throws Exception
    {
        PnfsId parentDir = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        FileAttributes newDirAttr = FileAttributes.ofMode(0755);
        given(inner.setFileAttributes(any(), eq(target), any(), any()))
                .willReturn(FileAttributes.of().accessTime(42).fileType(FileType.REGULAR).build());
        given(inner.find(any(), eq(target))).willReturn(singleLink(parentDir, "foo"));

        FileAttributes result = monitor.setFileAttributes(TEST_USER, target, newDirAttr,
                EnumSet.of(FileAttribute.ACCESS_TIME));

        assertThat(result.getAccessTime(), is(equalTo(42L)));
        verify(inner).setFileAttributes(eq(TEST_USER), eq(target), eq(newDirAttr),
                (Set<FileAttribute>)argThat(hasItem(FileAttribute.ACCESS_TIME)));
        verify(receiver).notifyChildEvent(EventType.IN_ATTRIB, parentDir, "foo", FileType.REGULAR);
        verify(receiver).notifySelfEvent(EventType.IN_ATTRIB, target, FileType.REGULAR);
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnFailedSetFileAttributes() throws Exception
    {
        given(inner.setFileAttributes(any(), any(), any(), any())).willThrow(CacheException.class);

        try {
            monitor.setFileAttributes(TEST_USER,
                    new PnfsId("000000000000000000000000000000000002"),
                    FileAttributes.ofMode(0755),
                    EnumSet.of(FileAttribute.ACCESS_TIME));

            fail("setFileAttributes unexpectedly succeeded");
        } catch (CacheException e) {
        }
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnCreateFile() throws Exception
    {
        PnfsId parentDir = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        FileAttributes newFileAttrs = FileAttributes.ofMode(0755);
        given(inner.createFile(any(), eq("/foo"), any(), any()))
                .willReturn(FileAttributes.of().pnfsId(target).accessTime(42).build());
        given(inner.find(any(), eq(target))).willReturn(singleLink(parentDir, "foo"));

        FileAttributes result = monitor.createFile(TEST_USER, "/foo", newFileAttrs,
                EnumSet.of(FileAttribute.ACCESS_TIME));

        assertThat(result.getAccessTime(), is(equalTo(42L)));
        verify(inner).createFile(eq(TEST_USER), eq("/foo"), eq(newFileAttrs),
                (Set<FileAttribute>)argThat(hasItem(FileAttribute.ACCESS_TIME)));
        verify(receiver).notifyChildEvent(EventType.IN_CREATE, parentDir, "foo", FileType.REGULAR);
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnFailedCreateFile() throws Exception
    {
        given(inner.createFile(any(), any(), any(), any())).willThrow(CacheException.class);

        try {
            monitor.createFile(TEST_USER, "/foo", FileAttributes.ofMode(0755),
                    EnumSet.of(FileAttribute.ACCESS_TIME));

            fail("createFile unexpectedly succeeded");
        } catch (CacheException e) {
        }
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnListWithNoEntries() throws Exception
    {
        ListHandler handler = mock(ListHandler.class);
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        given(inner.pathToPnfsid(any(), eq("/foo"), anyBoolean())).willReturn(target);
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "foo"));

        monitor.list(TEST_USER, "/foo", null, Range.all(), EnumSet.noneOf(FileAttribute.class), handler);

        verify(inner).list(eq(TEST_USER), eq("/foo"), eq(null), eq(Range.all()), any(), any());
        verify(handler,never()).addEntry(any(), any());
        InOrder childEvents = inOrder(receiver);
        childEvents.verify(receiver).notifyChildEvent(EventType.IN_OPEN, parent, "foo", FileType.DIR);
        childEvents.verify(receiver).notifyChildEvent(EventType.IN_CLOSE_NOWRITE, parent, "foo", FileType.DIR);
        InOrder dirEvents = inOrder(receiver);
        dirEvents.verify(receiver).notifySelfEvent(EventType.IN_OPEN, target, FileType.DIR);
        dirEvents.verify(receiver).notifySelfEvent(EventType.IN_CLOSE_NOWRITE, target, FileType.DIR);
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnListWithSingleEntry() throws Exception
    {
        ListHandler handler = mock(ListHandler.class);
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        given(inner.pathToPnfsid(any(), eq("/foo"), anyBoolean())).willReturn(target);
        willAnswer(i -> {
                    ListHandler h = i.getArgument(5, ListHandler.class);
                    h.addEntry("bar", new FileAttributes());
                    return null;
                }).given(inner).list(any(), eq("/foo"), any(), any(), any(), any());
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "foo"));

        monitor.list(TEST_USER, "/foo", null, Range.all(), EnumSet.noneOf(FileAttribute.class), handler);

        verify(inner).list(eq(TEST_USER), eq("/foo"), eq(null), eq(Range.all()), any(), any());
        verify(handler).addEntry(eq("bar"), any());
        InOrder childEvents = inOrder(receiver);
        childEvents.verify(receiver).notifyChildEvent(EventType.IN_OPEN, parent, "foo", FileType.DIR);
        childEvents.verify(receiver).notifyChildEvent(EventType.IN_CLOSE_NOWRITE, parent, "foo", FileType.DIR);
        InOrder dirEvents = inOrder(receiver);
        dirEvents.verify(receiver).notifySelfEvent(EventType.IN_OPEN, target, FileType.DIR);
        dirEvents.verify(receiver).notifySelfEvent(EventType.IN_CLOSE_NOWRITE, target, FileType.DIR);
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnListWithTwoEntries() throws Exception
    {
        ListHandler handler = mock(ListHandler.class);
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        given(inner.pathToPnfsid(any(), eq("/foo"), anyBoolean())).willReturn(target);
        willAnswer(i -> {
                    ListHandler h = i.getArgument(5, ListHandler.class);
                    h.addEntry("bar-1", new FileAttributes());
                    h.addEntry("bar-2", new FileAttributes());
                    return null;
                }).given(inner).list(any(), eq("/foo"), any(), any(), any(), any());
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "foo"));

        monitor.list(TEST_USER, "/foo", null, Range.all(), EnumSet.noneOf(FileAttribute.class), handler);

        verify(inner).list(eq(TEST_USER), eq("/foo"), eq(null), eq(Range.all()), any(), any());
        verify(handler).addEntry(eq("bar-1"), any());
        verify(handler).addEntry(eq("bar-2"), any());
        InOrder childEvents = inOrder(receiver);
        childEvents.verify(receiver).notifyChildEvent(EventType.IN_OPEN, parent, "foo", FileType.DIR);
        childEvents.verify(receiver).notifyChildEvent(EventType.IN_CLOSE_NOWRITE, parent, "foo", FileType.DIR);
        InOrder dirEvents = inOrder(receiver);
        dirEvents.verify(receiver).notifySelfEvent(EventType.IN_OPEN, target, FileType.DIR);
        dirEvents.verify(receiver).notifySelfEvent(EventType.IN_CLOSE_NOWRITE, target, FileType.DIR);
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulList() throws Exception
    {
        ListHandler handler = mock(ListHandler.class);
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        given(inner.pathToPnfsid(any(), eq("/foo"), anyBoolean())).willReturn(target);
        willThrow(CacheException.class).given(inner).list(any(), eq("/foo"), any(), any(), any(), any());
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "foo"));

        try {
            monitor.list(TEST_USER, "/foo", null, Range.all(), EnumSet.noneOf(FileAttribute.class), handler);

            fail("list unexpectedly succeeded");
        } catch (CacheException e) {
        }
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnCreateSymLink() throws Exception
    {
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId newSymLink = new PnfsId("000000000000000000000000000000000002");
        given(inner.find(any(), eq(newSymLink))).willReturn(singleLink(parent, "foo"));
        given(inner.createSymLink(any(), eq("/foo"), any(), any())).willReturn(newSymLink);

        PnfsId result = monitor.createSymLink(TEST_USER, "/foo", "/bar", new FileAttributes());

        verify(inner).createSymLink(eq(TEST_USER), eq("/foo"), eq("/bar"), any());
        assertThat(result, is(equalTo(newSymLink)));
        verify(receiver).notifyChildEvent(EventType.IN_CREATE, parent, "foo", FileType.LINK);
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulCreateSymLink() throws Exception
    {
        given(inner.createSymLink(any(), eq("/foo"), any(), any())).willThrow(CacheException.class);

        try {
            monitor.createSymLink(TEST_USER, "/foo", "/bar", new FileAttributes());

            fail("createSymLink unexpectedly succeeded");
        } catch (CacheException e) {
        }
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnRename() throws Exception
    {
        PnfsId sourceDir = new PnfsId("000000000000000000000000000000000001");
        PnfsId destinationDir = new PnfsId("000000000000000000000000000000000002");
        PnfsId entry = new PnfsId("000000000000000000000000000000000003");
        given(inner.pathToPnfsid(any(), eq("/foo"), anyBoolean())).willReturn(sourceDir);
        given(inner.pathToPnfsid(any(), eq("/bar"), anyBoolean())).willReturn(destinationDir);
        given(inner.getFileAttributes(any(), eq(entry), any())).willReturn(FileAttributes.ofFileType(FileType.REGULAR));

        monitor.rename(TEST_USER, entry, "/foo/baz1", "/bar/baz2", true);

        verify(inner).rename(eq(TEST_USER), eq(entry), eq("/foo/baz1"), eq("/bar/baz2"), eq(true));
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver).notifySelfEvent(EventType.IN_MOVE_SELF, entry, FileType.REGULAR);
        ArgumentCaptor<String> fromCookie = ArgumentCaptor.forClass(String.class);
        verify(receiver).notifyMovedEvent(eq(EventType.IN_MOVED_FROM), eq(sourceDir), eq("baz1"), fromCookie.capture(), eq(FileType.REGULAR));
        ArgumentCaptor<String> toCookie = ArgumentCaptor.forClass(String.class);
        verify(receiver).notifyMovedEvent(eq(EventType.IN_MOVED_TO), eq(destinationDir), eq("baz2"), toCookie.capture(), eq(FileType.REGULAR));
        assertThat("cookie mismatch", fromCookie.getValue(), is(equalTo(toCookie.getValue())));
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulRename() throws Exception
    {
        PnfsId entry = new PnfsId("000000000000000000000000000000000003");
        willThrow(CacheException.class).given(inner).rename(any(), any(), any(), any(), anyBoolean());

        try {
            monitor.rename(TEST_USER, entry, "/foo/baz1", "/bar/baz2", true);

            fail("rename unexpectedly succeeded");
        } catch (CacheException e) {
        }
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnPnfsIdDelete() throws Exception
    {
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "foo"));
        given(inner.deleteEntry(any(), any(), eq(target), any()))
                .willReturn(FileAttributes.of().fileType(FileType.REGULAR).accessTime(42L).build());

        FileAttributes result = monitor.deleteEntry(TEST_USER, EnumSet.allOf(FileType.class),
                target, EnumSet.of(FileAttribute.ACCESS_TIME));

        verify(inner).deleteEntry(eq(TEST_USER), eq(EnumSet.allOf(FileType.class)),
                eq(target), (Set<FileAttribute>)argThat(hasItem(FileAttribute.ACCESS_TIME)));
        assertThat(result.getAccessTime(), is(equalTo(42L)));
        verify(receiver).notifyChildEvent(EventType.IN_DELETE, parent, "foo", FileType.REGULAR);
        verify(receiver).notifySelfEvent(EventType.IN_DELETE_SELF, target, FileType.REGULAR);
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulPnfsIdDelete() throws Exception
    {
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "foo"));
        given(inner.deleteEntry(any(), any(), eq(target), any()))
                .willThrow(CacheException.class);

        try {
            monitor.deleteEntry(TEST_USER, EnumSet.allOf(FileType.class),
                    target, EnumSet.of(FileAttribute.ACCESS_TIME));

            fail("deleteEntry(PnfsId) unexpectedly succeeded");
        } catch (CacheException e) {
        }
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnPathDelete() throws Exception
    {
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        given(inner.pathToPnfsid(any(), eq("/foo"), anyBoolean())).willReturn(target);
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "foo"));
        given(inner.deleteEntry(any(), any(), eq("/foo"), any()))
                .willReturn(FileAttributes.of().pnfsId(target).fileType(FileType.REGULAR).accessTime(42L).build());

        FileAttributes result = monitor.deleteEntry(TEST_USER, EnumSet.allOf(FileType.class),
                "/foo", EnumSet.of(FileAttribute.ACCESS_TIME));

        verify(inner).deleteEntry(eq(TEST_USER), eq(EnumSet.allOf(FileType.class)),
                eq("/foo"), (Set<FileAttribute>)argThat(hasItem(FileAttribute.ACCESS_TIME)));
        assertThat(result.getAccessTime(), is(equalTo(42L)));
        verify(receiver).notifyChildEvent(EventType.IN_DELETE, parent, "foo", FileType.REGULAR);
        verify(receiver).notifySelfEvent(EventType.IN_DELETE_SELF, target, FileType.REGULAR);
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulPathDelete() throws Exception
    {
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        given(inner.pathToPnfsid(any(), eq("/foo"), anyBoolean())).willReturn(target);
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "foo"));
        given(inner.deleteEntry(any(), any(), eq("/foo"), any()))
                .willThrow(CacheException.class);

        try {
            monitor.deleteEntry(TEST_USER, EnumSet.allOf(FileType.class),
                    "/foo", EnumSet.of(FileAttribute.ACCESS_TIME));

            fail("deleteEntry(path) unexpectedly succeeded");
        } catch (CacheException e) {
        }
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnPnfsIdPathDelete() throws Exception
    {
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "foo"));
        given(inner.deleteEntry(any(), any(), eq(target), eq("/foo"), any()))
                .willReturn(FileAttributes.of().fileType(FileType.REGULAR).accessTime(42L).build());

        FileAttributes result = monitor.deleteEntry(TEST_USER, EnumSet.allOf(FileType.class),
                target, "/foo", EnumSet.of(FileAttribute.ACCESS_TIME));

        verify(inner).deleteEntry(eq(TEST_USER), eq(EnumSet.allOf(FileType.class)),
                eq(target), eq("/foo"), (Set<FileAttribute>)argThat(hasItem(FileAttribute.ACCESS_TIME)));
        assertThat(result.getAccessTime(), is(equalTo(42L)));
        verify(receiver).notifyChildEvent(EventType.IN_DELETE, parent, "foo", FileType.REGULAR);
        verify(receiver).notifySelfEvent(EventType.IN_DELETE_SELF, target, FileType.REGULAR);
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulPnfsIdPathDelete() throws Exception
    {
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "foo"));
        given(inner.deleteEntry(any(), any(), eq(target), eq("/foo"), any()))
                .willThrow(CacheException.class);

        try {
            monitor.deleteEntry(TEST_USER, EnumSet.allOf(FileType.class),
                    target, "/foo", EnumSet.of(FileAttribute.ACCESS_TIME));

            fail("deleteEntry(pnfsid,path) unexpectedly succeeded");
        } catch (CacheException e) {
        }
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnAddCacheLocation() throws Exception
    {
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "foo"));

        monitor.addCacheLocation(TEST_USER, target, "new-location");

        verify(inner).addCacheLocation(eq(TEST_USER), eq(target), eq("new-location"));
        verify(receiver).notifyChildEvent(EventType.IN_ATTRIB, parent, "foo", FileType.REGULAR);
        verify(receiver).notifySelfEvent(EventType.IN_ATTRIB, target, FileType.REGULAR);
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulAddCacheLocation() throws Exception
    {
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "foo"));
        willThrow(CacheException.class).given(inner).addCacheLocation(any(), any(), any());

        try {
            monitor.addCacheLocation(TEST_USER, target, "new-location");

            fail("addCacheLocation unexpectedly succeeded");
        } catch (CacheException e) {
        }
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnClearCacheLocation() throws Exception
    {
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "foo"));

        monitor.clearCacheLocation(TEST_USER, target, "new-location", false);

        verify(inner).clearCacheLocation(eq(TEST_USER), eq(target), eq("new-location"), eq(false));
        verify(receiver).notifyChildEvent(EventType.IN_ATTRIB, parent, "foo", FileType.REGULAR);
        verify(receiver).notifySelfEvent(EventType.IN_ATTRIB, target, FileType.REGULAR);
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulClearCacheLocation() throws Exception
    {
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "foo"));
        willThrow(CacheException.class).given(inner).clearCacheLocation(any(), any(), any(), anyBoolean());

        try {
            monitor.clearCacheLocation(TEST_USER, target, "new-location", false);

            fail("clearCacheLocation unexpectedly succeeded");
        } catch (CacheException e) {
        }
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldNotifyOnCommitUpload() throws Exception
    {
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        FsPath uploadPath = FsPath.create("/uploads/1/update-1/file-1");
        FsPath targetPath = FsPath.create("/data/file-1");
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "file-1"));
        given(inner.commitUpload(any(), eq(uploadPath), eq(targetPath), any(), any()))
                .willReturn(FileAttributes.of().accessTime(42L).pnfsId(target).build());

        FileAttributes result = monitor.commitUpload(TEST_USER, uploadPath, targetPath,
                EnumSet.noneOf(CreateOption.class),EnumSet.of(FileAttribute.ACCESS_TIME));

        verify(inner).commitUpload(eq(TEST_USER), eq(uploadPath), eq(targetPath),
                eq(EnumSet.noneOf(CreateOption.class)),
                (Set<FileAttribute>)argThat(hasItem(FileAttribute.ACCESS_TIME)));
        assertThat(result.getAccessTime(), is(equalTo(42L)));
        verify(receiver).notifyMovedEvent(eq(EventType.IN_MOVED_TO), eq(parent),
                eq("file-1"), argThat(not(emptyOrNullString())), eq(FileType.REGULAR));
    }

    @Test
    public void shouldNotNotifyOnUnsuccessfulCommitUpload() throws Exception
    {
        PnfsId parent = new PnfsId("000000000000000000000000000000000001");
        PnfsId target = new PnfsId("000000000000000000000000000000000002");
        FsPath uploadPath = FsPath.create("/uploads/1/update-1/file-1");
        FsPath targetPath = FsPath.create("/data/file-1");
        given(inner.find(any(), eq(target))).willReturn(singleLink(parent, "file-1"));
        given(inner.commitUpload(any(), eq(uploadPath), eq(targetPath), any(), any()))
                .willThrow(CacheException.class);

        try {
            monitor.commitUpload(TEST_USER, uploadPath, targetPath,
                    EnumSet.noneOf(CreateOption.class),EnumSet.of(FileAttribute.ACCESS_TIME));

            fail("commitUpload unexpectedly succeeded");
        } catch (CacheException e) {
        }
        verify(receiver, never()).notifyChildEvent(any(), any(), any(), any());
        verify(receiver, never()).notifySelfEvent(any(), any(), any());
        verify(receiver, never()).notifyMovedEvent(any(), any(), any(), any(), any());
    }


    private Collection<Link> singleLink(PnfsId id, String name)
    {
        return Collections.singleton(new Link(id, name));
    }
}

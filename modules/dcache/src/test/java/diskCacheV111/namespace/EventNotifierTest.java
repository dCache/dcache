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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.junit.After;

import org.dcache.cells.CellStub;
import org.dcache.namespace.events.EventType;
import org.dcache.namespace.FileType;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Executor;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.namespace.events.InotifyEvent;
import org.dcache.events.NotificationMessage;

import static java.util.Collections.singletonList;
import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_ADDED;
import static org.apache.curator.utils.ZKPaths.makePath;
import static org.dcache.namespace.events.EventType.IN_MOVE_SELF;
import static org.dcache.namespace.FileType.REGULAR;
import static org.junit.Assert.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;
import static org.hamcrest.CoreMatchers.*;

public class EventNotifierTest
{
    private EventNotifier notifier;
    private CellStub cellStub;
    private Executor dispatchExecutor;
    private Executor senderExecutor;

    @Before
    public void setup()
    {
        notifier = new EventNotifier();
        cellStub = mock(CellStub.class);
        dispatchExecutor = mock(Executor.class);
        senderExecutor = mock(Executor.class);

        PathChildrenCache cache = mock(PathChildrenCache.class);
        given(cache.getListenable()).willReturn(mock(ListenerContainer.class));
        notifier.setPathChildrenCache(cache);
        notifier.setCellStub(cellStub);
        notifier.setDispatchExecutor(dispatchExecutor);
        notifier.setSenderExecutor(senderExecutor);
        notifier.setEventBatchSize(10); // REVISIT allow tests to modify this?
        notifier.setMaximumQueuedEvents(10); // REVISIT allow tests to modify this?
        notifier.afterStart();
    }

    @After
    public void tearDown()
    {
        notifier.beforeStop();
    }

    private void verifyCellStubNeverSendAndWait() throws CacheException, InterruptedException, NoRouteToCellException
    {
        verify(cellStub, never()).sendAndWait((Message)any());
        verify(cellStub, never()).sendAndWait((CellPath)any(), (Message)any());
        verify(cellStub, never()).sendAndWait((Serializable)any(), (Class<Message>)any());
        verify(cellStub, never()).sendAndWait((Message)any(), anyInt());
        verify(cellStub, never()).sendAndWait((CellPath)any(), (Serializable)any(), (Class<Message>)any());
        verify(cellStub, never()).sendAndWait((CellPath)any(), (Message)any(), anyInt());
        verify(cellStub, never()).sendAndWait((Serializable)any(), (Class<Message>)any(), anyInt());
        verify(cellStub, never()).sendAndWait((CellPath)any(), (Serializable)any(), (Class<Message>)any(), anyInt());
    }

    private void verifyCellStubNeverSend()
    {
        verify(cellStub, never()).send((Message)any());
        verify(cellStub, never()).send((CellPath)any(), (Message)any());
        verify(cellStub, never()).send((Serializable)any(), (Class<Message>)any());
        verify(cellStub, never()).send((Message)any(), anyInt());
        verify(cellStub, never()).send((CellPath)any(), (Serializable)any(), (Class<Message>)any());
        verify(cellStub, never()).send((CellPath)any(), (Message)any(), anyLong());
        verify(cellStub, never()).send((Serializable)any(), (Class<Message>)any(), anyLong());
        verify(cellStub, never()).send((CellPath)any(), (Serializable)any(), (Class<Message>)any(), anyLong());
    }

    private void verifyCellStubNeverNotify()
    {
        verify(cellStub, never()).notify((CellPath)any(), (Serializable)any());
        verifyCellStubNeverUnexpectedNotify();
    }

    private void verifyCellStubNeverUnexpectedNotify()
    {
        verify(cellStub, never()).notify((Serializable)any());
        verify(cellStub, never()).notify((Serializable)any(), anyLong());
        verify(cellStub, never()).notify((CellPath)any(), (Serializable)any(), anyLong());
    }

    private void verifyNoOutboundMessage() throws CacheException, InterruptedException, NoRouteToCellException
    {
        verifyCellStubNeverNotify();
        verifyCellStubNeverSend();
        verifyCellStubNeverSendAndWait();
    }

    private List<NotificationMessage> sentNotificationMessages() throws InterruptedException, CacheException, NoRouteToCellException
    {
        ArgumentCaptor<NotificationMessage> argument = ArgumentCaptor.forClass(NotificationMessage.class);
        verify(cellStub).notify(eq(new CellPath("cell@domain")), argument.capture());
        verifyCellStubNeverUnexpectedNotify();
        verifyCellStubNeverSend();
        verifyCellStubNeverSendAndWait();
        return argument.getAllValues();
    }

    @Test
    public void shouldEnqueueSelfEvent() throws Exception
    {
        notifier.notifySelfEvent(IN_MOVE_SELF, new PnfsId("000000000000000000000000000000000001"),
                REGULAR);

        verify(dispatchExecutor).execute(any());
        verifyNoOutboundMessage();
    }

    @Test
    public void shouldNotQueueWithNoSubscriptions() throws Exception
    {
        givenSelfEventNotified(IN_MOVE_SELF, "000000000000000000000000000000000001", REGULAR);

        whenQueuedDispatchFired();

        verify(senderExecutor, never()).execute(any());
        verifyNoOutboundMessage();
    }

    @Test
    public void shouldStartSenderTaskWhenSubscribedEventFires() throws Exception
    {
        PnfsId target = new PnfsId("000000000000000000000000000000000001");
        givenZooKeeperEvent(CHILD_ADDED, "/dcache/inotify/cell@domain", target, EnumSet.allOf(EventType.class));
        givenSelfEventNotified(IN_MOVE_SELF, target, REGULAR);

        whenQueuedDispatchFired();

        verify(senderExecutor).execute(any());
        verifyNoOutboundMessage();
    }

    @Test
    public void shouldSendEventWhenSubscribedEventFires() throws Exception
    {
        PnfsId target = new PnfsId("000000000000000000000000000000000001");
        givenZooKeeperEvent(CHILD_ADDED, "/dcache/inotify/cell@domain", target, EnumSet.allOf(EventType.class));
        givenSelfEventNotified(IN_MOVE_SELF, target, REGULAR);
        givenQueuedDispatchFired();

        whenSenderTaskFires();

        InotifyEvent expected = new InotifyEvent(EventType.IN_MOVE_SELF, target, null, null, REGULAR);
        NotificationMessage expectedMsg = new NotificationMessage(expected);
        assertThat(sentNotificationMessages(), is(equalTo(singletonList(expectedMsg))));
    }

    private void whenSenderTaskFires()
    {
        givenSenderTaskFired();
    }

    private void givenZooKeeperEvent(PathChildrenCacheEvent.Type type, String path, PnfsId id, Collection<EventType> types)
    {
        byte[] data = EventNotifier.toZkData(Collections.singletonMap(id, types));
        ChildData childData = new ChildData(path, null, data);
        PathChildrenCacheEvent event = new PathChildrenCacheEvent(type, childData);

        try {
            notifier.childEvent(null, event);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Unexpected exception: " + e, e);
        }
    }

    private void whenQueuedDispatchFired()
    {
        givenQueuedDispatchFired();
    }

    private void givenQueuedDispatchFired()
    {
        ArgumentCaptor<Runnable> argument = ArgumentCaptor.forClass(Runnable.class);
        verify(dispatchExecutor).execute(argument.capture());
        argument.getValue().run();
    }

    private void givenSenderTaskFired()
    {
        ArgumentCaptor<Runnable> argument = ArgumentCaptor.forClass(Runnable.class);
        verify(senderExecutor).execute(argument.capture());
        argument.getValue().run();
    }

    private void givenSelfEventNotified(EventType eventType, String id, FileType fileType)
    {
        givenSelfEventNotified(eventType, new PnfsId(id), fileType);
    }

    private void givenSelfEventNotified(EventType eventType, PnfsId id, FileType fileType)
    {
        notifier.notifySelfEvent(eventType, id, fileType);
    }
}

/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package org.dcache.cells;

import static dmg.cells.nucleus.CellEndpoint.SendFlag.PASS_THROUGH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;
import java.util.concurrent.Executor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class CellStubTest {

    private CellEndpoint endpoint;
    private CellStub stub;

    @Before
    public void setup() {
        endpoint = mock(CellEndpoint.class);
        stub = new CellStub(endpoint, new CellPath("destination"));
    }

    @Test
    public void shouldSendNotifications() {
        stub.notify("test");

        ArgumentCaptor<CellMessage> envelope = ArgumentCaptor.forClass(CellMessage.class);
        verify(endpoint).sendMessage(envelope.capture());
        assertThat(envelope.getValue().getMessageObject().toString(), is("test"));
    }


    @Test
    public void shouldTranslateSuccessForMessageCallback() {
        Message message = new Message();
        message.setSucceeded();
        MessageCallback<Message> callback = mock(MessageCallback.class);
        CellStub.addCallback(Futures.immediateFuture(message), callback,
              MoreExecutors.directExecutor());
        verify(callback).setReply(message);
        verify(callback).success();
        verifyNoMoreInteractions(callback);
    }

    @Test
    public void shouldTranslateErrorsForMessageCallback() {
        Message message = new Message();
        message.setReply(1, "failed");
        MessageCallback<Message> callback = mock(MessageCallback.class);
        CellStub.addCallback(Futures.immediateFuture(message), callback,
              MoreExecutors.directExecutor());
        verify(callback).setReply(message);
        verify(callback).failure(1, "failed");
        verifyNoMoreInteractions(callback);
    }

    @Test
    public void shouldNotSpontaneouslyAddFlags() {
        stub.send(new Message());

        ArgumentCaptor<CellMessage> envelope = ArgumentCaptor.forClass(CellMessage.class);
        ArgumentCaptor<CellMessageAnswerable> callback = ArgumentCaptor.forClass(CellMessageAnswerable.class);
        ArgumentCaptor<Executor> executor = ArgumentCaptor.forClass(Executor.class);
        ArgumentCaptor<Long> timeout = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<CellEndpoint.SendFlag> flagArgs = ArgumentCaptor.forClass(CellEndpoint.SendFlag.class);
        verify(endpoint).sendMessage(envelope.capture(), callback.capture(),
                executor.capture(), timeout.capture(), flagArgs.capture());

        assertThat(flagArgs.getAllValues(), empty());
    }


    @Test
    public void shouldUsePassThroughFlagWhenSpecified() {
        stub.setFlags(PASS_THROUGH);

        stub.send(new Message());

        ArgumentCaptor<CellMessage> envelope = ArgumentCaptor.forClass(CellMessage.class);
        ArgumentCaptor<CellMessageAnswerable> callback = ArgumentCaptor.forClass(CellMessageAnswerable.class);
        ArgumentCaptor<Executor> executor = ArgumentCaptor.forClass(Executor.class);
        ArgumentCaptor<Long> timeout = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<CellEndpoint.SendFlag> flagArgs = ArgumentCaptor.forClass(CellEndpoint.SendFlag.class);
        verify(endpoint).sendMessage(envelope.capture(), callback.capture(),
                executor.capture(), timeout.capture(), flagArgs.capture());

        assertThat(flagArgs.getAllValues(), contains(PASS_THROUGH));
    }


    @Test
    public void shouldNotifyToNewDestination() {
        CellStub newStub = stub.withDestination(new CellPath("destination-2"));

        newStub.notify("test");

        ArgumentCaptor<CellMessage> envelope = ArgumentCaptor.forClass(CellMessage.class);
        verify(endpoint).sendMessage(envelope.capture());
        CellMessage msg = envelope.getValue();
        assertThat(msg.getMessageObject().toString(), is("test"));
        assertThat(msg.getDestinationPath(), is(equalTo(new CellPath("destination-2"))));
    }

    @Test
    public void shouldSendToNewDestination() {
        CellStub newStub = stub.withDestination(new CellPath("destination-2"));
        Message msg = new Message();

        newStub.send(msg);

        ArgumentCaptor<CellMessage> envelope = ArgumentCaptor.forClass(CellMessage.class);
        ArgumentCaptor<CellMessageAnswerable> callback = ArgumentCaptor.forClass(CellMessageAnswerable.class);
        ArgumentCaptor<Executor> executor = ArgumentCaptor.forClass(Executor.class);
        ArgumentCaptor<Long> timeout = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<CellEndpoint.SendFlag> flagArgs = ArgumentCaptor.forClass(CellEndpoint.SendFlag.class);
        verify(endpoint).sendMessage(envelope.capture(), callback.capture(),
                executor.capture(), timeout.capture(), flagArgs.capture());
        CellMessage capturedEnvelope = envelope.getValue();
        assertThat(capturedEnvelope.getMessageObject(), is(msg));
        assertThat(capturedEnvelope.getDestinationPath(),
                is(equalTo(new CellPath("destination-2"))));
    }

    @Test
    public void shouldNotSpontaneouslyAddFlagsToNewDestination() {
        CellStub newStub = stub.withDestination(new CellPath("destination-2"));

        newStub.send(new Message());

        ArgumentCaptor<CellMessage> envelope = ArgumentCaptor.forClass(CellMessage.class);
        ArgumentCaptor<CellMessageAnswerable> callback = ArgumentCaptor.forClass(CellMessageAnswerable.class);
        ArgumentCaptor<Executor> executor = ArgumentCaptor.forClass(Executor.class);
        ArgumentCaptor<Long> timeout = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<CellEndpoint.SendFlag> flagArgs = ArgumentCaptor.forClass(CellEndpoint.SendFlag.class);
        verify(endpoint).sendMessage(envelope.capture(), callback.capture(),
                executor.capture(), timeout.capture(), flagArgs.capture());

        assertThat(flagArgs.getAllValues(), empty());
    }

    @Test
    public void shouldPersistPassThroughFlagToNewDestination() {
        stub.setFlags(PASS_THROUGH);
        CellStub newStub = stub.withDestination(new CellPath("destination-2"));

        newStub.send(new Message());

        ArgumentCaptor<CellMessage> envelope = ArgumentCaptor.forClass(CellMessage.class);
        ArgumentCaptor<CellMessageAnswerable> callback = ArgumentCaptor.forClass(CellMessageAnswerable.class);
        ArgumentCaptor<Executor> executor = ArgumentCaptor.forClass(Executor.class);
        ArgumentCaptor<Long> timeout = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<CellEndpoint.SendFlag> flagArgs = ArgumentCaptor.forClass(CellEndpoint.SendFlag.class);
        verify(endpoint).sendMessage(envelope.capture(), callback.capture(),
                executor.capture(), timeout.capture(), flagArgs.capture());

        assertThat(flagArgs.getAllValues(), contains(PASS_THROUGH));
    }
}

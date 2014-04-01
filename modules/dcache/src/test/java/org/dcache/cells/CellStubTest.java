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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class CellStubTest
{
    private CellEndpoint endpoint;
    private CellStub stub;

    @Before
    public void setup()
    {
        endpoint = mock(CellEndpoint.class);
        stub = new CellStub(endpoint, new CellPath("destination"));
    }

    @Test
    public void shouldSendNotifications() throws NoRouteToCellException
    {
        stub.notify("test");

        ArgumentCaptor<CellMessage> envelope = ArgumentCaptor.forClass(CellMessage.class);
        verify(endpoint).sendMessage(envelope.capture());
        assertThat(envelope.getValue().getMessageObject().toString(), is("test"));
    }

    @Test(expected = NoRouteToCellException.class)
    public void shouldRehrowNoRouteToCellExceptionOnNotification() throws NoRouteToCellException
    {
        doThrow(new NoRouteToCellException("")).when(endpoint).sendMessage(any(CellMessage.class));
        stub.notify("test");
    }

    @Test
    public void shouldTranslateSuccessForMessageCallback()
    {
        Message message = new Message();
        message.setSucceeded();
        MessageCallback<Message> callback = mock(MessageCallback.class);
        CellStub.addCallback(Futures.immediateFuture(message), callback, MoreExecutors.sameThreadExecutor());
        verify(callback).setReply(message);
        verify(callback).success();
        verifyNoMoreInteractions(callback);
    }

    @Test
    public void shouldTranslateErrorsForMessageCallback()
    {
        Message message = new Message();
        message.setReply(1, "failed");
        MessageCallback<Message> callback = mock(MessageCallback.class);
        CellStub.addCallback(Futures.immediateFuture(message), callback, MoreExecutors.sameThreadExecutor());
        verify(callback).setReply(message);
        verify(callback).failure(1, "failed");
        verifyNoMoreInteractions(callback);
    }

}

package org.dcache.cells;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.UOID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;


public class MessageReplyTest {

    @Test(expected = NullPointerException.class)
    public void shouldThrowAnExceptionIfDeliveringWithoutEndpointAndEnvelope() {
        MessageReply messageReply = new MessageReply();
        messageReply.deliver(null, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowAnExceptionIfDeliveringWithoutEndpoint() {
        MessageReply messageReply = new MessageReply();
        CellMessage envelope = new CellMessage();

        messageReply.deliver(null, envelope);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowAnExceptionIfDeliveringWithoutEnvelope() {
        MessageReply messageReply = new MessageReply();
        CellEndpoint endpoint = mock(CellEndpoint.class);

        messageReply.deliver(endpoint, null);
    }

    @Test
    public void shouldNotSendIfDeliveringWithoutMessage() {
        MessageReply messageReply = new MessageReply();
        CellMessage envelope = new CellMessage();
        CellEndpoint endpoint = mock(CellEndpoint.class);

        messageReply.deliver(endpoint, envelope);
        verify(endpoint, times(0)).sendMessage(envelope);
    }

    @Test
    public void shouldSendIfDeliveringWithAMessage() {
        MessageReply<Message> messageReply = new MessageReply<>();
        Message message = new Message();
        CellMessage envelope = new CellMessage();
        CellAddressCore pathSource = new CellAddressCore("foo", "source");
        CellEndpoint endpoint = mock(CellEndpoint.class);

        envelope.addSourceAddress(pathSource);

        messageReply.reply(message);
        messageReply.deliver(endpoint, envelope);
        verify(endpoint, times(1)).sendMessage(envelope);
    }

    @Test
    public void shouldBeValidIfEnvelopeIsNull() {
        MessageReply messageReply = new MessageReply();
        assertTrue(messageReply.isValidIn(0));
    }

    @Test
    public void shouldBeValidIfDelayIsSmallerThanTimeToLiveOfEnvelope() {
        MessageReply messageReply = new MessageReply();
        CellMessage envelope = new CellMessage();
        CellEndpoint endpoint = mock(CellEndpoint.class);

        messageReply.deliver(endpoint, envelope);

        long timeToLive = envelope.getTtl();
        long increasedLifetime = timeToLive + TimeUnit.SECONDS.toMillis(1);
        long newTimeToLive = increasedLifetime < timeToLive ? Long.MAX_VALUE : increasedLifetime;

        envelope.setTtl(newTimeToLive);
        assertTrue(messageReply.isValidIn(timeToLive));
    }

    @Test
    public void shouldBeInvalidIfDelayIsBiggerThanTimeToLiveOfEnvelope() {
        MessageReply messageReply = new MessageReply();
        CellMessage envelope = new CellMessage();
        CellEndpoint endpoint = mock(CellEndpoint.class);

        messageReply.deliver(endpoint, envelope);

        long timeToLive = envelope.getTtl();
        long decreasedLifetime = timeToLive - TimeUnit.SECONDS.toMillis(1);
        long newTimeToLive = decreasedLifetime < 0 ? 0 : decreasedLifetime;

        envelope.setTtl(newTimeToLive);
        assertFalse(messageReply.isValidIn(timeToLive));
    }

    @Test
    public void shouldFailWithCacheException() {
        MessageReply<Message> messageReply = new MessageReply<>();
        Message message = mock(Message.class);
        Throwable e = new CacheException("ExceptionMessage");

        messageReply.fail(message, e);
        verify(message,
              times(1)).setFailed(CacheException.DEFAULT_ERROR_CODE, e.getMessage());
    }

    @Test
    public void shouldFailWithIllegalArgumentException() {
        MessageReply<Message> messageReply = new MessageReply<>();
        Message message = mock(Message.class);
        Throwable e = new IllegalArgumentException("ExceptionMessage");

        messageReply.fail(message, e);
        verify(message,
              times(1)).setFailed(CacheException.INVALID_ARGS, e.getMessage());
    }

    @Test
    public void shouldFailWithUnexpectedException() {
        MessageReply<Message> messageReply = new MessageReply<>();
        Message message = mock(Message.class);
        Throwable e = new Exception("ExceptionMessage");

        messageReply.fail(message, e);
        verify(message,
              times(1)).setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
    }

    @Test
    public void shouldSetAFailureWithReturnCodeAndErrorObjectAndReply() {
        MessageReply<Message> messageReply = new MessageReply<>();
        Message message = mock(Message.class);
        CellMessage envelope = new CellMessage();

        messageReply.fail(message, 0, envelope);
        verify(message, times(1)).setFailed(0, envelope);
        verify(message, times(1)).setReply();
    }

    @Test
    public void shouldNotSendIfReplyingWithoutAnEnvelope()
          throws ExecutionException, InterruptedException {
        MessageReply<Message> messageReply = new MessageReply<>();
        Message message = new Message();

        messageReply.reply(message);

        MessageReply<Message> messageReplySpy = spy(messageReply);
        doNothing().when(messageReplySpy).send();

        messageReplySpy.reply(message);
        verify(messageReplySpy, times(0)).send();

        assertEquals(messageReplySpy.get(), message);
        assertTrue(message.isReply());
    }

    @Test
    public void shouldSendIfReplyingWithAnEnvelope()
          throws ExecutionException, InterruptedException {
        MessageReply<Message> messageReply = new MessageReply<>();
        Message message = new Message();
        CellMessage envelope = new CellMessage();
        CellAddressCore pathSource = new CellAddressCore("foo", "source");
        envelope.addSourceAddress(pathSource);
        CellEndpoint endpoint = mock(CellEndpoint.class);

        messageReply.deliver(endpoint, envelope);
        messageReply.reply(message);

        verify(endpoint, times(1)).sendMessage(envelope);
        assertEquals(messageReply.get(), message);
        assertTrue(message.isReply());
    }

    @Test
    public void shouldSendAMessageIfEverythingIsSet() {
        MessageReply<Message> messageReply = new MessageReply<>();
        Message message = new Message();
        CellMessage envelope = new CellMessage();
        CellEndpoint endpoint = mock(CellEndpoint.class);

        CellAddressCore pathSource = new CellAddressCore("foo", "source");
        UOID umit = envelope.getUOID();
        envelope.addSourceAddress(pathSource);

        messageReply.reply(message);
        messageReply.deliver(endpoint, envelope);

        verify(endpoint, times(1)).sendMessage(envelope);

        // TODO Mocking of final classes (like CellMessage) is an incubating feature
        //      and can be done by using a Mockito extension. This would simplify the tests.
        assertEquals("[>foo@source]", envelope.getDestinationPath().toString());
        assertEquals("[empty]", envelope.getSourcePath().toString());
        assertEquals(umit, envelope.getLastUOID());
        assertNotEquals(umit, envelope.getUOID());
        assertTrue(envelope.isReply());

        assertEquals(envelope.getMessageObject(), message);
    }

    @Test
    public void shouldAlwaysReturnFalseIfCancel() {
        MessageReply messageReply = new MessageReply();
        assertFalse(messageReply.cancel(false));
        assertFalse(messageReply.cancel(true));
    }

    @Test
    public void shouldReturnAMessageIfAMessageExists()
          throws ExecutionException, InterruptedException {

        MessageReply<Message> messageReply = new MessageReply<>();
        Message message = new Message();

        messageReply.reply(message);
        assertEquals(message, messageReply.get());
    }

    @Test(expected = ExecutionException.class)
    public void shouldThrowAnExecutionExceptionIfAExceptionOccursDuringTheRequestForAMessage()
          throws ExecutionException, InterruptedException {

        MessageReply<Message> messageReply = new MessageReply<>();
        Message message = new Message();
        Throwable e = new Exception();

        message.setReply(1, e);
        messageReply.reply(message);
        messageReply.get();
    }

    @Test(expected = ExecutionException.class)
    public void shouldThrowAnExecutionExceptionIfAThrowableOccursDuringTheRequestForAMessage()
          throws ExecutionException, InterruptedException {

        MessageReply<Message> messageReply = new MessageReply<>();
        Message message = new Message();
        Throwable e = new Throwable();

        message.setReply(1, e);
        messageReply.reply(message);
        messageReply.get();
    }

    @Test
    public void shouldWaitUntilAMessageIsSet() throws InterruptedException {
        MessageReply<Message> messageReply = new MessageReply<>();
        Message message = new Message();

        Thread thread = new Thread(() -> {
            try {
                messageReply.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
        thread.start();

        TimeUnit.MILLISECONDS.sleep(100);
        assertEquals(Thread.State.WAITING, thread.getState());

        messageReply.reply(message);

        TimeUnit.MILLISECONDS.sleep(100);
        assertEquals(Thread.State.TERMINATED, thread.getState());
    }

    @Test
    public void shouldReturnAMessageIfAMessageIsSetInACertainTimeInterval()
          throws ExecutionException, InterruptedException, TimeoutException {

        MessageReply<Message> messageReply = new MessageReply<>();
        Message message = new Message();

        messageReply.reply(message);
        assertEquals(message, messageReply.get(0, TimeUnit.SECONDS));
    }

    @Test(expected = TimeoutException.class)
    public void shouldThrowATimeoutExceptionIfAMessageIsNotSetInACertainlyTimeInterval()
          throws InterruptedException, ExecutionException, TimeoutException {

        MessageReply messageReply = new MessageReply();
        messageReply.get(0, TimeUnit.SECONDS);
    }

    @Test
    public void shouldWaitForACertainTimeOrUntilAMessageIsSet() throws InterruptedException {
        MessageReply<Message> messageReply = new MessageReply<>();
        Message message = new Message();

        Thread thread = new Thread(() -> {
            try {
                messageReply.get(1, TimeUnit.SECONDS);
            } catch (ExecutionException | InterruptedException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        thread.start();

        TimeUnit.MILLISECONDS.sleep(100);
        assertEquals(Thread.State.TIMED_WAITING, thread.getState());

        messageReply.reply(message);

        TimeUnit.MILLISECONDS.sleep(100);
        assertEquals(Thread.State.TERMINATED, thread.getState());
    }

    @Test
    public void shouldAlwaysReturnFalseIfCancelled() {
        MessageReply messageReply = new MessageReply();
        assertFalse(messageReply.isCancelled());
    }

    @Test
    public void shouldReturnFalseIfNoMessageIsSet() {
        MessageReply messageReply = new MessageReply();
        assertFalse(messageReply.isDone());
    }

    @Test
    public void shouldReturnTrueIfAMessageIsSet() {
        MessageReply<Message> messageReply = new MessageReply<>();
        Message message = new Message();

        messageReply.reply(message);
        assertTrue(messageReply.isDone());
    }
}
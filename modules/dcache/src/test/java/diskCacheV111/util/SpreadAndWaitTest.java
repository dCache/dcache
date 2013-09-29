package diskCacheV111.util;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.Serializable;

import dmg.cells.nucleus.CellPath;

import org.dcache.cells.CellStub;
import org.dcache.cells.MessageCallback;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SpreadAndWaitTest
{
    @Test
    public void testNext() throws InterruptedException
    {
        CellStub stub = mock(CellStub.class);
        doAnswer(new InvokesSuccess()).when(stub).send(any(CellPath.class), any(Serializable.class), any(Class.class), any(MessageCallback.class));
        SpreadAndWait<String> sut = new SpreadAndWait<>(stub);
        sut.send(new CellPath("test"), String.class, "test");
        assertThat(sut.next(), is("test"));
        assertThat(sut.next(), is(nullValue()));
    }

    private static class InvokesSuccess implements Answer
    {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable
        {
            Object[] arguments = invocationOnMock.getArguments();
            MessageCallback<Object> callback = (MessageCallback<Object>) arguments[3];
            callback.setReply(arguments[1]);
            callback.success();
            return null;
        }
    }
}

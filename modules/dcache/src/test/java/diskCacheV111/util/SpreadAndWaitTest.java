package diskCacheV111.util;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.Serializable;

import dmg.cells.nucleus.CellPath;

import org.dcache.cells.CellStub;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpreadAndWaitTest
{
    @Test
    public void testNext() throws InterruptedException
    {
        CellStub stub = mock(CellStub.class);
        when(stub.send(any(CellPath.class), any(Serializable.class), any(Class.class))).thenAnswer(new InvokesSuccess<>());
        SpreadAndWait<String> sut = new SpreadAndWait<>(stub);
        sut.send(new CellPath("test"), String.class, "test");
        assertThat(sut.next(), is("test"));
        assertThat(sut.next(), is(nullValue()));
    }

    private static class InvokesSuccess<T> implements Answer<ListenableFuture<T>>
    {
        @Override
        public ListenableFuture<T> answer(InvocationOnMock invocationOnMock) throws Throwable
        {
            Object[] arguments = invocationOnMock.getArguments();
            return Futures.immediateFuture((T) arguments[1]);
        }
    }
}

package org.dcache.tests.util;

import static org.junit.Assert.*;

import org.junit.*;


import diskCacheV111.util.CacheException;
import org.dcache.cells.CellMessageDispatcher;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

public class CellMessageDispatcherTest
{
    private CellMessageDispatcher dispatcher;
    private Listener1 listener1;
    private Listener2 listener2;

    Object message1 = new Message1();
    Object message2 = new Message2();
    Object message3 = new Message3();
    Object message4 = new Message4();
    Object message5 = new Message5();
    Object message6 = new Message6();
    Object message7 = new Message7();
    Object message8 = new Message8();
    Object message9 = new Message9();

    class Message1 {}
    class Message2 {}
    class Message3 {}
    class Message4 {}
    class Message5 extends Message4 {}
    class Message6 {}
    class Message7 {}
    class Message8 {}
    class Message9 {}

    public class Listener1
    {
        int delivered;

        public void messageArrived(Message1 o)
        {
            assertEquals(delivered, 0);
            delivered = 1;
        }

        public void messageArrived(Message3 o)
        {
            assertEquals(delivered, 0);
            delivered = 3;
        }

        public void messageArrived(Message4 o)
        {
            assertEquals(delivered, 0);
            delivered = 4;
        }

        public void messageArrived(Message5 o)
        {
            assertEquals(delivered, 0);
            delivered = 5;
        }

        public int messageArrived(Message6 o)
        {
            assertEquals(delivered, 0);
            delivered = 6;
            return 0;
        }

        public int messageArrived(Message7 o)
        {
            assertEquals(delivered, 0);
            delivered = 7;
            return 0;
        }

        public void messageArrived(Message8 o)
            throws CacheException
        {
            assertEquals(delivered, 0);
            delivered = 8;
            throw new CacheException(0, "bla");
        }

        public void messageArrived(Message9 o)
            throws CacheException
        {
            assertEquals(delivered, 0);
            delivered = 9;
            throw new CacheException(0, "bla");
        }
    }

    public class Listener2
    {
        int delivered;

        public void messageArrived(Message3 o)
        {
            assertEquals(delivered, 0);
            delivered = 3;
        }

        public void messageArrived(Message4 o)
        {
            assertEquals(delivered, 0);
            delivered = 4;
        }

        public void messageArrived(Message6 o)
        {
            assertEquals(delivered, 0);
            delivered = 6;
        }

        public int messageArrived(Message7 o)
        {
            assertEquals(delivered, 0);
            delivered = 7;
            return 0;
        }

        public void messageArrived(Message9 o)
            throws CacheException
        {
            assertEquals(delivered, 0);
            delivered = 9;
            throw new CacheException(0, "bla");
        }
    }


    @Before
    public void setUp()
    {
        dispatcher = new CellMessageDispatcher("messageArrived");
        listener1 = new Listener1();
        listener2 = new Listener2();
        dispatcher.addMessageListener(listener1);
        dispatcher.addMessageListener(listener2);
    }

    private Object deliver(Object msg, int result1, int result2)
    {
        try {
            return
                dispatcher.call(new CellMessage(new CellPath(""), msg));
        } finally {
            assertEquals(listener1.delivered, result1);
            assertEquals(listener2.delivered, result2);
        }
    }

    @Test
    public void testDeliverMessage()
    {
        Object o = deliver(message1, 1, 0);
        assertEquals(o, null);
    }

    @Test
    public void testDeliverMessageNoReceiver()
    {
        Object o = deliver(message2, 0, 0);
        assertEquals(o, null);
    }

    @Test
    public void testDeliverMessageMultipleReceivers()
    {
        Object o = deliver(message3, 3, 3);
        assertEquals(o, null);
    }

    @Test
    public void testDeliverMostSpecific()
    {
        Object o = deliver(message5, 5, 4);
        assertEquals(o, null);
    }

    @Test
    public void testReturnValue()
    {
        Object o = deliver(message6, 6, 6);
        assertEquals(0, o);
    }

    @Test(expected=RuntimeException.class)
    public void testMultipleReturnValues()
    {
        Object o = deliver(message7, 7, 7);
        assertEquals(o, null);
    }

    @Test
    public void testExceptionReturned()
    {
        Object o = deliver(message8, 8, 0);
        assertTrue(o instanceof CacheException);
    }

    @Test(expected=RuntimeException.class)
    public void testMultipleExceptions()
    {
        deliver(message9, 9, 9);
    }

}

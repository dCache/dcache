package org.dcache.tests.util;

import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import org.dcache.cells.CellMessageDispatcher;
import dmg.cells.nucleus.CellMessageReceiver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CellMessageDispatcherTest
{
    private CellMessageDispatcher dispatcher;
    private Listener1 listener1;
    private Listener2 listener2;

    Serializable message1 = new Message1();
    Serializable message2 = new Message2();
    Serializable message3 = new Message3();
    Serializable message4 = new Message4();
    Serializable message5 = new Message5();
    Serializable message6 = new Message6();
    Serializable message7 = new Message7();
    Serializable message8 = new Message8();
    Serializable message9 = new Message9();

    class Message1 implements Serializable {}
    class Message2 implements Serializable {}
    class Message3 implements Serializable {}
    class Message4 implements Serializable {}
    class Message5 extends Message4 {}
    class Message6 implements Serializable {}
    class Message7 implements Serializable {}
    class Message8 implements Serializable {}
    class Message9 implements Serializable {}

    public class Listener1 implements CellMessageReceiver
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

    public class Listener2 implements CellMessageReceiver
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

    private Object deliver(Serializable msg, int result1, int result2)
    {
        try {
            return
                dispatcher.call(new CellMessage(new CellPath("test"), msg));
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

package org.dcache.tests.cells;

import static org.junit.Assert.*;

import com.google.common.primitives.Ints;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.NoRouteToCellException;

import java.io.Serializable;
import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

/**
 * Abstract helper class for creating cell stubs for testing.
 *
 * The intention is that this class is subclassed for each test case.
 * The test case is implemented in the <code>run</code> method, which
 * is executed as part of the stub constructor.
 *
 * The subclass implements a number of message handlers. A message
 * handler is a method annotated with <code>@Message</code>. The stub
 * will create a cell for the message handler and direct messages to
 * the handler. A handler is used exactly once. If the same message is
 * expected several times, several message handlers must be defined.
 *
 * To precisely test the cell interaction, the test is split into a
 * number of steps. The test starts at step 0. Each message handler is
 * bound to a step and the test is automatically advanced to the next
 * step matching the message handler used. In case several message
 * handlers apply, the one with the lowest step is used. The
 * <code>run</code> method can assert the progress of the test by
 * calling <code>assertStep</code>.
 *
 */
public abstract class CellStubHelper
{
    /**
     * This class wraps methods for handling message.  Handlers
     * implement the chain of command pattern.
     */
    class Handler implements Comparable<Handler>
    {
        /** The method this handler wraps. */
        private final Method _method;

        /** The annotation of the method. */
        private final Message _annotation;

        /** Whether the handler has been called. */
        private boolean _used;

        public Handler(Method method, Message annotation)
        {
            _method = method;
            _method.setAccessible(true);
            _annotation = annotation;
            _used = false;
        }

        public boolean isRequired()
        {
            return _annotation.required();
        }

        public boolean isUsed()
        {
            return _used;
        }

        public int getStep()
        {
            return _annotation.step();
        }

        public String getCellName()
        {
            return _annotation.cell();
        }

        /** Handlers are ordered according to the step annotation. */
        @Override
        public int compareTo(Handler handler)
        {
            return Ints.compare(getStep(), handler.getStep());
        }

        public boolean call(CellMessage msg)
            throws IllegalAccessException, InvocationTargetException
        {
            /* Cannot use handler belonging to a previous step.
             */
            if (_step > getStep()) {
                return false;
            }

            /* Can only use handler once.
             */
            if (_used) {
                return false;
            }

            /* Only accept messages sent to the cell for this messages
             * was written.
             */
            CellPath dest = msg.getDestinationPath();
            String cell = getCellName();
            if (!cell.equals(dest.getCellName())) {
                return false;
            }

            /* Advance step. Will fail if required handlers belonging
             * to earlier steps have not been called.
             */
            _used = true;
            assertStep("Required messages missing", getStep(), 0);

            /* Deliver message.
             */
            try {
                Serializable obj = msg.getMessageObject();
                obj = (Serializable) _method.invoke(CellStubHelper.this, obj);

                if (obj != null) {
                    msg.revertDirection();
                    msg.setMessageObject(obj);
                    send(cell, msg);
                }
            } catch (IllegalArgumentException e) {
                /* Handler parameters didn't match message object.
                 */
                return false;
            }

            return true;
        }

        public String toString()
        {
            return _method.toString();
        }
    }

    protected final List<Handler> _handlers = new ArrayList<>();

    protected final Map<String, CellAdapterHelper> _cells =
        new HashMap<>();

    protected int _step;

    protected Throwable _failed;

    public CellStubHelper()
        throws Throwable
    {
        _step = 0;

        try {
            /* Collect message handlers.
             */
            Set<String> cells = new HashSet<>();
            for (Method method : getClass().getDeclaredMethods()) {
                Message annotation = method.getAnnotation(Message.class);
                if (annotation != null) {
                    cells.add(annotation.cell());
                    _handlers.add(new Handler(method, annotation));
                }
            }
            Collections.sort(_handlers);

            /* Create cells.
             */
            for (String cell : cells) {
                _cells.put(cell, new CellAdapterHelper(cell, "") {
                        @Override
                        public void messageArrived(CellMessage msg)
                        {
                            CellStubHelper.this.messageArrived(msg);
                        }
                    });
            }

            /* Execute the test.
             */
            run();

            assertStep("Required messages missing", Integer.MAX_VALUE);
        } finally {
            /* Shut down the nuclei.
             */
            for (CellAdapterHelper cell : _cells.values()) {
                cell.die();
            }

            if (_failed != null) {
                throw _failed;
            }
        }
    }

    synchronized public void messageArrived(CellMessage msg)
    {
        try {
            try {
                for (Handler handler : _handlers) {
                    if (handler.call(msg)) {
                        return;
                    }
                }

                fail("Unexpected message: " + msg);
            } catch (IllegalAccessException e) {
                fail("Unexpected failure while invoking message handler: " +
                     e.getMessage());
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        } catch (Throwable e) {
            _failed = e;
            String cell = msg.getDestinationPath().getCellName();
            msg.revertDirection();
            msg.setMessageObject(e);
            send(cell, msg);
        }
    }

    protected void send(String sender, CellMessage msg)
    {
        try {
            _cells.get(sender).sendMessage(msg);
        } catch (NoRouteToCellException e) {
            fail("No route to cell: " + e.getMessage());
        }
    }

    protected void assertStep(String message, int step)
    {
        assertStep(message, step, 100);
    }

    protected void assertStep(String message, int step, long timeout)
    {
        /* We need to make sure that messages have actually been
         * delivered. Currently I cannot find a better way than to
         * sleep for a moment... (REVISIT).
         */
        try {
            Thread.currentThread().sleep(timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertTrue(message, _step <= step);

        for (Handler handler: _handlers) {
            assertTrue("Required message missing: " + handler,
                       !handler.isRequired()
                       || handler.isUsed()
                       || handler.getStep() >= step);
        }

        _step = step;
    }

    protected abstract void run() throws Exception;
}

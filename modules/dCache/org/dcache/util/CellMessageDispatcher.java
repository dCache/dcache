package org.dcache.util;

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;

import java.lang.reflect.Method;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import dmg.cells.nucleus.CellMessage;
import diskCacheV111.vehicles.Message;

import org.dcache.util.ReflectionUtils;

/**
 * Helper class for message dispatching. Used internally in
 * CellMessageDispatcher;
 */
abstract class Receiver
{
    final protected Object _object;
    final protected Method _method;

    public Receiver(Object object, Method method)
    {
        _object = object;
        _method = method;
    }

    abstract public void deliver(CellMessage envelope, Message message)
        throws IllegalAccessException, InvocationTargetException;

    public String toString()
    {
        return String.format("Object: %1$s; Method: %2$s", _object, _method);
    }
}

class ShortReceiver extends Receiver
{
    public ShortReceiver(Object object, Method method)
    {
        super(object, method);
    }

    public void deliver(CellMessage envelope, Message message)
        throws IllegalAccessException, InvocationTargetException
    {
        _method.invoke(_object, message);
    }
}


class LongReceiver extends Receiver
{
    public LongReceiver(Object object, Method method)
    {
        super(object, method);
    }

    public void deliver(CellMessage envelope, Message message)
        throws IllegalAccessException, InvocationTargetException
    {
        _method.invoke(_object, envelope, message);
    }
}


/**
 * Automatic dispatch of dCache messages to message handlers.
 */
public class CellMessageDispatcher
{
    /** Cached message handlers for fast dispatch. */
    private final Map<Class,Collection<Receiver>> _receivers =
        new HashMap<Class,Collection<Receiver>>();

    /**
     * Registered message listeners.
     *
     * @see addMessageListener
     */
    private final Collection<Object> _messageListeners =
        new CopyOnWriteArrayList<Object>();

    public CellMessageDispatcher()
    {

    }

    /**
     * Returns true if <code>c</code> has a method
     * <code>messageArrived</code> suitable for message delivery.
     */
    private boolean hasMessageArrived(Class c)
    {
        for (Method m : c.getMethods()) {
            if (m.getName().equals("messageArrived")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds a listener for dCache messages.
     *
     * The object is scanned for public methods named
     * <code>messageArrived(diskCacheV111.vehicles.Message)</code> or
     * <code>messageArrived(CellMessage,
     * diskCacheV111.vehicles.Message)</code>, where <code>CellMessage</code>
     * is the envelope or context containing the message of type
     * <code>Message</code>).
     *
     * After registration, all cell messages with a message object
     * matching the type of the argument will be send to object.
     *
     * Message dispatching is performed in
     * <code>messageArrived</code>. If that method is overridden in
     * derivatives, the derivative must make sure that
     * <code>messageArrived</code> is still called.
     */
    public void addMessageListener(Object o)
    {
        Class c = o.getClass();
        if (hasMessageArrived(c)) {
            synchronized (_receivers) {
                if (_messageListeners.add(o)) {
                    _receivers.clear();
                }
            }
        }
    }

    /**
     * Removes a listener previously added with addMessageListener.
     */
    public void removeMessageListener(Object o)
    {
        synchronized (_receivers) {
            if (_messageListeners.remove(o)) {
                _receivers.clear();
            }
        }

    }

    /**
     * Finds the objects and methods, in other words the receivers, of
     * messages of a given type.
     *
     * FIXME: This is still not quite the right thing: if you have
     * messageArrived(CellMessage, X) and messageArrived(Y) and Y is
     * more specific than X, then you would expect the latter to be
     * called for message Y. This is not yet the case.
     */
    private Collection<Receiver> findReceivers(Class c)
    {
        synchronized (_receivers) {
            Collection<Receiver> receivers = new ArrayList<Receiver>();
            for (Object listener : _messageListeners) {
                Method m = ReflectionUtils.resolve(listener.getClass(),
                                                   "messageArrived",
                                                   CellMessage.class, c);
                if (m != null) {
                    receivers.add(new LongReceiver(listener, m));
                    continue;
                }

                m = ReflectionUtils.resolve(listener.getClass(),
                                            "messageArrived",
                                            c);
                if (m != null) {
                    receivers.add(new ShortReceiver(listener, m));
                }
            }
            return receivers;
        }
    }

    /**
     * Delivers messages to registered message listeners.
     */
    public void messageArrived(CellMessage envelope)
        throws InvocationTargetException
    {
        Object message = envelope.getMessageObject();
        if (message instanceof Message) {
            Class c = message.getClass();
            Collection<Receiver> receivers;

            synchronized (_receivers) {
                receivers = _receivers.get(c);
                if (receivers == null) {
                    receivers = findReceivers(c);
                    _receivers.put(c, receivers);
                }
            }

            for (Receiver receiver : receivers) {
                try {
                    receiver.deliver(envelope, (Message)message);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Cannot process message due to access error", e);
                }
            }
        }
    }
}

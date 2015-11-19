package org.dcache.cells;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.util.ReflectionUtils;

/**
 * Automatic dispatch of dCache messages to message handlers.
 */
public class CellMessageDispatcher
{
    /** Cached message handlers for fast dispatch. */
    private final Map<Class<? extends Serializable>, Collection<Receiver>>
            _receivers = new HashMap<>();

    /** Name of receiver methods. */
    private final String _receiverName;

    /**
     * Registered message listeners.
     *
     * @see addMessageListener
     */
    private final Collection<CellMessageReceiver> _messageListeners =
        new CopyOnWriteArrayList<>();

    public CellMessageDispatcher(String receiverName)
    {
        _receiverName = receiverName;
    }

    /**
     * Returns true if <code>c</code> has a method suitable for
     * message delivery.
     */
    private boolean hasListener(Class<? extends CellMessageReceiver> c)
    {
        for (Method m : c.getMethods()) {
            if (m.getName().equals(_receiverName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds a listener for dCache messages.
     *
     * The object is scanned for public methods with the signature
     * <code>name(Object message)</code> or <code>name(CellMessage
     * envelope, Object message)</code>, where <code>name</code> is
     * the receiver name, <code>envelope</code> is the envelope
     * containing the message.
     *
     * After registration, all cell messages with a message object
     * matching the type of the argument will be send to object.
     *
     * Message dispatching is performed in the <code>call</code>
     * method. If that method is overridden in derivatives, the
     * derivative must make sure that <code>call</code> is still
     * called.
     */
    public void addMessageListener(CellMessageReceiver o)
    {
        Class<? extends CellMessageReceiver> c = o.getClass();
        if (hasListener(c)) {
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
    public void removeMessageListener(CellMessageReceiver o)
    {
        synchronized (_receivers) {
            if (_messageListeners.remove(o)) {
                _receivers.clear();
            }
        }
    }

    /**
     * Returns the message types that can be reveived by an object of
     * the given class.
     */
    public Collection<Class<? extends Serializable>> getMessageTypes(Object o)
    {
        Class<?> c = o.getClass();
        Collection<Class<? extends Serializable>> types = new ArrayList<>();

        for (Method method : c.getMethods()) {
            if (method.getName().equals(_receiverName)) {
                Class<?>[] parameterTypes = method.getParameterTypes();
                switch (parameterTypes.length) {
                case 1:
                    types.add(parameterTypes[0].asSubclass(Serializable.class));
                    break;
                case 2:
                    if (CellMessage.class.isAssignableFrom(parameterTypes[0])) {
                        types.add(parameterTypes[1].asSubclass(Serializable.class));
                    }
                    break;
                }
            }
        }
        return types;
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
    private Collection<Receiver> findReceivers(Class<?> c)
    {
        synchronized (_receivers) {
            Collection<Receiver> receivers = new ArrayList<>();
            for (CellMessageReceiver listener : _messageListeners) {
                Method m = ReflectionUtils.resolve(listener.getClass(),
                                                   _receiverName,
                                                   CellMessage.class, c);
                if (m != null) {
                    m.setAccessible(true);
                    receivers.add(new LongReceiver(listener, m));
                    continue;
                }

                m = ReflectionUtils.resolve(listener.getClass(),
                                            _receiverName,
                                            c);
                if (m != null) {
                    m.setAccessible(true);
                    receivers.add(new ShortReceiver(listener, m));
                }
            }
            return receivers;
        }
    }

    private String multipleRepliesError(Collection<Receiver> receivers, Object message)
    {
        return String.format("Processing of message [%s] of type %s failed: Multiple replies were generated by %s.", message, message.getClass().getName(), receivers);
    }

    /**
     * Delivers messages to registered message listeners. The return
     * value is determined by the following rules (in order):
     *
     * 1. If any message listener throws an unchecked exception other
     *    than IllegalArgumentException or IllegalStateException, that
     *    exception is rethrown.
     *
     * 2. If more than one message listener returns a non-null value
     *    or throws a checked exception, IllegalArgumentException or
     *    IllegalStateException, then a RuntimeException is thrown
     *    reporting that multiple replies have been generated. This is
     *    a coding error.
     *
     * 3. If one message listener returns a non null value then that
     *    value is returned. If one message listener throws a checked
     *    exception, IllegalArgumentException or
     *    IllegalStateException, then that exception is returned.
     *
     * 4. Otherwise null is returned.
     */
    public Object call(CellMessage envelope)
    {
        Serializable message = envelope.getMessageObject();
        Class<? extends Serializable> c = message.getClass();
        Collection<Receiver> receivers;

        synchronized (_receivers) {
            receivers = _receivers.get(c);
            if (receivers == null) {
                receivers = findReceivers(c);
                _receivers.put(c, receivers);
            }
        }

        Object result = null;
        for (Receiver receiver : receivers) {
            try {
                Object obj = receiver.deliver(envelope, message);
                if (obj != null) {
                    if (result != null) {
                        throw new RuntimeException(multipleRepliesError(receivers, message));
                    }
                    result = obj;
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Cannot process message due to access error", e);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IllegalArgumentException ||
                    cause instanceof IllegalStateException ||
                    receiver.isDeclaredToThrow(cause.getClass())) {
                    /* We recognize IllegalArgumentException,
                     * IllegalStateException, and any exception
                     * declared to be thrown by the method as part of
                     * the public contract of the receiver and
                     * propagate the exception back to the client.
                     */
                } else if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                } else if (cause instanceof Error) {
                    throw (Error) cause;
                } else {
                    /* Since any Throwable not a RuntimeException and
                     * not an Error should have been declared to be
                     * thrown by the method, this branch should be
                     * unreachable.
                     */
                    throw new RuntimeException("Bug: This should have been unreachable. Please report to support@dcache.org.", cause);
                }

                if (result != null) {
                    throw new RuntimeException(multipleRepliesError(receivers, message));
                }
                result = cause;
            }
        }

        return result;
    }

    /**
     * Helper class for message dispatching.
     */
    abstract static class Receiver
    {
        protected final CellMessageReceiver _object;
        protected final Method _method;

        public Receiver(CellMessageReceiver object, Method method)
        {
            _object = object;
            _method = method;
        }

        public abstract Object deliver(CellMessage envelope, Object message)
            throws IllegalAccessException, InvocationTargetException;

        public String toString()
        {
            return String.format("Object: %1$s; Method: %2$s", _object, _method);
        }

        public boolean isDeclaredToThrow(Class<?> exceptionClass)
        {
            for (Class<?> clazz: _method.getExceptionTypes()) {
                if (clazz.isAssignableFrom(exceptionClass)) {
                    return true;
                }
            }
            return false;
        }
    }

    static class ShortReceiver extends Receiver
    {
        public ShortReceiver(CellMessageReceiver object, Method method)
        {
            super(object, method);
        }

        @Override
        public Object deliver(CellMessage envelope, Object message)
            throws IllegalAccessException, InvocationTargetException
        {
            return _method.invoke(_object, message);
        }
    }


    static class LongReceiver extends Receiver
    {
        public LongReceiver(CellMessageReceiver object, Method method)
        {
            super(object, method);
        }

        @Override
        public Object deliver(CellMessage envelope, Object message)
            throws IllegalAccessException, InvocationTargetException
        {
            return _method.invoke(_object, envelope, message);
        }
    }
}

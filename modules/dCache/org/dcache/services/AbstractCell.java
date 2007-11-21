package org.dcache.services;

import org.apache.log4j.Logger;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collection;
import java.math.BigInteger;

import java.lang.reflect.Method;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;
import diskCacheV111.vehicles.Message;

/**
 * Helper class for message dispatching. Used internally in
 * AbstractCell.
 */
class Receiver
{
    final protected Object _object;
    final protected Method _method;

    public Receiver(Object object, Method method)
    {
        _object = object;
        _method = method;
    }

    public void deliver(Message message)
        throws IllegalAccessException,
               InvocationTargetException
    {
        _method.invoke(_object, message);
    }
}

/**
 * Abstract cell implementation providing features needed by many
 * dCache cells:
 *
 * * Automatic dispatch of dCache messages to message handler.
 *
 * * Annotation based option parsing.
 */
public class AbstractCell extends CellAdapter
{
    /**
     * Logger for the package of the instantiated class. Notice that
     * this is not a static field, as the logger instance to use
     * depends on the particular instance of this class.
     */
    protected Logger _logger;

    /** Message handler for fast dispatch. */
    final private Map<Class,Collection<Receiver>> _receivers =
        new HashMap<Class,Collection<Receiver>>();

    public AbstractCell(String cellName, String args, boolean startNow)
    {
        super(cellName, args, startNow);

        _logger =
            Logger.getLogger(getClass().getPackage().getName());

        parseOptions();

        addMessageListener(this);
    }

    public void debug(String str)
    {
        _logger.debug(str);
    }

    public void info(String str)
    {
        pin(str);
        _logger.info(str);
    }

    public void warn(String str)
    {
        pin(str);
        _logger.warn(str);
    }

    public void error(String str)
    {
        pin(str);
        _logger.error(str);
    }

    /**
     * Convert an instance to a specific type (kind of intelligent
     * casting).  Note: you can set primitive types as input
     * <i>type</i> but the return type will be the corresponding
     * wrapper type (e.g. Integer.TYPE will result in Integer.class)
     * with the difference that instead of a result 'null' a numeric 0
     * (or boolean false) will be returned because primitive types
     * can't be null.
     *
     * <p>
     * Supported simple destination types are:
     * <ul>
     * <li>java.lang.Boolean, Boolean.TYPE (= boolean.class)
     * <li>java.lang.Byte, Byte.TYPE (= byte.class)
     * <li>java.lang.Character, Character.TYPE (= char.class)
     * <li>java.lang.Double, Double.TYPE (= double.class)
     * <li>java.lang.Float, Float.TYPE (= float.class)
     * <li>java.lang.Integer, Integer.TYPE (= int.class)
     * <li>java.lang.Long, Long.TYPE (= long.class)
     * <li>java.lang.Short, Short.TYPE (= short.class)
     * <li>java.lang.String
     * <li>java.math.BigDecimal
     * <li>java.math.BigInteger
     * </ul>
     *
     * @param object Instance to convert.
     * @param type Destination type (e.g. Boolean.class).
     * @return Converted instance/datatype/collection or null if
     *         input object is null.
     * @throws ClassCastException if <i>object</i> can't be converted to
     *                            <i>type</i>.
     * @author MartinHilpert at SUN's Java Forum
     */
    @SuppressWarnings("unchecked")
    static public <T> T toType(final Object object, final Class<T> type)
    {
        T result = null;

        if (object == null) {
            //initalize primitive types:
            if (type == Boolean.TYPE) {
                result = ((Class<T>) Boolean.class).cast(false);
            } else if (type == Byte.TYPE) {
                result = ((Class<T>) Byte.class).cast(0);
            } else if (type == Character.TYPE) {
                result = ((Class<T>) Character.class).cast(0);
            } else if (type == Double.TYPE) {
                result = ((Class<T>) Double.class).cast(0.0);
            } else if (type == Float.TYPE) {
                result = ((Class<T>) Float.class).cast(0.0);
            } else if (type == Integer.TYPE) {
                result = ((Class<T>) Integer.class).cast(0);
            } else if (type == Long.TYPE) {
                result = ((Class<T>) Long.class).cast(0);
            } else if (type == Short.TYPE) {
                result = ((Class<T>) Short.class).cast(0);
            }
        } else {
            final String so = object.toString();

            //custom type conversions:
            if (type == BigInteger.class) {
                result = type.cast(new BigInteger(so));
            } else if (type == Boolean.class || type == Boolean.TYPE) {
                Boolean r = null;
                if ("1".equals(so) || "true".equalsIgnoreCase(so) || "yes".equalsIgnoreCase(so) || "on".equalsIgnoreCase(so) || "enabled".equalsIgnoreCase(so)) {
                    r = Boolean.TRUE;
                } else if ("0".equals(object) || "false".equalsIgnoreCase(so) || "no".equalsIgnoreCase(so) || "off".equalsIgnoreCase(so) || "disabled".equalsIgnoreCase(so)) {
                    r = Boolean.FALSE;
                } else {
                    r = Boolean.valueOf(so);
                }

                if (type == Boolean.TYPE) {
                    result = ((Class<T>) Boolean.class).cast(r); //avoid ClassCastException through autoboxing
                } else {
                    result = type.cast(r);
                }
            } else if (type == Byte.class || type == Byte.TYPE) {
                Byte i = Byte.valueOf(so);
                if (type == Byte.TYPE) {
                    result = ((Class<T>) Byte.class).cast(i); //avoid ClassCastException through autoboxing
                } else {
                    result = type.cast(i);
                }
            } else if (type == Character.class || type == Character.TYPE) {
                Character i = new Character(so.charAt(0));
                if (type == Character.TYPE) {
                    result = ((Class<T>) Character.class).cast(i); //avoid ClassCastException through autoboxing
                } else {
                    result = type.cast(i);
                }
            } else if (type == Double.class || type == Double.TYPE) {
                Double i = Double.valueOf(so);
                if (type == Double.TYPE) {
                    result = ((Class<T>) Double.class).cast(i); //avoid ClassCastException through autoboxing
                } else {
                    result = type.cast(i);
                }
            } else if (type == Float.class || type == Float.TYPE) {
                Float i = Float.valueOf(so);
                if (type == Float.TYPE) {
                    result = ((Class<T>) Float.class).cast(i); //avoid ClassCastException through autoboxing
                } else {
                    result = type.cast(i);
                }
            } else if (type == Integer.class || type == Integer.TYPE) {
                Integer i = Integer.valueOf(so);
                if (type == Integer.TYPE) {
                    result = ((Class<T>) Integer.class).cast(i); //avoid ClassCastException through autoboxing
                } else {
                    result = type.cast(i);
                }
            } else if (type == Long.class || type == Long.TYPE) {
                Long i = Long.valueOf(so);
                if (type == Long.TYPE) {
                    result = ((Class<T>) Long.class).cast(i); //avoid ClassCastException through autoboxing
                } else {
                    result = type.cast(i);
                }
            } else if (type == Short.class || type == Short.TYPE) {
                Short i = Short.valueOf(so);
                if (type == Short.TYPE) {
                    result = ((Class<T>) Short.class).cast(i); //avoid ClassCastException through autoboxing
                } else {
                    result = type.cast(i);
                }
            } else {
                try {
                    Constructor<T> constructor =
                        type.getConstructor(String.class);
                    result = constructor.newInstance(object);
                } catch (NoSuchMethodException e) {
                    //hard cast:
                    result = type.cast(object);
                } catch (SecurityException e) {
                    //hard cast:
                    result = type.cast(object);
                } catch (InstantiationException e) {
                    //hard cast:
                    result = type.cast(object);
                } catch (IllegalAccessException e) {
                    //hard cast:
                    result = type.cast(object);
                } catch (InvocationTargetException e) {
                    //hard cast:
                    result = type.cast(object);
                }
            }
        }

        return result;
    }

    /**
     * Returns the value of an option. If the option is found as a
     * cell argument, the value is taken from there. Otherwise it is
     * taken from the domain context, if found.
     *
     * @param name the name of the option
     * @param required if true, an exception is thrown if the option
     *                 is not defined
     * @return the value of the option, or null if the option is
     *         not defined
     * @throws IllegalArgumentException if <code>required</code> is true
     *                                  and the option is not defined.
     */
    protected String getOption(Option option)
    {
        String s;

        s = getArgs().getOpt(option.name());
        if (s != null && (s.length() > 0 || !option.required()))
            return s;

        s = (String)getDomainContext().get(option.name());
        if (s != null && (s.length() > 0 || !option.required()))
            return s;

        if (option.required())
            throw new IllegalArgumentException(option.name()
                                               + " is a required argument");

        return option.defaultValue();
    }

    /**
     * Parses options for this cell.
     *
     * Option parsing is based on <code>Option</code> annotation of
     * fields. This fields must not be class private.
     *
     * Values are logger at the INFO level.
     */
    protected void parseOptions()
    {
        for (Field field : getClass().getDeclaredFields()) {
            Option option = field.getAnnotation(Option.class);
            try {
                if (option != null) {
                    field.setAccessible(true);

                    String s = getOption(option);
                    Object value;
                    if (s != null && s.length() > 0) {
                        try {
                            value = toType(s, field.getType());
                            field.set(this, value);
                        } catch (ClassCastException e) {
                            throw new IllegalArgumentException("Cannot convert '" + s + "' to " + field.getType(), e);
                        }
                    } else {
                        value = field.get(this);
                    }

                    if (option.log()) {
                        String description = option.description();
                        String unit = option.unit();
                        if (description.length() == 0)
                            description = option.name();
                        if (unit.length() > 0) {
                            info(description + " set to " + value + " " + unit);
                        } else {
                            info(description + " set to " + value);
                        }
                    }
                }
            } catch (SecurityException e) {
                throw new RuntimeException("Bug detected while processing option " + option.name(), e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Bug detected while processing option " + option.name(), e);
            }
        }
    }

    /**
     * Adds a receiver for dCache messages.
     *
     * The object is scanned for methods named
     * <code>messageArrived</code>, accepting exactly one
     * argument. This argument must be a derivative of
     * diskCacheV111.vehicles.Message.
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
        synchronized (_receivers) {
            Class c = o.getClass();
            for (Method m : c.getMethods()) {
                if (m.getName().equals("messageArrived")) {
                    Class[] parameters = m.getParameterTypes();
                    if (parameters.length == 1) {
                        Class parameter = parameters[0];
                        if (Message.class.isAssignableFrom(parameter)) {
                            Collection<Receiver> receivers =
                                _receivers.get(parameter);
                            if (receivers == null) {
                                receivers = new ArrayList<Receiver>();
                                _receivers.put(parameter, receivers);
                            }
                            receivers.add(new Receiver(o, m));
                        }
                    }
                }
            }
        }
    }

    public void messageArrived(CellMessage message)
    {
        Object o = message.getMessageObject();
        if (o instanceof Message) {
            synchronized (_receivers) {
                Collection<Receiver> receivers = _receivers.get(o.getClass());
                if (receivers != null) {
                    for (Receiver receiver : receivers) {
                        try {
                            receiver.deliver((Message)o);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException("Cannot process message due to access error", e);
                        } catch (InvocationTargetException e) {
                            error("Failed to process " + o.getClass()
                                  + ": " + e.getCause());
                            e.getCause().printStackTrace();
                        }
                    }
                }
            }
        }
    }
}

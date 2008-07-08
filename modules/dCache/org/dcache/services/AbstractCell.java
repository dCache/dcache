package org.dcache.services;

import org.apache.log4j.Logger;
import java.math.BigInteger;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import dmg.util.Args;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;

import org.dcache.util.PinboardAppender;
import org.dcache.util.CellMessageDispatcher;

/**
 * Abstract cell implementation providing features needed by many
 * dCache cells.
 *
 * <h2>Automatic dispatch of dCache messages to message handler</h2>
 *
 * <h2>Option parsing</h2>
 *
 * AbstractCell supports automatic option parsing based on annotations
 * of fields. A field is annotated with the Option annotation. The
 * annotation supports the following attributes:
 *
 * <dl>
 * <dt>name</dt>
 * <dd>The name of the option.</dd>
 *
 * <dt>description</dt>
 * <dd>A one line description of the option.</dd>
 *
 * <dt>defaultValue</dt>
 * <dd>The default value if the option is not specified,
 * specified as a string.</dd>
 *
 * <dt>unit</dt>
 * <dd>The unit of the value, if any, e.g. seconds.</dd>
 *
 * <dt>required</dt>
 * <dd>Whether this is a mandatory option. Defaults to false.</dd>
 *
 * <dt>log</dt>
 * <dd>Whether to log the value of the option during startup.
 * Defaults to true, but should be disabled for sensitive
 * information.</dd>
 * </dl>
 *
 * Options are automatically converted to the type of the field. In
 * case of non-POD fields, the class must have a one-argument
 * constructor taking a String. The File class is an example of such a
 * class.
 *
 * By defaults options are logged at the info level. The description
 * and unit should be formulated in such a way that the a message can
 * be formed as "<description> set to <value> <unit>".
 *
 * In case a required option is missing, an IllegalArgumentException
 * is thrown during option parsing.
 *
 * It is important that fields used for storing options do not have an
 * initializer. An initializer would overwrite the value retrieved
 * from the option.
 *
 * Example code:
 *
 * <code>
 *   @Option(
 *       name = "maxPinDuration",
 *       description = "Max. lifetime of a pin",
 *       defaultValue = "86400000", // one day
 *       unit = "ms"
 *   )
 *   protected long _maxPinDuration;
 *
 */
public class AbstractCell extends CellAdapter
{
    /**
     * Logger for the package of the instantiated class. Notice that
     * this is not a static field, as the logger instance to use
     * depends on the particular instance of this class.
     */
    protected Logger _logger;

    private final CellMessageDispatcher _messageDispatcher =
        new CellMessageDispatcher();

    public AbstractCell(String cellName, String args, boolean startNow)
    {
        this(cellName, new Args(args), startNow);
    }

    public AbstractCell(String cellName, Args args, boolean startNow)
    {
        super(cellName, args, startNow);

        _logger = Logger.getLogger(getClass());

        PinboardAppender.addCell(this);

        parseOptions();

        addMessageListener(this);
    }

    /**
     * Returns the friendly cell name used for logging. It defaults to
     * the cell name.
     */
    protected String getFriendlyName()
    {
        return getCellName();
    }

    public void debug(String str)
    {
        _logger.debug(str);
    }

    public void debug(Throwable t)
    {
        _logger.debug(t.getMessage());
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        for (String s : sw.toString().split("\n")) {
            _logger.debug(s);
        }
    }

    public void info(String str)
    {
        _logger.info(str);
    }

    public void warn(String str)
    {
        _logger.warn(str);
    }

    public void error(String str)
    {
        _logger.error(str);
    }

    public void error(Throwable t)
    {
        _logger.error(t.getMessage());
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        for (String s : sw.toString().split("\n")) {
            _logger.error(s);
        }
    }

    public void fatal(String str)
    {
        _logger.fatal(str);
    }

    public void fatal(Throwable t)
    {
        _logger.fatal(t.getMessage());
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        for (String s : sw.toString().split("\n")) {
            _logger.fatal(s);
        }
    }

    /** @deprecated */
    public void say(String s)
    {
        info(s);
    }

    /** @deprecated */
    public void esay(String s)
    {
        error(s);
    }

    /** @deprecated */
    public void esay(Throwable t)
    {
        error(t);
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
        for (Class c = getClass(); c != null; c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
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
    }

    /**
     * Writes information about all options (Option annotated fields)
     * to a writer.
     */
    protected void writeOptions(PrintWriter out)
    {
        for (Class c = getClass(); c != null; c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                Option option = field.getAnnotation(Option.class);
                try {
                    if (option != null) {
                        if (option.log()) {
                            field.setAccessible(true);
                            Object value = field.get(this);
                            String description = option.description();
                            String unit = option.unit();
                            if (description.length() == 0)
                                description = option.name();
                            out.println(description + " is " + value + " " + unit);
                        }
                    }
                } catch (SecurityException e) {
                    throw new RuntimeException("Bug detected while processing option " + option.name(), e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Bug detected while processing option " + option.name(), e);
                }
            }
        }
    }

    /**
     * Adds a listener for dCache messages.
     *
     * @see CellMessageDispatcher.addMessageListener
     */
    public void addMessageListener(Object o)
    {
        _messageDispatcher.addMessageListener(o);
    }

    /**
     * Removes a listener previously added with addMessageListener.
     */
    public void removeMessageListener(Object o)
    {
        _messageDispatcher.removeMessageListener(o);
    }

    /**
     * Delivers messages to registered message listeners.
     */
    public void messageArrived(CellMessage envelope)
    {
        super.messageArrived(envelope);

        try {
            _messageDispatcher.messageArrived(envelope);
        } catch (InvocationTargetException e) {
            error("Failed to process " + envelope
                  + ": " + e.getCause());
        }
    }
}

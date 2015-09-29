package org.dcache.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigInteger;

public class OptionParser
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OptionParser.class);
    private final Args args;

    public OptionParser(Args args)
    {
        this.args = args;
    }

    /**
     * Injects the options of {@code args} and default values into {@code object} using the
     * Option annotation of fields of {@code <T>}.
     *
     * @param args Arguments providing option values.
     * @param object Target object to inject options into.
     * @param <T> Type of the target object.
     * @return Returns {@code object}.
     */
    public static <T> T inject(Args args, T object)
    {
        return new OptionParser(args).inject(object);
    }

    /**
     * Injects the options default values for options into {@code object} using the
     * Option annotation of fields of {@code <T>}.
     *
     * @param object Target object to inject options into.
     * @param <T> Type of the target object.
     * @return Returns {@code object}.
     */
    public static <T> T injectDefaults(T object)
    {
        return inject(new Args(new String[0]), object);
    }

    /**
     * Convert an instance to a specific type (kind of intelligent
     * casting).  Note: you can set primitive types as input
     * <i>type</i> but the return type will be the corresponding
     * wrapper type (e.g. Integer.TYPE will result in Integer.class)
     * with the difference that instead of a result 'null' a numeric 0
     * (or boolean false) will be returned because primitive types
     * can't be null.
     * <p>
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
     * <li>java.lang.Class
     * </ul>
     *
     * @param object Instance to convert.
     * @param type   Destination type (e.g. Boolean.class).
     * @return Converted instance/datatype/collection or null if
     * input object is null.
     * @throws ClassCastException if <i>object</i> can't be converted to
     *                            <i>type</i>.
     * @author MartinHilpert at SUN's Java Forum
     */
    @SuppressWarnings("unchecked")
    public static <T> T toType(final Object object, final Class<T> type)
    {
        if (object == null) {
            // initialize primitive types:
            if (type == Boolean.TYPE) {
                return ((Class<T>) Boolean.class).cast(false);
            } else if (type == Byte.TYPE) {
                return ((Class<T>) Byte.class).cast(0);
            } else if (type == Character.TYPE) {
                return ((Class<T>) Character.class).cast(0);
            } else if (type == Double.TYPE) {
                return ((Class<T>) Double.class).cast(0.0);
            } else if (type == Float.TYPE) {
                return ((Class<T>) Float.class).cast(0.0);
            } else if (type == Integer.TYPE) {
                return ((Class<T>) Integer.class).cast(0);
            } else if (type == Long.TYPE) {
                return ((Class<T>) Long.class).cast(0);
            } else if (type == Short.TYPE) {
                return ((Class<T>) Short.class).cast(0);
            }
            return null;
        } else {
            final String so = object.toString();

            // custom type conversions:
            if (type == BigInteger.class) {
                return type.cast(new BigInteger(so));
            } else if (type == Boolean.class || type == Boolean.TYPE) {
                Boolean r;
                if ("1".equals(so) || "true".equalsIgnoreCase(so) || "yes".equalsIgnoreCase(
                        so) || "on".equalsIgnoreCase(so) || "enabled".equalsIgnoreCase(so)) {
                    r = Boolean.TRUE;
                } else if ("0".equals(object) || "false".equalsIgnoreCase(so) || "no".equalsIgnoreCase(
                        so) || "off".equalsIgnoreCase(so) || "disabled".equalsIgnoreCase(so)) {
                    r = Boolean.FALSE;
                } else {
                    r = Boolean.valueOf(so);
                }

                if (type == Boolean.TYPE) {
                    return ((Class<T>) Boolean.class).cast(r); //avoid ClassCastException through autoboxing
                } else {
                    return type.cast(r);
                }
            } else if (type == Byte.class || type == Byte.TYPE) {
                Byte i = Byte.valueOf(so);
                if (type == Byte.TYPE) {
                    return ((Class<T>) Byte.class).cast(i); //avoid ClassCastException through autoboxing
                } else {
                    return type.cast(i);
                }
            } else if (type == Character.class || type == Character.TYPE) {
                Character i = so.charAt(0);
                if (type == Character.TYPE) {
                    return ((Class<T>) Character.class).cast(i); //avoid ClassCastException through autoboxing
                } else {
                    return type.cast(i);
                }
            } else if (type == Double.class || type == Double.TYPE) {
                Double i = Double.valueOf(so);
                if (type == Double.TYPE) {
                    return ((Class<T>) Double.class).cast(i); //avoid ClassCastException through autoboxing
                } else {
                    return type.cast(i);
                }
            } else if (type == Float.class || type == Float.TYPE) {
                Float i = Float.valueOf(so);
                if (type == Float.TYPE) {
                    return ((Class<T>) Float.class).cast(i); //avoid ClassCastException through autoboxing
                } else {
                    return type.cast(i);
                }
            } else if (type == Integer.class || type == Integer.TYPE) {
                Integer i = Integer.valueOf(so);
                if (type == Integer.TYPE) {
                    return ((Class<T>) Integer.class).cast(i); //avoid ClassCastException through autoboxing
                } else {
                    return type.cast(i);
                }
            } else if (type == Long.class || type == Long.TYPE) {
                Long i = Long.valueOf(so);
                if (type == Long.TYPE) {
                    return ((Class<T>) Long.class).cast(i); //avoid ClassCastException through autoboxing
                } else {
                    return type.cast(i);
                }
            } else if (type == Short.class || type == Short.TYPE) {
                Short i = Short.valueOf(so);
                if (type == Short.TYPE) {
                    return ((Class<T>) Short.class).cast(i); //avoid ClassCastException through autoboxing
                } else {
                    return type.cast(i);
                }
            } else if (Enum.class.isAssignableFrom(type)) {
                return (T) Enum.valueOf((Class<Enum>) type, so);
            } else if (Class.class.isAssignableFrom(type)) {
                try {
                    return type.cast(Class.forName(so));
                } catch (ClassNotFoundException e) {
                    return type.cast(object);
                }
            } else {
                try {
                    Method valueOf = type.getMethod("valueOf", object.getClass());
                    if (Modifier.isStatic(valueOf.getModifiers()) && type.isAssignableFrom(valueOf.getReturnType())) {
                        return type.cast(valueOf.invoke(null, object));
                    }
                } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ignored) {
                }
                try {
                    return type.getConstructor(String.class).newInstance(object);
                } catch (NoSuchMethodException | SecurityException | IllegalAccessException | InstantiationException | InvocationTargetException ignored) {
                }
                return type.cast(object);
            }
        }
    }

    /**
     * Returns the value of an option.
     *
     * @param option the option
     * @return the value of the option, or null if the option is
     * not defined
     * @throws IllegalArgumentException if <code>required</code> is true
     *                                  and the option is not defined.
     */
    protected String getOption(Option option)
    {
        String s;

        s = args.getOpt(option.name());
        if (s != null && (s.length() > 0 || !option.required())) {
            return s;
        }

        if (option.required()) {
            throw new IllegalArgumentException(option.name()
                                               + " is a required argument");
        }

        return option.defaultValue();
    }

    /**
     * Injects options into an object.
     * <p>
     * Option parsing is based on <code>Option</code> annotation of
     * fields. These fields must not be class private.
     * <p>
     * Values are logger at the INFO level.
     */
    public <T> T inject(T obj)
    {
        for (Class<?> c = obj.getClass(); c != null; c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                Option option = field.getAnnotation(Option.class);
                try {
                    if (option != null) {
                        field.setAccessible(true);

                        String s = getOption(option);
                        Object value;
//                        this filters empty strings with the result that they
//                        become null
                        if (s != null && s.length() > 0) {
                            try {
                                value = toType(s, field.getType());
                                field.set(obj, value);
                            } catch (ClassCastException e) {
                                throw new IllegalArgumentException("Cannot convert '" + s + "' to " + field.getType(),
                                                                   e);
                            }
                        } else {
                            value = field.get(obj);
                        }

                        if (option.log()) {
                            String description = option.description();
                            String unit = option.unit();
                            if (description.length() == 0) {
                                description = option.name();
                            }
                            if (unit.length() > 0) {
                                LOGGER.info("{} set to {} {}", description, value, unit);
                            } else {
                                LOGGER.info("{} set to {}", description, value);
                            }
                        }
                    }
                } catch (SecurityException | IllegalAccessException e) {
                    throw new RuntimeException("Bug detected while processing option " + option.name(), e);
                }
            }
        }
        return obj;
    }
}
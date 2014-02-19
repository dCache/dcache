package org.dcache.util.cli;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import dmg.util.CommandException;
import dmg.util.CommandSyntaxException;
import dmg.util.CommandThrowableException;
import dmg.util.command.AnnotatedCommandHelpPrinter;
import dmg.util.command.AnsiHelpPrinter;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.CommandLine;
import dmg.util.command.HelpFormat;
import dmg.util.command.Option;
import dmg.util.command.PlainHelpPrinter;

import org.dcache.util.Args;

import static com.google.common.collect.Iterables.*;
import static java.util.Arrays.asList;

/**
 * CommandExecutor for commands implemented as non-static inner
 * classes implementing Callable and annotated with @Command.
 */
public class AnnotatedCommandExecutor implements CommandExecutor
{
    private static final Function<Handler, Integer> GET_MAX_ARGS =
            new Function<Handler,Integer>() {
                @Override
                public Integer apply(Handler handler)
                {
                    return handler.getMaxArguments();
                }
            };

    private static final ImmutableMap<HelpFormat, AnnotatedCommandHelpPrinter> HELP_PRINTERS =
            ImmutableMap.<HelpFormat, AnnotatedCommandHelpPrinter>builder()
                    .put(HelpFormat.PLAIN, new PlainHelpPrinter())
                    .put(HelpFormat.ANSI, new AnsiHelpPrinter())
                    .build();

    private static final Joiner AS_COMMA_LIST = Joiner.on(", ");

    private final Object _parent;
    private final Command _command;
    private final Constructor<? extends Callable<? extends Serializable>> _constructor;
    private final List<Handler> _handlers;

    public AnnotatedCommandExecutor(Object parent, Command command,
                                    Constructor<? extends Callable<? extends Serializable>> constructor)
    {
        _parent = parent;
        _command = command;
        _constructor = constructor;
        _handlers = createFieldHandlers(command, _constructor.getDeclaringClass());
    }

    @Override
    public boolean hasACLs()
    {
        return _command.acl().length > 0;
    }

    @Override
    public String[] getACLs()
    {
        return _command.acl();
    }

    private AnnotatedCommandHelpPrinter getHelpPrinter(HelpFormat format)
    {
        AnnotatedCommandHelpPrinter printer = HELP_PRINTERS.get(format);
        if (printer == null) {
            printer = HELP_PRINTERS.get(HelpFormat.PLAIN);
        }
        return printer;
    }

    @Override
    public String getHelpHint(HelpFormat format)
    {
        return getHelpPrinter(format).getHelpHint(_command, _constructor.getDeclaringClass());
    }

    @Override
    public String getFullHelp(HelpFormat format)
    {
        return getHelpPrinter(format).getHelp(createInstance());
    }

    @Override
    public Serializable execute(Args arguments) throws CommandException
    {
        Callable<? extends Serializable> command = createInstance();
        try {
            for (Handler handler: _handlers) {
                handler.apply(command, arguments);
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException("This is a bug. Please notify " +
                    "support@dcache.org", e);
        } catch (IllegalArgumentException e) {
            throw new CommandSyntaxException(e.getMessage());
        }

        try {
            return command.call();
        } catch (CommandException e) {
            throw e;
        } catch (RuntimeException e) {
            try {
                /* We treat uncaught RuntimeExceptions other than
                 * those declared to be thrown by the method as
                 * bugs and propagate them.
                 */
                boolean declared = false;
                Method method = command.getClass().getMethod("call");
                for (Class<?> clazz: method.getExceptionTypes()) {
                    if (clazz.isAssignableFrom(e.getClass())) {
                        declared = true;
                    }
                }

                if (!declared) {
                    throw e;
                }
                throw new CommandThrowableException(
                        e.toString() + " from " + _command.name(), e);
            } catch (NoSuchMethodException nsme) {
                throw new RuntimeException(
                        "This is a bug. Please notify support@dcache.org", nsme);
            }
        } catch (Exception e) {
            throw new CommandThrowableException(
                    e.toString() + " from " + _command.name(), e);
        }
    }

    private Callable<? extends Serializable> createInstance()
    {
        try {
            return _constructor.newInstance(_parent);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(
                    "This is a bug. Please notify support@dcache.org", e);
        }
    }

    private static Handler createFieldHandler(Field field, Option option)
    {
        Class<?> type = field.getType();
        if (type.isArray()) {
            Function<String,Object> typeConverter =
                    createTypeConverter(type.getComponentType());
            if (option.values().length > 0) {
                typeConverter = new MultipleChoiceValidator(
                        typeConverter, asList(option.values()));
            }
            if (option.separator().isEmpty()) {
                return new MultiValuedOptionHandler(field, typeConverter, option);
            } else {
                return new SplittingOptionHandler(field, typeConverter, option);
            }
        } else {
            Function<String,Object> typeConverter = createTypeConverter(type);
            if (option.values().length > 0) {
                typeConverter = new MultipleChoiceValidator(
                        typeConverter, asList(option.values()));
            }
            return new OptionHandler(field, typeConverter, option);
        }
    }

    private static Handler createFieldHandler(Field field, Argument argument)
    {
        Class<?> type = field.getType();
        if (type.isArray()) {
            Function<String,Object> typeConverter =
                    createTypeConverter(type.getComponentType());
            return new MultiValuedArgumentHandler(field, typeConverter, argument);
        } else {
            Function<String,Object> typeConverter = createTypeConverter(type);
            return new ArgumentHandler(field, typeConverter, argument);
        }
    }

    private static Handler createFieldHandler(Command command, Field field, CommandLine commandLine)
    {
        Class<?> type = field.getType();
        if (type.isAssignableFrom(Args.class)) {
            return new ArgsHandler(field);
        } else if (type.isAssignableFrom(String.class)) {
            return new CommandLineHandler(command, field);
        } else {
            throw new IllegalArgumentException("CommandLine annotation is only applicable to Args and String fields");
        }
    }

    private static List<Handler> createFieldHandlers(Command command, Class<? extends Callable<?>> clazz)
    {
        Set<String> names = new HashSet<>();

        List<Handler> handlers = Lists.newArrayList();
        for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
            for (Field field: c.getDeclaredFields()) {
                Option option = field.getAnnotation(Option.class);
                if (option != null) {
                    handlers.add(createFieldHandler(field, option));
                    names.add(option.name());
                }

                Argument argument = field.getAnnotation(Argument.class);
                if (argument != null) {
                    handlers.add(createFieldHandler(field, argument));
                }

                CommandLine commandLine = field.getAnnotation(CommandLine.class);
                if (commandLine != null) {
                    handlers.add(createFieldHandler(command, field, commandLine));
                }
            }
        }

        int maxArgs = handlers.isEmpty() ? 0 :
                Ordering.natural().max(transform(handlers, GET_MAX_ARGS));
        if (maxArgs < Integer.MAX_VALUE) {
            handlers.add(new MaxArgumentsHandler(maxArgs));
        }

        handlers.add(new CheckOptionsKnownHandler(names));

        return handlers;
    }

    private static Function<String,Object> createTypeConverter(Class<?> type)
    {
        if (Boolean.class.equals(type) || Boolean.TYPE.equals(type)) {
            return new BooleanTypeConverter();
        } else if (Byte.class.equals(type) || Byte.TYPE.equals(type)) {
            return new ByteTypeConverter();
        } else if (Character.class.equals(type) || Character.TYPE.equals(type)) {
            return new CharacterTypeConverter();
        } else if (Double.class.equals(type) || Double.TYPE.equals(type)) {
            return new DoubleTypeConverter();
        } else if (Float.class.equals(type) || Float.TYPE.equals(type)) {
            return new FloatTypeConverter();
        } else if (Integer.class.equals(type) || Integer.TYPE.equals(type)) {
            return new IntegerTypeConverter();
        } else if (Long.class.equals(type) || Long.TYPE.equals(type)) {
            return new LongTypeConverter();
        } else if (Short.class.equals(type) || Short.TYPE.equals(type)) {
            return new ShortTypeConverter();
        } else if (String.class.equals(type)) {
            return new StringTypeConverter();
        } else if (type.isEnum()) {
            return new EnumTypeConverter(type.asSubclass(Enum.class));
        } else if (!type.isInterface() && !type.isAnnotation() && !type.isPrimitive()) {
            try {
                Method method = type.getMethod("valueOf", String.class);
                if (Modifier.isStatic(method.getModifiers())) {
                    return new ValueOfTypeConverter(method);
                }
            } catch (NoSuchMethodException ignored) {
            }
            try {
                return new StringConstructorTypeConverter(
                        type.getConstructor(String.class));
            } catch (NoSuchMethodException ignored) {
            }
        }
        throw new RuntimeException("This is a bug. Please notify " +
                "support@dcache.org. Cannot convert to type " + type);
    }

    /**
     * Implementations of this interface can process arguments and
     * options represented as an Args object. A typical implementation
     * will apply an argument or option to a command object's field
     * using reflection.
     */
    private interface Handler
    {
        /**
         * Applies processing to the args parameter.
         *
         * May modify the fields of the command object.
         *
         * @param object A command object
         * @param args Args object to which to apply processing
         * @throws IllegalAccessException if reflection on a field
         * of object fails due to lack of access
         * @throws IllegalArgumentException If any of the options or
         * arguments in args violates the constraints of the handler-
         */
        void apply(Object object, Args args)
                throws IllegalAccessException;

        /**
         * Maximum number of arguments consumed by this handler.
         *
         * @return maximum number of arguments or Integer.MAX_VALUE
         * if there is no upper bound.
         */
        int getMaxArguments();
    }

    /**
     * Rejects arguments lists longer than a specified maximum.
     */
    private static class MaxArgumentsHandler implements Handler
    {
        private final int _max;

        public MaxArgumentsHandler(int max)
        {
            _max = max;
        }

        @Override
        public int getMaxArguments()
        {
            return 0;
        }

        @Override
        public void apply(Object object, Args args)
                throws IllegalAccessException
        {
            if (args.argc() > _max) {
                throw new IllegalArgumentException("Too many arguments: " +
                        Joiner.on(" ").join(args.getArguments()
                                .subList(_max, args.argc())));
            }
        }
    }

    /**
     * Abstract base class for handlers that apply a value to a field
     * of the command object.
     */
    private static abstract class FieldHandler implements Handler
    {
        protected final Field _field;

        public FieldHandler(Field field)
        {
            _field = field;
            _field.setAccessible(true);
        }

        protected abstract Object getValue(Args args);

        @Override
        public void apply(Object object, Args args)
                throws IllegalAccessException
        {
            Object value = getValue(args);
            if (value != null) {
                _field.set(object, value);
            }
        }
    }

    /**
     * Maps single value arguments to a field annotated as an @Argument.
     */
    private static class ArgumentHandler extends FieldHandler
    {
        private final Function<String,Object> _typeConverter;
        private final Argument _argument;

        public ArgumentHandler(Field field, Function<String,Object> typeConverter, Argument argument)
        {
            super(field);

            _typeConverter = typeConverter;
            _argument = argument;
        }

        @Override
        public int getMaxArguments()
        {
            return (_argument.index() >= 0) ? _argument.index() + 1 : -_argument.index();
        }

        @Override
        protected Object getValue(Args args)
        {
            int index = _argument.index();
            if (index < 0) {
                index += args.argc();
            }

            if (0 <= index && index < args.argc()) {
                return _typeConverter.apply(args.argv(index));
            } else if (_argument.required()) {
                throw new IllegalArgumentException("Argument " + (index + 1) + " is required");
            }
            return null;
        }
    }

    /**
     * Maps arguments to an array field annotated as an @Argument.
     */
    private static class MultiValuedArgumentHandler extends FieldHandler
    {
        private final Function<String,Object> _typeConverter;
        private final Argument _argument;

        public MultiValuedArgumentHandler(Field field,
                                          Function<String, Object> typeConverter,
                                          Argument argument)
        {
            super(field);

            if (argument.index() < 0) {
                throw new IllegalArgumentException("Negative index is not allowed for multi valued arguments");
            }

            _typeConverter = typeConverter;
            _argument = argument;
        }

        @Override
        public int getMaxArguments()
        {
            return Integer.MAX_VALUE;
        }

        @Override
        protected Object getValue(Args args)
        {
            int index = _argument.index();

            if (index < args.argc()) {
                Class<?> type = _field.getType().getComponentType();
                Object values = Array.newInstance(type, args.argc() - index);
                for (int i = index; i < args.argc(); i++) {
                    Object value = _typeConverter.apply(args.argv(i));
                    Array.set(values, i - index, value);
                }
                return values;
            } else if (_argument.required()) {
                throw new IllegalArgumentException("Argument " + (index + 1) + " is required");
            }
            return null;
        }
    }

    /**
     * Maps a single value option to a field annotated as an @Option.
     */
    private static class OptionHandler extends FieldHandler
    {
        private final Function<String,Object> _typeConverter;
        private final Option _option;

        public OptionHandler(Field field, Function<String,Object> typeConverter,
                             Option option)
        {
            super(field);
            _typeConverter = typeConverter;
            _option = option;
        }

        @Override
        public int getMaxArguments()
        {
            return 0;
        }

        @Override
        protected Object getValue(Args args)
        {
            String value = args.getOption(_option.name());
            if (value != null) {
                return _typeConverter.apply(value);
            } else if (_option.required()) {
                throw new IllegalArgumentException("Option " + _option.name() + " is required");
            }
            return null;
        }
    }

    /**
     * Maps an option list to an array field annotated as an @Option.
     */
    private static class MultiValuedOptionHandler extends FieldHandler
    {
        private final Function<String,Object> _typeConverter;
        private final Option _option;

        public MultiValuedOptionHandler(Field field, Function<String,Object> typeConverter,
                                        Option option)
        {
            super(field);
            _typeConverter = typeConverter;
            _option = option;
        }

        @Override
        public int getMaxArguments()
        {
            return 0;
        }

        @Override @SuppressWarnings("unchecked")
        protected Object getValue(Args args)
        {
            ImmutableList<String> values = args.getOptions(_option.name());
            if (!values.isEmpty()) {
                return toArray(transform(values, _typeConverter),
                        (Class) _field.getType().getComponentType());
            } else if (_option.required()) {
                throw new IllegalArgumentException("Option " + _option.name() + " is required");
            }
            return null;
        }
    }

    /**
     * Maps an option list to an array field annotated as an @Option.
     * Option values are split into elements according to a separator
     * string.
     */
    private static class SplittingOptionHandler extends FieldHandler
    {
        private final Function<String,Object> _typeConverter;
        private final Option _option;
        private final Splitter _splitter;

        public SplittingOptionHandler(Field field,
                                      Function<String,Object> typeConverter,
                                      Option option)
        {
            super(field);
            _typeConverter = typeConverter;
            _option = option;
            _splitter = Splitter.on(option.separator());
        }

        @Override
        public int getMaxArguments()
        {
            return 0;
        }

        @Override @SuppressWarnings("unchecked")
        protected Object getValue(Args args)
        {
            ImmutableList<String> values = args.getOptions(_option.name());
            if (!values.isEmpty()) {
                List<String> fragments = Lists.newArrayList();
                for (String value: values) {
                    if (!value.isEmpty()) {
                        addAll(fragments, _splitter.split(value));
                    }
                }
                return toArray(transform(fragments, _typeConverter),
                        (Class) _field.getType().getComponentType());
            } else if (_option.required()) {
                throw new IllegalArgumentException("Option " + _option.name() + " is required");
            }
            return null;
        }
    }

    /**
     * Assigns the entire Args object to a field.
     */
    private static class ArgsHandler extends FieldHandler
    {
        private ArgsHandler(Field field)
        {
            super(field);
        }

        @Override
        protected Object getValue(Args args)
        {
            return args;
        }

        @Override
        public int getMaxArguments()
        {
            return 0;
        }
    }

    /**
     * Assigns the command line to a field.
     */
    private static class CommandLineHandler extends FieldHandler
    {
        private final String command;

        private CommandLineHandler(Command command, Field field)
        {
            super(field);
            this.command = command.name();
        }

        @Override
        protected Object getValue(Args args)
        {
            return (args.optc() == 0 && args.argc() == 0) ? command : (command + " " + args);
        }

        @Override
        public int getMaxArguments()
        {
            return 0;
        }
    }

    /**
     * Prevents running a command if user supplied any options that are unknown.
     */
    private static class CheckOptionsKnownHandler implements Handler
    {
        private final Set<String> _known;

        private CheckOptionsKnownHandler(Set<String> names)
        {
            _known = names;
        }

        @Override
        public void apply(Object object, Args args) throws IllegalAccessException
        {
            Set<String> supplied = args.options().keySet();
            Set<String> unknown = Sets.difference(supplied, _known);
            if (!unknown.isEmpty()) {
                throw new IllegalArgumentException("Unknown option" +
                        (unknown.size() > 1 ? "s" : "") + ": " +
                        AS_COMMA_LIST.join(unknown));
            }
        }

        @Override
        public int getMaxArguments()
        {
            return Integer.MAX_VALUE;
        }
    }

    /**
     * A wrapper for other functions, which restricts the domain of the function
     * to a finite set of prespecified values.
     */
    private static class MultipleChoiceValidator implements Function<String,Object>
    {
        private final Function<String,Object> _inner;
        private final List<String> _values;

        public MultipleChoiceValidator(Function<String,Object> inner, List<String> values)
        {
            _inner = inner;
            _values = values;
        }

        @Override
        public Object apply(String value)
        {
            if (!_values.contains(value)) {
                throw new IllegalArgumentException("Invalid value: " + value);
            }
            return _inner.apply(value);
        }
    }

    /**
     * A function from String to Boolean.
     */
    private static class BooleanTypeConverter implements Function<String,Object>
    {
        @Override
        public Boolean apply(String value)
        {
            if ("true".equalsIgnoreCase(value) || value.isEmpty()) {
                return Boolean.TRUE;
            } else if ("false".equalsIgnoreCase(value)) {
                return Boolean.FALSE;
            } else {
                throw new IllegalArgumentException("Invalid value for boolean: " + value);
            }
        }
    }

    /**
     * A function from String to Byte.
     */
    private static class ByteTypeConverter implements Function<String,Object>
    {
        @Override
        public Byte apply(String value)
        {
            return Byte.valueOf(value);
        }
    }

    /**
     * A function from String to Character.
     */
    private static class CharacterTypeConverter implements Function<String,Object>
    {
        @Override
        public Character apply(String value)
        {
            return value.charAt(0);
        }
    }

    /**
     * A function from String to Double.
     */
    private static class DoubleTypeConverter implements Function<String,Object>
    {
        @Override
        public Double apply(String value)
        {
            return Double.valueOf(value);
        }
    }

    /**
     * A function from String to Float.
     */
    private static class FloatTypeConverter implements Function<String,Object>
    {
        @Override
        public Float apply(String value)
        {
            return Float.valueOf(value);
        }
    }

    /**
     * A function from String to Integer.
     */
    private static class IntegerTypeConverter implements Function<String,Object>
    {
        @Override
        public Integer apply(String value)
        {
            return Integer.valueOf(value);
        }
    }

    /**
     * A function from String to Long.
     */
    private static class LongTypeConverter implements Function<String,Object>
    {
        @Override
        public Long apply(String value)
        {
            return Long.valueOf(value);
        }
    }

    /**
     * A function from String to Short.
     */
    private static class ShortTypeConverter implements Function<String,Object>
    {
        @Override
        public Short apply(String value)
        {
            return Short.valueOf(value);
        }
    }

    /**
     * Identity function from String to String.
     */
    private static class StringTypeConverter implements Function<String,Object>
    {
        @Override
        public String apply(String value)
        {
            return value;
        }
    }

    /**
     * A function from String to a class with a String constructor.
     */
    private static class StringConstructorTypeConverter implements Function<String,Object>
    {
        private Constructor<?> _constructor;

        public StringConstructorTypeConverter(Constructor<?> constructor)
        {
            _constructor = constructor;
        }

        @Override
        public Object apply(String value)
        {
            try {
                return _constructor.newInstance(value);
            } catch (InvocationTargetException e) {
                Throwable t = e.getTargetException();
                Throwables.propagateIfPossible(t);
                throw new IllegalArgumentException(t.getMessage(), t);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("This is a bug. Please notify support@dcache.org", e);
            }
        }
    }

    /**
     * A function from String to a class with a static valueOf factory method.
     */
    private static class ValueOfTypeConverter implements Function<String,Object>
    {
        private Method _method;

        public ValueOfTypeConverter(Method method)
        {
            _method = method;
        }

        @Override
        public Object apply(String value)
        {
            try {
                return _method.invoke(null, value);
            } catch (InvocationTargetException e) {
                Throwable t = e.getTargetException();
                Throwables.propagateIfPossible(t);
                throw new IllegalArgumentException(t.getMessage(), t);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("This is a bug. Please notify support@dcache.org", e);
            }
        }
    }

    /**
     * A function from String to an enum class.
     */
    private static class EnumTypeConverter implements Function<String,Object>
    {
        private Class<? extends Enum> _type;

        public EnumTypeConverter(Class<? extends Enum> type)
        {
            _type = type;
        }

        @Override
        public Object apply(String value)
        {
            return Enum.valueOf(_type, value.toUpperCase());
        }
    }
}

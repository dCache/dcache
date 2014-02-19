package org.dcache.util.cli;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import dmg.util.command.Command;

import static java.util.Arrays.asList;

/**
 * CommandScanner for commands implemented as non-static inner
 * classes implementing Callable and annotated with @Command.
 */
public class AnnotatedCommandScanner implements CommandScanner
{
    private final static TypeToken<Callable<? extends Serializable>> EXPECTED_TYPE =
            new TypeToken<Callable<? extends Serializable>>() {};

    /**
     * Verifies that clazz implements Callable<? extends Serializable> and casts it
     * to that type.
     *
     * @param clazz The clazz of the command object
     * @return clazz cast to Callable<? extends Serializable>
     */
    @SuppressWarnings("unchecked")
    private Class<? extends Callable<? extends Serializable>> cast(Class<?> clazz)
    {
        if (EXPECTED_TYPE.isAssignableFrom(clazz)) {
            return (Class<? extends Callable<? extends Serializable>>) clazz.asSubclass(Callable.class);
        }
        throw new RuntimeException("This is a bug. Please notify support@dcache.org (" + clazz +
                                       " does not implement Callable<? extends Serializable>).");
    }

    @Override
    public Map<List<String>, ? extends CommandExecutor> scan(Object obj)
    {
        Map<List<String>, AnnotatedCommandExecutor> commands = Maps.newHashMap();

        Class<?> containerClass = obj.getClass();
        while (containerClass != null) {
            Class<?>[] classes = containerClass.getDeclaredClasses();
            for (Class<?> commandClass : classes) {
                Command command = commandClass.getAnnotation(Command.class);
                if (command != null && !commandClass.isInterface() &&
                    Callable.class.isAssignableFrom(commandClass)) {
                    try {
                        Constructor<? extends Callable<? extends Serializable>> constructor =
                            cast(commandClass).getDeclaredConstructor(commandClass.getDeclaringClass());
                        constructor.setAccessible(true);
                        commands.put(asList(command.name().split(" ")),
                                new AnnotatedCommandExecutor(obj, command, constructor));
                    } catch (NoSuchMethodException e) {
                        throw new RuntimeException("This is a bug. Please notify support@dcache.org.", e);
                    }
                }
            }
            containerClass = containerClass.getSuperclass();
        }

        return commands;
    }
}

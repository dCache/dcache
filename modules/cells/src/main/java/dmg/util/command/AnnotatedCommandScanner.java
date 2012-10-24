package dmg.util.command;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static java.util.Arrays.asList;

/**
 * CommandScanner for commands implemented as non-static inner
 * classes implementing Callable and annotated with @Command.
 */
public class AnnotatedCommandScanner implements CommandScanner
{
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
        for (Type type: clazz.getGenericInterfaces()) {
            if (type instanceof ParameterizedType) {
                ParameterizedType parameterizedType = (ParameterizedType) type;
                if (parameterizedType.getRawType().equals(Callable.class)) {
                    Type argument = parameterizedType.getActualTypeArguments()[0];
                    if (argument instanceof Class && Serializable.class.isAssignableFrom((Class<?>) argument)) {
                        return (Class<? extends Callable<? extends Serializable>>) clazz.asSubclass(Callable.class);
                    }
                }
            }
        }
        throw new RuntimeException("This is a bug. Please notify support@dcache.org (" + clazz +
                                       " does not implement Callable<? extends Serializable>).");
    }

    @Override
    public Map<List<String>, CommandExecutor> scan(Object obj)
    {
        Map<List<String>, CommandExecutor> commands = Maps.newHashMap();

        Class<?>[] classes = obj.getClass().getDeclaredClasses();
        for (Class<?> clazz: classes) {
            Command command = clazz.getAnnotation(Command.class);
            if (command != null && !clazz.isInterface() &&
                Callable.class.isAssignableFrom(clazz)) {
                try {
                    Constructor<? extends Callable<? extends Serializable>> constructor =
                        cast(clazz).getDeclaredConstructor(obj.getClass());
                    constructor.setAccessible(true);
                    commands.put(asList(command.name().split(" ")),
                            new AnnotatedCommandExecutor(obj, command, constructor));
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException("This is a bug. Please notify support@dcache.org.", e);
                }
            }
        }

        return commands;
    }
}

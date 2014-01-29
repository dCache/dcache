package dmg.cells.services.login;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import dmg.cells.nucleus.Cell;
import dmg.util.StreamEngine;

import org.dcache.util.Args;

import static com.google.common.base.Preconditions.checkArgument;

public class LegacyLoginCellProvider implements LoginCellProvider
{
    private static final Class<?>[] LOGIN_CON_SIGNATURE =
            { String.class, StreamEngine.class };
    private static final Class<?>[] LOGIN_CON_WITH_ARGS_SIGNATURE =
            { String.class, StreamEngine.class, Args.class };

    @Override
    public int getPriority(String name)
    {
        try {
            Class<?> clazz = Class.forName(name);
            if (!Cell.class.isAssignableFrom(clazz)) {
                return Integer.MIN_VALUE;
            }
            Class<? extends Cell> loginClass = clazz.asSubclass(Cell.class);
            Constructor<? extends Cell> constructor;
            try {
                constructor = loginClass.getConstructor(LOGIN_CON_WITH_ARGS_SIGNATURE);
            } catch (NoSuchMethodException e) {
                constructor = loginClass.getConstructor(LOGIN_CON_SIGNATURE);
            }
            checkConstructor(constructor);
        } catch (IllegalArgumentException | ClassNotFoundException | NoSuchMethodException e) {
            return Integer.MIN_VALUE;
        }
        return 0;
    }

    @Override
    public LoginCellFactory createFactory(String name, Args args, String parentCellName)
    {
        try {
            Class<? extends Cell> loginClass = Class.forName(name).asSubclass(Cell.class);
            try {
                Constructor<? extends Cell> constructor = loginClass.getConstructor(LOGIN_CON_WITH_ARGS_SIGNATURE);
                checkConstructor(constructor);
                return new LegacyWithArgsLoginCellFactory(constructor, args, parentCellName);
            } catch (NoSuchMethodException e) {
                Constructor<? extends Cell> constructor = loginClass.getConstructor(LOGIN_CON_SIGNATURE);
                checkConstructor(constructor);
                return new LegacyLoginCellFactory(constructor, parentCellName);
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("No such login cell: " + args.argv(0));
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Login cell lacks appropriate constructor: " + args.argv(0));
        }
    }

    private void checkConstructor(Constructor<? extends Cell> constructor)
    {
        checkArgument(Modifier.isPublic(constructor.getDeclaringClass().getModifiers()), "Login cell is not public");
        checkArgument(Modifier.isPublic(constructor.getModifiers()), "Login cell constructor is not public");
        checkArgument(!Modifier.isAbstract(constructor.getModifiers()), "Login cell is abstract");
    }
}

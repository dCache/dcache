package dmg.cells.services.login;

import static com.google.common.base.Preconditions.checkArgument;

import dmg.cells.nucleus.Cell;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellEndpoint;
import dmg.util.StreamEngine;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import org.dcache.util.Args;

public class LegacyLoginCellProvider implements LoginCellProvider {

    private static final Class<?>[] LOGIN_CON_SIGNATURE =
          {String.class, StreamEngine.class};
    private static final Class<?>[] LOGIN_CON_WITH_ARGS_SIGNATURE =
          {String.class, StreamEngine.class, Args.class};

    @Override
    public int getPriority(String name) {
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
    public LoginCellFactory createFactory(String name, Args args, CellEndpoint parentEndpoint,
          String parentCellName) {
        try {
            Class<? extends CellAdapter> loginClass = Class.forName(name)
                  .asSubclass(CellAdapter.class);
            try {
                Constructor<? extends CellAdapter> constructor = loginClass.getConstructor(
                      LOGIN_CON_WITH_ARGS_SIGNATURE);
                checkConstructor(constructor);
                return new LegacyWithArgsLoginCellFactory(constructor, args, parentEndpoint,
                      parentCellName);
            } catch (NoSuchMethodException e) {
                Constructor<? extends CellAdapter> constructor = loginClass.getConstructor(
                      LOGIN_CON_SIGNATURE);
                checkConstructor(constructor);
                return new LegacyLoginCellFactory(constructor, args, parentEndpoint,
                      parentCellName);
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("No such login cell: " + args.argv(0));
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                  "Login cell lacks appropriate constructor: " + args.argv(0));
        }
    }

    private void checkConstructor(Constructor<? extends Cell> constructor) {
        checkArgument(Modifier.isPublic(constructor.getDeclaringClass().getModifiers()),
              "Login cell is not public");
        checkArgument(Modifier.isPublic(constructor.getModifiers()),
              "Login cell constructor is not public");
        checkArgument(!Modifier.isAbstract(constructor.getModifiers()), "Login cell is abstract");
    }
}

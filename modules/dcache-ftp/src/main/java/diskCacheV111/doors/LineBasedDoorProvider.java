package diskCacheV111.doors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.services.login.LoginCellFactory;
import dmg.cells.services.login.LoginCellProvider;

import org.dcache.util.Args;

public class LineBasedDoorProvider implements LoginCellProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LineBasedDoorProvider.class);

    @Override
    public int getPriority(String name)
    {
        try {
            if (LineBasedInterpreterFactory.class.isAssignableFrom(Class.forName(name))) {
                return 100;
            }
        } catch (ClassNotFoundException ignored) {
        }
        return Integer.MIN_VALUE;
    }

    @Override
    public LoginCellFactory createFactory(String name, Args args, String parentCellName) throws IllegalArgumentException
    {
        try {
            Class<?> interpreter = Class.forName(name);
            if (LineBasedInterpreterFactory.class.isAssignableFrom(interpreter)) {
                LineBasedInterpreterFactory factory = interpreter.asSubclass(LineBasedInterpreterFactory.class).newInstance();
                factory.configure(args);
                return new LineBasedDoorFactory(factory, args, parentCellName);
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOGGER.error("Failed to instantiate interpreter factory: {}", e.toString());
        }
        throw new IllegalArgumentException();
    }
}

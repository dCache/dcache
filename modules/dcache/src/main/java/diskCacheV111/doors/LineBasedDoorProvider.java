package diskCacheV111.doors;

import diskCacheV111.util.ConfigurationException;

import dmg.cells.services.login.LoginCellFactory;
import dmg.cells.services.login.LoginCellProvider;

import org.dcache.util.Args;

public class LineBasedDoorProvider implements LoginCellProvider
{
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
            throw new IllegalArgumentException("Not a LineBasedInterpreterFactory: " + interpreter);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | ConfigurationException e) {
            throw new IllegalArgumentException("Failed to instantiate interpreter factory: " + e.toString(), e);
        }
    }
}

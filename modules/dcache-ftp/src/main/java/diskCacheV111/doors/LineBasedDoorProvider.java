package diskCacheV111.doors;

import dmg.cells.services.login.LoginCellFactory;
import dmg.cells.services.login.LoginCellProvider;
import dmg.util.Args;

import static diskCacheV111.doors.LineBasedDoor.LineBasedInterpreter;

public class LineBasedDoorProvider implements LoginCellProvider
{
    @Override
    public int getPriority(String name)
    {
        try {
            if (LineBasedInterpreter.class.isAssignableFrom(Class.forName(name))) {
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
            if (LineBasedInterpreter.class.isAssignableFrom(interpreter)) {
                return new LineBasedDoorFactory(interpreter.asSubclass(LineBasedInterpreter.class), args, parentCellName);
            }
        } catch (ClassNotFoundException ignored) {
        }
        throw new IllegalArgumentException();
    }
}

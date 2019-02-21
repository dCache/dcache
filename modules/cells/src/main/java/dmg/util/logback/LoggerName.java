package dmg.util.logback;

import org.slf4j.LOGGER;

public class LOGGERName
{
    public static final LOGGERName ROOT =
        new LOGGERName(LOGGER.ROOT_LOGGER_NAME);

    private String _name;

    public static LOGGERName getInstance(LOGGER LOGGER)
    {
        return getInstance(LOGGER.getName());
    }

    public static LOGGERName getInstance(String name)
    {
        if (name.equalsIgnoreCase(LOGGER.ROOT_LOGGER_NAME)) {
            return ROOT;
        } else {
            return new LOGGERName(name);
        }
    }

    public static LOGGERName valueOf(String name)
    {
        return getInstance(name);
    }

    private LOGGERName(String name)
    {
        _name = name;
    }

    public boolean isNameOfLOGGER(LOGGER LOGGER)
    {
        return LOGGER.getName().equals(_name);
    }

    @Override
    public String toString()
    {
        return _name;
    }

    @Override
    public boolean equals(Object that)
    {
        if (this == that) {
            return true;
        }

        if (that == null || !that.getClass().equals(LOGGERName.class)) {
            return false;
        }

        LOGGERName other = (LOGGERName) that;
        return _name.equals(other._name);
    }

    @Override
    public int hashCode()
    {
        return _name.hashCode();
    }

    public LOGGERName getParent()
    {
        if (this == ROOT) {
            return null;
        }
        int pos = Math.max(_name.lastIndexOf('.'), _name.lastIndexOf('$'));
        if (pos > -1) {
            return new LOGGERName(_name.substring(0, pos));
        } else {
            return ROOT;
        }
    }
}

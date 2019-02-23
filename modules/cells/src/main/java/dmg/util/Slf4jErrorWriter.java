package dmg.util;

import org.slf4j.Logger;

/**
 * Slf4j LOGGER adapter implementing the LineWriter interface. Logs
 * all lines at info level.
 */
public class Slf4jErrorWriter
    implements LineWriter
{
    private final Logger LOGGER;

    public Slf4jErrorWriter(Logger LOGGER)
    {
        LOGGER = LOGGER;
    }

    @Override
    public void writeLine(String line)
    {
        LOGGER.error(line);
    }
}

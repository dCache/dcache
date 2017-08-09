package dmg.util;

import org.slf4j.Logger;

/**
 * Slf4j Logger adapter implementing the LineWriter interface. Logs
 * all lines at info level.
 */
public class Slf4jErrorWriter
    implements LineWriter
{
    private final Logger logger;

    public Slf4jErrorWriter(Logger logger)
    {
        this.logger = logger;
    }

    @Override
    public void writeLine(String line)
    {
        this.logger.error(line);
    }
}

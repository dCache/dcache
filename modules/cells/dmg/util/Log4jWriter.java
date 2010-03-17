package dmg.util;

import java.io.Writer;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Log4jWriter
    extends Writer
{
    private final Logger _logger;
    private final Level _level;
    private final StringBuilder _buffer;

    public Log4jWriter(Logger logger, Level level)
    {
        _logger = logger;
        _level = level;
        _buffer = new StringBuilder();
    }

    public void close()
    {
        flush();
    }

    private void flushCompletedLines()
    {
        int i;
        while ((i = _buffer.indexOf("\n")) > -1) {
            _logger.log(_level, _buffer.substring(0, i));
            _buffer.delete(0, i + 1);
        }
    }

    public void flush()
    {
        flushCompletedLines();
        if (_buffer.length() > 0) {
            _logger.log(_level, _buffer.toString());
            _buffer.delete(0, _buffer.length());
        }
    }

    public void write(char[] cbuf, int off, int len)
    {
        _buffer.append(cbuf, off, len);
        flushCompletedLines();
    }
}
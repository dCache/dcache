package dmg.util;

import java.io.Writer;

/**
 * Buffered LineWriter adaptor implementing the Writer interface.
 */
public class BufferedLineWriter
    extends Writer
{
    private final LineWriter _writer;
    private final StringBuilder _buffer;

    public BufferedLineWriter(LineWriter writer)
    {
        _writer = writer;
        _buffer = new StringBuilder();
    }

    @Override
    public void close()
    {
        flush();
    }

    private void flushCompletedLines()
    {
        int i;
        while ((i = _buffer.indexOf("\n")) > -1) {
            _writer.writeLine(_buffer.substring(0, i));
            _buffer.delete(0, i + 1);
        }
    }

    @Override
    public void flush()
    {
        flushCompletedLines();
        if (_buffer.length() > 0) {
            _writer.writeLine(_buffer.toString());
            _buffer.delete(0, _buffer.length());
        }
    }

    @Override
    public void write(char[] cbuf, int off, int len)
    {
        _buffer.append(cbuf, off, len);
        flushCompletedLines();
    }
}
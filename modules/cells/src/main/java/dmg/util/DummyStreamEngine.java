package dmg.util;

import java.net.*;
import java.io.*;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.Channels;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

public class DummyStreamEngine implements StreamEngine
{
    private final static Logger _logger =
        LoggerFactory.getLogger(DummyStreamEngine.class);

    private final Socket _socket;
    private Subject _subject = new Subject();
    private ByteChannel _channel;

    private OutputStream _outputStream;
    private InputStream _inputStream;
    private Reader _reader;
    private Writer _writer;

    public DummyStreamEngine(Socket socket)
    {
        _socket = socket;

        SocketChannel channel = _socket.getChannel();
        if (channel != null) {
            _channel = wrapChannel(channel);
        }

        try {
            Method meth = _socket.getClass().getMethod("getSubject", new Class[0]);
            Subject subject = (Subject)meth.invoke(_socket, new Object[0]);

            setSubject(subject);
        } catch (NoSuchMethodException nsm) {

        } catch (Exception e) {
            _logger.warn("Failed to initialize user name in DummyStreamEngine", e);
        }
    }

    public void setSubject(Subject subject)
    {
        _subject = subject;
    }

    @Override
    public Subject getSubject()
    {
        return _subject;
    }

    @Override
    public InetAddress getInetAddress()
    {
        return _socket.getInetAddress();
    }

    @Override
    public synchronized InputStream getInputStream()
    {
        if (_inputStream == null) {
            if (_channel == null) {
                try {
                    _inputStream = _socket.getInputStream();
                } catch (IOException e) {
                    _logger.error("Could not create input stream: " + e.getMessage());
                }
            } else {
                _inputStream = Channels.newInputStream(_channel);
            }
        }
        return _inputStream;
    }

    @Override
    public synchronized OutputStream getOutputStream()
    {
        if (_outputStream == null) {
            if (_channel == null) {
                try {
                    _outputStream = _socket.getOutputStream();
                } catch (IOException e) {
                    _logger.error("Could not create output stream: " + e.getMessage());
                }
            } else {
                _outputStream = Channels.newOutputStream(_channel);
            }
        }
        return _outputStream;
    }

    @Override
    public synchronized Reader getReader()
    {
        if (_reader == null) {
            if (_channel == null) {
                _reader = new InputStreamReader(getInputStream());
            } else {
                _reader = Channels.
                    newReader(_channel, Charset.defaultCharset().newDecoder(), -1);
            }
        }
        return _reader;
    }

    @Override
    public synchronized Writer getWriter()
    {
        if (_writer == null) {
            if (_channel == null) {
                _writer = new OutputStreamWriter(getOutputStream());
            } else {
                _writer = Channels.
                    newWriter(_channel, Charset.defaultCharset().newEncoder(), -1);
            }
        }
        return _writer;
    }

    @Override
    public Socket getSocket()
    {
        return _socket;
    }

    @Override
    public InetAddress getLocalAddress()
    {
        return _socket.getLocalAddress();
    }

    @Override
    public String getTerminalType()
    {
        return "dumb";
    }

    @Override
    public int getTerminalWidth()
    {
        return 0;
    }

    @Override
    public int getTerminalHeight()
    {
        return 0;
    }

    /**
     * Workaround for Java bug
     * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4509080. The
     * workaround was taken from
     * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4774871.
     */
    private static ByteChannel wrapChannel(final ByteChannel channel)
    {
        return new ByteChannel()
        {
            @Override
            public int write(ByteBuffer src)
                throws IOException
            {
                return channel.write(src);
            }

            @Override
            public int read(ByteBuffer dst)
                throws IOException
            {
                return channel.read(dst);
            }

            @Override
            public boolean isOpen()
            {
                return channel.isOpen();
            }

            @Override
            public void close()
                throws IOException
            {
                channel.close();
            }
        };
    }
}


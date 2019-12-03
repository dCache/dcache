package dmg.util;

import javatunnel.TunnelSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;

public class DummyStreamEngine implements StreamEngine
{
    private static final Logger _logger =
        LoggerFactory.getLogger(DummyStreamEngine.class);

    private final Socket _socket;
    private Subject _subject = new Subject();
    private ByteChannel _channel;

    private OutputStream _outputStream;
    private InputStream _inputStream;
    private Reader _reader;
    private Writer _writer;

    public DummyStreamEngine(Socket socket) throws IOException
    {
        _socket = socket;

        if (socket instanceof TunnelSocket) {
            try {
                ((TunnelSocket) socket).verify();
            } catch (IOException e) {
                socket.close();
                throw new IOException("Tunnel verification failed: " + Exceptions.meaningfulMessage(e));
            }
            setSubject(((TunnelSocket) socket).getSubject());
        }

        SocketChannel channel = _socket.getChannel();
        if (channel != null) {
            _channel = wrapChannel(channel);
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
                    _logger.error("Could not create input stream: {}", e.getMessage());
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
                    _logger.error("Could not create output stream: {}", e.getMessage());
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


package org.dcache.services.info.conduits;

import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import org.dcache.services.info.serialisation.StateSerialiser;

/**
 * Information Exporter class.<br>
 * This class is instantiated by the <code>InfoCollector</code> to send
 * over a plain TCP socket a <code>Schema</code> object that carries out
 * dynamic information from dCache.<br><br>
 * Also this class is independent from the particular implementation of
 * Schema used. As matter of fact, this class serialises a generic Schema
 * object over a socket. It's a job of the client to know what particular
 * implementation of Schema was sent.<br><br>
 * Note that client needs only to know the specializing class of the Schema.
 */
public class XmlConduit extends AbstractThreadedConduit
{
    private static Logger _log = LoggerFactory.getLogger(XmlConduit.class);

    /** TCP port that the server listens on */
    private int _port;

    /** TCP backlog */
    private int _backlog;

    /** IP address to bind to */
    private String _bindAddress;

    /** Server Socket reference */
    private ServerSocket _svr_skt;

    /** Our serialiser for the current dCache state */
    private StateSerialiser _serialiser;

    @Required
    public void setSerialiser(StateSerialiser serialiser)
    {
        _serialiser = serialiser;
    }

    @Required
    public void setPort(int port)
    {
        _port = port;
    }

    public int getPort()
    {
        return _port;
    }

    @Required
    public void setBacklog(int backlog)
    {
        _backlog = backlog;
    }

    public int getBacklog()
    {
        return _backlog;
    }

    @Required
    public void setBindAddress(String address)
    {
        _bindAddress = address;
    }

    public String getBindAddress()
    {
        return _bindAddress;
    }

    @Override
    public void enable()
    {
        try {
            _svr_skt = new ServerSocket(_port, _backlog, InetAddresses.forString(_bindAddress));
        } catch (IOException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (SecurityException e) {
            _log.error("security issue creating port " + _port, e);
            return;
        }
        super.enable(); // start the thread.
    }


    @Override
    void triggerBlockingActivityToReturn()
    {
        if (_svr_skt == null) {
            return;
        }

        try {
            _svr_skt.close();
        } catch (IOException e) {
            _log.error("Problem closing server socket", e);
        } finally {
            _svr_skt = null;
        }
    }


    /**
     * Wait for an incoming connection to the listening socket.  When
     * one is received, send it the XML serialisation of our current state.
     */
    @Override
    void blockingActivity()
    {
        Socket skt = null;

        try {
            skt = _svr_skt.accept();
        } catch (SocketException e) {
            if (_svr_skt != null && (this._should_run || !_svr_skt.isClosed())) {
                _log.error("accept() failed", e);
            }
        } catch (IOException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (SecurityException e) {
            _log.error ("accept() failed for security reasons", e);
            return;
        } catch (Exception e) {
            _log.error("accept() failed for an unknown reason", e);
            return;
        }

        if (skt != null) {
            if (_log.isInfoEnabled()) {
                _log.info("Incoming connection from " + skt
                        .toString());
            }

            try {
                _callCount++;
                String data = _serialiser.serialise();
                skt.getOutputStream().write(data.getBytes());
            } catch (IOException e) {
                _log.error("failed to write XML data", e);
            } catch (Exception e) {
                _log.error("unknown failure writing XML data", e);
            } finally {
                try {
                    skt.close();
                } catch (IOException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}

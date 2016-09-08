package dmg.cells.network;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.UnresolvedAddressException;
import java.nio.channels.UnsupportedAddressTypeException;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import dmg.cells.nucleus.CellAdapter;
import dmg.util.DummyStreamEngine;
import dmg.util.StreamEngine;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.util.Args;
import org.dcache.util.NDC;

import static java.util.Objects.requireNonNull;

public class LocationManagerConnector
    extends CellAdapter
    implements Runnable
{
    private static final Logger _log =
        LoggerFactory.getLogger("org.dcache.cells.network");

    private final String _domain;
    private final SocketFactory _ssf;
    private final InetSocketAddress _address;
    private Thread _thread;
    private volatile String _status = "disconnected";
    private volatile int _retries;
    private volatile boolean _isRunning;

    public LocationManagerConnector(String cellName, String args, SocketFactory socketFactory)
    {
        super(cellName, "System", args);
        Args a = getArgs();
        _domain = a.getOpt("domain");
        _ssf = requireNonNull(socketFactory);
        HostAndPort where = HostAndPort.fromString(a.getOpt("where"));
        _address = new InetSocketAddress(where.getHostText(), where.getPort());
    }

    @Override
    protected void started()
    {
        _thread = getNucleus().newThread(this, "TunnelConnector-" + _domain);
        _thread.start();
        _isRunning = true;
    }

    private StreamEngine connect()
        throws IOException, InterruptedException
    {
        _status = "Connecting to " + _address;
        Socket socket;
        try {
            socket = _ssf.createSocket(_address.getAddress(), _address.getPort());
        } catch (UnsupportedAddressTypeException e) {
            throw new IOException("Unsupported address type: " + _address, e);
        } catch (UnresolvedAddressException e) {
            throw new IOException("Unable to resolve " + _address, e);
        } catch (InterruptedIOException | ClosedByInterruptException e) {
            throw e;
        } catch (IOException e) {
            throw new IOException("Failed to connect to " + _address + ": " + e, e);
        }
        socket.setKeepAlive(true);
        _log.info("Connecting using {}", socket.getClass().getSimpleName());
        return new DummyStreamEngine(socket);
    }

    @Override
    public void run()
    {
        /* Thread for creating the tunnel. There is a grace period of
         * 4 to 20 seconds between connection attempts. The thread is
         * terminated by interrupting it.
         */
        Args args = getArgs();
        String name = getCellName() + '*';
        Random random = new Random();
        NDC.push(_address.toString());
        try {
            while (_isRunning) {
                try {
                    _retries++;

                    LocationMgrTunnel tunnel = new LocationMgrTunnel(name, connect(), args);
                    try {
                        tunnel.start().get();
                        _retries = 0;
                        _status = "Connected";
                        getNucleus().join(tunnel.getCellName());
                    } finally {
                        getNucleus().kill(tunnel.getCellName());
                    }
                } catch (InterruptedIOException | ClosedByInterruptException e) {
                    throw e;
                } catch (ExecutionException | IOException e) {
                    _log.warn(AlarmMarkerFactory.getMarker(PredefinedAlarm.LOCATION_MANAGER_FAILURE,
                                                           name,
                                                           _domain,
                                                           e.getMessage()),
                              "Failed to connect to " + _domain + ": "
                                                      + e.getMessage());
                }

                _status = "Sleeping";
                long sleep = random.nextInt(16000) + 4000;
                _log.warn("Sleeping {} seconds", sleep / 1000);
                Thread.sleep(sleep);
            }
        } catch (InterruptedIOException | InterruptedException | ClosedByInterruptException ignored) {
        } finally {
            NDC.pop();
            _status = "Terminated";
        }
    }

    public String toString()
    {
        return _status;
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("Location manager connector : " + getCellName());
        pw.println("Status   : " + _status);
        pw.println("Retries  : " + _retries);
    }

    @Override
    public void stopped()
    {
        _isRunning = false;
        Thread thread = _thread;
        if (thread != null) {
            thread.interrupt();
            Uninterruptibles.joinUninterruptibly(thread);
        }
    }
}

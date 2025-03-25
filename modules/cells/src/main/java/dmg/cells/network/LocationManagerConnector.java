package dmg.cells.network;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Uninterruptibles;
import dmg.cells.nucleus.CellAdapter;
import dmg.util.DummyStreamEngine;
import dmg.util.Exceptions;
import dmg.util.StreamEngine;
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
import javax.net.SocketFactory;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.util.Args;
import org.dcache.util.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationManagerConnector
      extends CellAdapter
      implements Runnable {

    private static final Logger _log =
          LoggerFactory.getLogger("org.dcache.cells.network");

    private final String _domain;
    private final SocketFactory _ssf;
    private final InetSocketAddress _address;
    private Thread _thread;
    private volatile String _status = "disconnected";
    private volatile int _retries;
    private volatile boolean _isRunning;

    /**
     * Create a new connector to the location manager.
     * @param cellName the name of this connector as a cell
     * @param socketFactory the socket factory to use for creating connections
     * @param remoteDomain the domain name of the location manager
     * @param endpoint the address of the location manager
     */
    public LocationManagerConnector(String cellName, SocketFactory socketFactory, String remoteDomain, InetSocketAddress endpoint) {
        super(cellName, "System");
        _domain = remoteDomain;
        _ssf = requireNonNull(socketFactory);
        _address = endpoint;
    }

    @Override
    protected void started() {
        _thread = getNucleus().newThread(this, "TunnelConnector-" + _domain);
        _isRunning = true;
        _thread.start();
    }

    private StreamEngine connect()
          throws IOException, InterruptedException {
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
    public void run() {
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
                } catch (InterruptedIOException | InterruptedException | ClosedByInterruptException e) {
                    _log.warn("Connection to {} ({}) interrupted. Reason: {}", _domain, _address, e.toString());
                } catch (ExecutionException | IOException e) {
                    String error = Exceptions.meaningfulMessage(Throwables.getRootCause(e));
                    _log.warn(AlarmMarkerFactory.getMarker(PredefinedAlarm.LOCATION_MANAGER_FAILURE,
                                name,
                                _domain,
                                    error),
                          "Failed to connect to " + _domain + ": " + e);
                }

                _status = "Sleeping";
                long sleep = random.nextInt(16000) + 4000;
                _log.warn("Sleeping {} seconds", sleep / 1000);
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    // restore interrupted status
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            NDC.pop();
            _thread = null;
            _status = "Terminated";
        }
    }

    public String toString() {
        return _status;
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.println("Location manager connector : " + getCellName());
        pw.println("Status   : " + _status);
        pw.println("Retries  : " + _retries);
    }

    @Override
    public void stopped() {
        _isRunning = false;
        Thread thread = _thread;
        if (thread != null) {
            thread.interrupt();
            Uninterruptibles.joinUninterruptibly(thread);
        }
    }
}

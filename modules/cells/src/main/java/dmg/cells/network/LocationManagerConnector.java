package dmg.cells.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.nio.channels.UnsupportedAddressTypeException;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.DummyStreamEngine;
import dmg.util.StreamEngine;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.util.Args;

public class LocationManagerConnector
    extends CellAdapter
    implements Runnable
{
    private static final Logger _log =
        LoggerFactory.getLogger("org.dcache.cells.network");

    private final String _domain;
    private final String _lm;
    private Thread _thread;
    private String _status = "disconnected";
    private int _retries;

    public LocationManagerConnector(String cellName, String args)
    {
        super(cellName, "System", args);
        Args a = getArgs();
        _domain = a.getOpt("domain");
        _lm = a.getOpt("lm");
    }

    @Override
    protected void started()
    {
        _thread = getNucleus().newThread(this, "TunnelConnector");
        _thread.start();
    }

    private synchronized void setStatus(String s)
    {
        _status = s;
    }

    private synchronized String getStatus()
    {
        return _status;
    }

    private String whereIs(String domain)
        throws IOException, InterruptedException
    {
        try {
            String      query = "where is " + domain;
            CellPath    path  = new CellPath(_lm);
            CellMessage reply = getNucleus().sendAndWait(new CellMessage(path, query), 5000);

            if (reply == null) {
                throw new IOException("Timeout querying location manager");
            }

            Object obj = reply.getMessageObject();
            if (obj == null || !(obj instanceof String)) {
                throw new IOException("Invalid reply from location manager");
            }

            return obj.toString();
        } catch (NoRouteToCellException e) {
            throw new IOException("No route to location manager", e);
        } catch (ExecutionException e) {
            throw new IOException(e.getCause().getMessage(), e);
        }
    }

    private StreamEngine connect(String domain)
        throws IOException, InterruptedException
    {
        setStatus("Locating " + domain);
        Args reply = new Args(whereIs(domain));

        if (reply.argc() < 3 ||
            !reply.argv(0).equals("location") ||
            !reply.argv(1).equals(domain)) {
            throw new IOException("Invalid reply from location manager: " + reply);
        }

        String[] s = reply.argv(2).split(":");
        if (s.length != 2) {
            throw new IOException("Invalid address: " + reply.argv(2));
        }
        InetSocketAddress address =
            new InetSocketAddress(s[0], Integer.parseInt(s[1]));


        setStatus("Connecting to " + address);
        Socket socket;
        try {
            socket = SocketChannel.open(address).socket();
        } catch (UnsupportedAddressTypeException e) {
            throw new IOException("Unsupported address type: " + address, e);
        } catch (UnresolvedAddressException e) {
            throw new IOException("Unable to resolve " + address, e);
        } catch (IOException e) {
            throw new IOException("Failed to connect to " + address + ": " + e.toString(), e);
        }
        socket.setKeepAlive(true);

        String security = reply.getOpt("security");
        if (security == null) {
            _log.info("Using clear text channel");
            return new DummyStreamEngine(socket);
        } else {
            /* Currently unused, but somewhere around here we would need to hook SSL support in
             * if we wanted to do that.
             */
            Args x = new Args(security);
            String prot = x.getOpt("prot");
            if (prot == null) {
                if (x.argc() == 0) {
                    socket.close();
                    throw new
                        IOException("Not a proper security context \""+security+"\"");
                }
                prot = x.argv(0);
            }

            socket.close();
            throw new IOException("Security mode not supported : " + security);
        }
    }

    @Override
    public void run()
    {
        /* Thread for creating the tunnel. There is a grace period of
         * 4 to 30 seconds between connection attempts. The thread is
         * terminated by interrupting it.
         */
        Args args = getArgs();
        String name = getCellName() + "*";
        Random random = new Random();
        try {
            while (true) {
                try {
                    _retries++;
                    LocationMgrTunnel tunnel =
                        new LocationMgrTunnel(name, connect(_domain), args);
                    tunnel.start().get();
                    _retries = 0;
                    setStatus("Connected");
                    tunnel.join();
                } catch (InterruptedIOException e) {
                    throw e;
                } catch (ExecutionException | IOException e) {
                    _log.warn(AlarmMarkerFactory.getMarker(PredefinedAlarm.LOCATION_MANAGER_FAILURE,
                                                           name,
                                                           _domain,
                                                           e.getMessage()),
                              "Failed to connect to " + _domain + ": "
                                                      + e.getMessage());
                }

                setStatus("Sleeping");
                long sleep = random.nextInt(26000) + 4000;
                _log.warn("Sleeping " + (sleep / 1000) + " seconds");
                Thread.sleep(sleep);
            }
        } catch (InterruptedIOException | InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public String toString()
    {
        return getStatus();
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("Location manager connector : " + getCellName());
        pw.println("Status   : " + getStatus());
        pw.println("Retries  : " + _retries);
    }

    @Override
    public void cleanUp()
    {
        if (_thread != null) {
            _thread.interrupt();
        }
    }
}

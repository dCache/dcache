package dmg.cells.network;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.services.login.SshCAuth_Key;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;
import dmg.util.StreamEngine;
import dmg.util.DummyStreamEngine;
import dmg.protocols.ssh.SshStreamEngine;

public class LocationManagerConnector
    extends CellAdapter
    implements Runnable
{
    private final static Logger _log =
        LoggerFactory.getLogger("org.dcache.cells.network");

    private final String _domain;
    private final String _lm;
    private final Thread _thread;
    private String _status = "disconnected";
    private int _retries;

    public LocationManagerConnector(String cellName, String args)
    {
        super(cellName, "System", args, true);

        Args a = getArgs();
        _domain = a.getOpt("domain");
        _lm = a.getOpt("lm");

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
            CellMessage reply = sendAndWait(new CellMessage(path, query), 5000);

            if (reply == null) {
                throw new IOException("Timeout querying location manager");
            }

            Object obj = reply.getMessageObject();
            if (obj == null || !(obj instanceof String)) {
                throw new IOException("Invalid reply from location manager");
            }

            return obj.toString();
        } catch (NoRouteToCellException e) {
            throw new IOException("No route to location manager");
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
        Socket socket = SocketChannel.open(address).socket();
        socket.setKeepAlive(true);

        String security = reply.getOpt("security");
        if (security == null) {
            _log.info("Using clear text channel");
            return new DummyStreamEngine(socket);
        } else {
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

            if (!prot.equalsIgnoreCase("ssh") &&
                !prot.equalsIgnoreCase("ssh1")) {
                socket.close();
                throw new
                    IOException("Security mode not supported : " + security);
            }

            _log.info("Using encrypted channel");
            try {
                SshCAuth_Key key = new SshCAuth_Key(getNucleus(), getArgs());
                return new SshStreamEngine(socket, key);
            } catch (Exception e) {
                throw new IOException("Failure creating SSH stream engine: " + e.getMessage());
            }
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
                    _retries = 0;
                    setStatus("Connected");
                    tunnel.join();
                } catch (InterruptedIOException e) {
                    throw e;
                } catch (IOException e) {
                    _log.warn("Failed to connect to " + _domain + ": " + e.getMessage());
                }

                setStatus("Sleeping");
                long sleep = random.nextInt(26000) + 4000;
                _log.warn("Sleeping " + (sleep / 1000) + " seconds");
                Thread.sleep(sleep);
            }
        } catch (InterruptedIOException e) {
            Thread.currentThread().interrupt();
        } catch (InterruptedException e) {
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
        _thread.interrupt();
    }
}

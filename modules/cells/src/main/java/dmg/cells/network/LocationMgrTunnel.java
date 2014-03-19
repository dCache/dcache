package dmg.cells.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellDomainInfo;
import dmg.cells.nucleus.CellExceptionMessage;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.CellTunnel;
import dmg.cells.nucleus.CellTunnelInfo;
import dmg.cells.nucleus.MessageEvent;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.RoutedMessageEvent;
import dmg.cells.nucleus.SerializationException;
import dmg.util.StreamEngine;

import org.dcache.util.Args;
import org.dcache.util.Version;

/**
 *
 *
 * @author Patrick Fuhrmann
 * @version 0.1, 5 Mar 2001
 */
public class LocationMgrTunnel
    extends CellAdapter
    implements CellTunnel, Runnable
{

    /**
     * We use a single shared instance of Tunnels to coordinate route
     * creation between tunnels.
     */
    private final static Tunnels _tunnels = new Tunnels();

    private final static Logger _log =
        LoggerFactory.getLogger(LocationMgrTunnel.class);

    private final CellNucleus  _nucleus;

    private CellDomainInfo  _remoteDomainInfo;
    private final Socket _socket;

    private final OutputStream _rawOut;
    private final InputStream _rawIn;

    private ObjectSource _input;
    private ObjectSink _output;

    private boolean _down;

    //
    // some statistics
    //
    private int  _messagesToTunnel;
    private int  _messagesToSystem;

    public LocationMgrTunnel(String cellName, StreamEngine engine, Args args)
        throws IOException
    {
        super(cellName, "System", args, false);

        try {
            _nucleus = getNucleus();
            _socket = engine.getSocket();
            _socket.setTcpNoDelay(true);

            _rawOut = new BufferedOutputStream(engine.getOutputStream());
            _rawIn = new BufferedInputStream(engine.getInputStream());
        } catch (IOException e) {
            start();
            kill();
            throw e;
        }
        getNucleus().newThread(this, "Tunnel").start();
    }

    private void handshake() throws IOException
    {
        try  {
            ObjectOutputStream out = new ObjectOutputStream(_rawOut);
            out.writeObject(new CellDomainInfo(_nucleus.getCellDomainName(),
                    Version.of(LocationMgrTunnel.class).getVersion()));
            out.flush();
            ObjectInputStream in = new ObjectInputStream(_rawIn);

            _remoteDomainInfo = (CellDomainInfo) in.readObject();

            if (_remoteDomainInfo == null) {
                throw new IOException("EOS encountered while reading DomainInfo");
            }

            _input = new JavaObjectSource(in);
            _output = new JavaObjectSink(out);
        } catch (ClassNotFoundException e) {
            throw new IOException("Cannot deserialize object. This is most likely due to a version mismatch.", e);
        }

        _log.debug("Established tunnel to {}", getRemoteDomainName());
    }

    synchronized private void setDown(boolean down)
    {
        _down = down;
        notifyAll();
    }

    synchronized private boolean isDown()
    {
        return _down;
    }

    private void returnToSender(CellMessage msg, NoRouteToCellException e)
        throws SerializationException
    {
        try {
            if (!(msg instanceof CellExceptionMessage)) {
                CellPath retAddr = msg.getSourcePath().revert();
                CellExceptionMessage ret = new CellExceptionMessage(retAddr, e);
                ret.setLastUOID(msg.getUOID());
                _nucleus.sendMessage(ret, true, true);
            }
        } catch (NoRouteToCellException f) {
            _log.warn("Unable to deliver message and unable to return it to sender: " + msg);
        }
    }

    private void receive()
        throws IOException, ClassNotFoundException
    {
        CellMessage msg;
        while ((msg = _input.readObject()) != null) {
            try {
                sendMessage(msg);
                _messagesToSystem++;
            } catch (NoRouteToCellException e) {
                returnToSender(msg, e);
            }
        }
    }

    @Override
    public void run()
    {
        if (isDown()) {
            throw new IllegalStateException("Tunnel has already been closed");
        }

        try {
            handshake();
            start();

            _tunnels.add(this);
            try {
                receive();
            } finally {
                _tunnels.remove(this);
            }
        } catch (EOFException | InterruptedException e) {
        } catch (ClassNotFoundException e) {
            _log.warn("Cannot deserialize object. This is most likely due to a version mismatch.");
        } catch (IOException e) {
            _log.warn("Error while reading from tunnel: " + e.getMessage());
        } finally {
            start();
            kill();
        }
    }

    @Override
    public void messageArrived(MessageEvent me)
    {
        if (me instanceof RoutedMessageEvent) {
            CellMessage msg = me.getMessage();
            try {
                if (isDown()) {
                    throw new IOException("Tunnel has been shut down.");
                }
                _messagesToTunnel++;
                _output.writeObject(msg);
            } catch (IOException e) {
                _log.warn("Error while sending message: " + e.getMessage());
                returnToSender(msg,
                               new NoRouteToCellException("Communication failure. Message could not be delivered."));
                kill();
            }
        } else {
            super.messageArrived(me);
        }
    }

    @Override
    public CellTunnelInfo getCellTunnelInfo()
    {
        return new CellTunnelInfo(getCellName(),
                new CellDomainInfo(_nucleus.getCellDomainName()),
                                  _remoteDomainInfo);
    }

    protected String getRemoteDomainName()
    {
        return (_remoteDomainInfo == null)
            ? ""
            : _remoteDomainInfo.getCellDomainName();
    }

    public String toString()
    {
        return "Connected to " + getRemoteDomainName();
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("Location Mgr Tunnel : " + getCellName());
        pw.println("-> Tunnel     : " + _messagesToTunnel);
        pw.println("-> Domain     : " + _messagesToSystem);
        pw.println("Peer          : " + getRemoteDomainName());
    }

    @Override
    public void cleanUp()
    {
        _log.info("Closing tunnel to " + getRemoteDomainName());
        setDown(true);
        try {
            _socket.shutdownInput();
            _socket.close();
        } catch (IOException e) {
            _log.warn("Failed to close socket: " + e.getMessage());
        }
    }

    public synchronized void join() throws InterruptedException
    {
        while (!isDown()) {
            wait();
        }
    }

    /**
     * This class encapsulates routing table management. It ensures
     * that at most one tunnel to any given domain is registered at a
     * time.
     *
     * It is assumed that all tunnels share the same cell glue (this
     * is normally the case for cells in the same domain).
     */
    private static class Tunnels
    {
        private Map<String,LocationMgrTunnel> _tunnels =
                new HashMap<>();

        /**
         * Adds a new tunnel. A route for the tunnel destination is
         * registered in the CellNucleus. The same tunnel cannot be
         * registered twice; unregister it first.
         *
         * If another tunnel is already registered for the same
         * destination, then the other tunnel is killed.
         */
        public synchronized void add(LocationMgrTunnel tunnel)
                throws InterruptedException
        {
            CellNucleus nucleus = tunnel.getNucleus();

            if (_tunnels.containsValue(tunnel)) {
                throw new IllegalArgumentException("Cannot register the same tunnel twice");
            }

            String domain = tunnel.getRemoteDomainName();

            /* Kill old tunnel first.
             */
            LocationMgrTunnel old;
            while ((old = _tunnels.get(domain)) != null) {
                old.kill();
                wait();
            }

            /* Add new route.
             */
            CellRoute route = new CellRoute(domain,
                    tunnel.getCellName(),
                    CellRoute.DOMAIN);
            try {
                nucleus.routeAdd(route);
            } catch (IllegalArgumentException e) {
                /* Crap, somehow the entry already exists. Well, we
                 * insist on adding a new one, so we delete the old
                 * one first.
                 */
                nucleus.routeDelete(route);
                nucleus.routeAdd(route);
            }

            /* Keep track of what we did.
             */
            _tunnels.put(domain, tunnel);
            notifyAll();
        }

        /**
         * Removes a tunnel and unregisters its routes. If the tunnel
         * was already removed, then nothing happens.
         *
         * It is crucial that the <code>_remoteDomainInfo</code> of
         * the tunnel does not change between the point at which it is
         * added and the point at which it is removed.
         */
        public synchronized void remove(LocationMgrTunnel tunnel)
        {
            CellNucleus nucleus = tunnel.getNucleus();
            String domain = tunnel.getRemoteDomainName();
            if (_tunnels.get(domain) == tunnel) {
                _tunnels.remove(domain);
                nucleus.routeDelete(new CellRoute(domain,
                        tunnel.getCellName(),
                        CellRoute.DOMAIN));
                notifyAll();
            }
        }
    }

    private interface ObjectSource
    {
        CellMessage readObject() throws IOException, ClassNotFoundException;
    }

    private interface ObjectSink
    {
        void writeObject(CellMessage message) throws IOException;
    }

    private static class JavaObjectSource implements ObjectSource
    {
        private ObjectInputStream in;

        private JavaObjectSource(ObjectInputStream in)
        {
            this.in = in;
        }

        @Override
        public CellMessage readObject() throws IOException, ClassNotFoundException
        {
            return (CellMessage) in.readObject();
        }
    }

    private static class JavaObjectSink implements ObjectSink
    {
        private ObjectOutputStream out;

        private JavaObjectSink(ObjectOutputStream out)
        {
            this.out = out;
        }

        @Override
        public void writeObject(CellMessage message) throws IOException
        {
            /* An object output stream will only serialize an object once
             * and likewise the object input stream will recreate the
             * object DAG at the other end. To avoid that the receiver
             * needs to unnecessarily keep references to previous objects,
             * we reset the stream. Notice that resetting the stream sends
             * a reset messsage. Hence we reset the stream before flushing
             * it.
             */
            out.writeObject(message);
            out.reset();
            out.flush();
        }
    }
}

package dmg.cells.network;

import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Map;
import java.util.HashMap;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellExceptionMessage;
import dmg.cells.nucleus.CellDomainInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.CellTunnel;
import dmg.cells.nucleus.CellTunnelInfo;
import dmg.cells.nucleus.MessageEvent;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.RoutedMessageEvent;
import dmg.cells.nucleus.SerializationException;
import dmg.util.Args;
import dmg.util.StreamEngine;

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
     * This class encapsulates routing table management. It ensures
     * that at most one tunnel to any given domain is registered at a
     * time.
     *
     * It is assumed that all tunnels share the same cell glue (this
     * is normally the case for cells in the same domain).
     */
    static class Tunnels
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

    /**
     * We use a single shared instance of Tunnels to coordinate route
     * creation between tunnels.
     */
    private final static Tunnels _tunnels = new Tunnels();

    private final static Logger _log =
        LoggerFactory.getLogger(LocationMgrTunnel.class);

    private final static Logger _logMessages =
        LoggerFactory.getLogger("logger.org.dcache.cells.messages");

    private final CellNucleus  _nucleus;

    private CellDomainInfo  _remoteDomainInfo;
    private final Socket _socket;
    private final ObjectInputStream  _input;
    private final ObjectOutputStream _output;

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

            /* The constructor of ObjectOutputStream writes a header
             * to the stream. Hence we flush the stream after
             * creation.
             */
            _output = new ObjectOutputStream(new BufferedOutputStream(engine.getOutputStream()));
            _output.flush();
            _input = new ObjectInputStream(new BufferedInputStream(engine.getInputStream()));
        } catch (IOException e) {
            start();
            kill();
            throw e;
        }
        getNucleus().newThread(this, "Tunnel").start();
    }

    private CellDomainInfo negotiateDomainInfo(ObjectOutputStream out,
                                               ObjectInputStream in)
        throws IOException
    {
        try  {
            send(_nucleus.getCellDomainInfo());

            Object obj = in.readObject();
            if (obj == null) {
                throw new IOException("EOS encountered while reading DomainInfo");
            }
            return (CellDomainInfo)obj;
        } catch (ClassNotFoundException e) {
            throw new IOException("Cannot deserialize object. This is most likely due to a version mismatch.");
        }
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

    private void logSend(CellMessage msg)
    {
        if (_logMessages.isDebugEnabled()) {
            Object object = msg.getMessageObject();
            String messageObject =
                object == null ? "NULL" : object.getClass().getName();
            _logMessages.debug("tunnelMessageSent src="
                               + msg.getSourcePath()
                               + " dest=" + msg.getDestinationPath()
                               + " [" + messageObject + "] UOID="
                               + msg.getUOID().toString());
        }
    }

    private void logReceive(CellMessage msg)
    {
        if (_logMessages.isDebugEnabled()) {
            Object object = msg.getMessageObject();
            String messageObject =
                object == null ? "NULL" : object.getClass().getName();

            _logMessages.debug("tunnelMessageReceived src="
                               + msg.getSourcePath()
                               + " dest=" + msg.getDestinationPath()
                               + " [" + messageObject + "] UOID="
                               + msg.getUOID().toString());
        }
    }

    private void returnToSender(CellMessage msg, NoRouteToCellException e)
        throws SerializationException
    {
        try {
            if (!(msg instanceof CellExceptionMessage)) {
                CellPath retAddr = (CellPath)msg.getSourcePath().clone();
                retAddr.revert();
                CellExceptionMessage ret = new CellExceptionMessage(retAddr, e);
                ret.setLastUOID(msg.getUOID());
                _nucleus.sendMessage(ret);
            }
        } catch (NoRouteToCellException f) {
            _log.warn("Unable to deliver message and unable to return it to sender: " + msg);
        }
    }

    private void receive()
        throws IOException, ClassNotFoundException
    {
        CellMessage msg;
        while ((msg = (CellMessage)_input.readObject()) != null) {
            logReceive(msg);

            try {
                sendMessage(msg);
                _messagesToSystem++;
            } catch (NoRouteToCellException e) {
                returnToSender(msg, e);
            }
        }
    }

    private void send(Object msg)
        throws IOException
    {
        if (isDown()) {
            throw new IOException("Tunnel has been shut down.");
        }

        _output.writeObject(msg);

        /* An object output stream will only serialize an object ones
         * and likewise the object input stream will recreate the
         * object DAG at the other end. To avoid that the receiver
         * needs to unnecessarily keep references to previous objects,
         * we reset the stream. Notice that resetting the stream sends
         * a reset messsage. Hence we reset the stream before flushing
         * it.
         */
        _output.reset();
        _output.flush();
    }

    @Override
    public void run()
    {
        if (isDown()) {
            throw new IllegalStateException("Tunnel has already been closed");
        }

        try {
            _remoteDomainInfo = negotiateDomainInfo(_output, _input);
            _log.info("Established tunnel to " + getRemoteDomainName());

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
                logSend(msg);
                _messagesToTunnel++;
                send(msg);
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
                                  _nucleus.getCellDomainInfo(),
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
}

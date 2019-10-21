/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2001 - 2018 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dmg.cells.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.channels.AsynchronousCloseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellDomainInfo;
import dmg.cells.nucleus.CellDomainRole;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.CellTunnel;
import dmg.cells.nucleus.CellTunnelInfo;
import dmg.cells.nucleus.MessageEvent;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.RoutedMessageEvent;
import dmg.cells.nucleus.SerializationHandler;
import dmg.util.Releases;
import dmg.util.Releases.BadVersionException;
import dmg.util.StreamEngine;

import org.dcache.util.Args;
import org.dcache.util.NDC;
import org.dcache.util.Version;

public class LocationMgrTunnel
    extends CellAdapter
    implements CellTunnel, Runnable
{
    /**
     * We use a single shared instance of Tunnels to coordinate route
     * creation between tunnels.
     */
    private static final Tunnels _tunnels = new Tunnels();

    private static final Logger _log =
        LoggerFactory.getLogger(LocationMgrTunnel.class);

    private final CellNucleus  _nucleus;

    private final CellDomainInfo  _localDomainInfo;
    private CellDomainInfo  _remoteDomainInfo;
    private boolean _allowForwardingOfRemoteMessages;

    private Thread _thread;
    private final Socket _socket;

    private final OutputStream _rawOut;
    private final InputStream _rawIn;

    private ObjectSource _input;
    private ObjectSink _output;

    private SerializationHandler.Serializer _serializer;

    //
    // some statistics
    //
    private LongAdder _messagesToTunnel = new LongAdder();
    private LongAdder _messagesToSystem = new LongAdder();

    public LocationMgrTunnel(String cellName, StreamEngine engine, Args args)
    {
        super(cellName, "System", args);
        _nucleus = getNucleus();
        _socket = engine.getSocket();
        _rawOut = new BufferedOutputStream(engine.getOutputStream());
        _rawIn = new BufferedInputStream(engine.getInputStream());
        CellDomainRole role = args.hasOption("role") ? CellDomainRole.valueOf(
                args.getOption("role").toUpperCase()) : CellDomainRole.SATELLITE;
        _localDomainInfo = new CellDomainInfo(_nucleus.getCellDomainName(),
                                              Version.of(LocationMgrTunnel.class).getVersion(),
                                              role,
                                              _nucleus.getZone());
    }

    @Override
    protected void starting() throws Exception
    {
        _socket.setTcpNoDelay(true);
        handshake();
        _tunnels.add(this);
    }

    @Override
    protected void started()
    {
        installRoutes();
        _thread = _nucleus.newThread(this, "Tunnel");
        _thread.start();
    }

    @Override
    public void stopped()
    {
        _log.info("Closing tunnel to {}", getRemoteDomainName());
        _tunnels.remove(this);
        try {
            try {
                _socket.shutdownOutput();
                if (_thread != null) {
                    _thread.join(2_000);
                }
            } catch (IOException e) {
                _log.debug("Failed to shutdown socket: {}", e.getMessage());
            } catch (UnsupportedOperationException ignored) {
                // SSLSocket does not support shutdown.
            }  catch (InterruptedException ignored) {
            }
        } finally {
            try {
                _socket.close();
            } catch (IOException e) {
                _log.warn("Failed to close socket: {}", e.getMessage());
            }
        }
    }

    private void installRoutes()
    {
        String domain = getRemoteDomainName();
        CellNucleus nucleus = getNucleus();

        /* Add domain route.
         */
        CellRoute route = new CellRoute(domain, nucleus.getThisAddress(), nucleus.getZone(), CellRoute.DOMAIN);
        try {
            nucleus.routeAdd(route);
        } catch (IllegalArgumentException e) {
            _log.warn("Failed to add route: {}", e.getMessage());
        }
    }

    private void handshake() throws IOException
    {
        try {
            ObjectOutputStream out = new ObjectOutputStream(_rawOut);
            out.writeObject(_localDomainInfo);
            out.flush();
            ObjectInputStream in = new ObjectInputStream(_rawIn);

            _remoteDomainInfo = (CellDomainInfo) in.readObject();
            if (_remoteDomainInfo == null) {
                throw new IOException("Remote dCache domain disconnected during handshake.");
            }
            short release = _remoteDomainInfo.getRelease();
            if (release < Releases.RELEASE_3_0) {
                throw new IOException("Connection from incompatible domain " + _remoteDomainInfo + " rejected.");
            } else {
                _log.debug("Using raw serialization for message envelope.");

                // Due to lack of message versioning support in FST, we always use JOS with different dCache versions
                boolean samedVersionEndpoint = release == _localDomainInfo.getRelease();

                SerializationHandler.Serializer serializer = samedVersionEndpoint ?
                        _nucleus.getMsgSerialization() : SerializationHandler.Serializer.JOS;
                this._serializer = serializer;
                /* Since dCache 3.0 we use raw encoding of CellMessage. */
                _input = new RawObjectSource(_rawIn);

                _output = new RawObjectSink(_rawOut, serializer);
            }

            _allowForwardingOfRemoteMessages = (_remoteDomainInfo.getRole() != CellDomainRole.CORE);

            _log.info("Established connection with {}", _remoteDomainInfo);
        } catch (BadVersionException e) {
            throw new IOException("Invalid information presented during handshake: " + e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            throw new IOException("Cannot deserialize object. This is most likely due to a version mismatch.", e);
        }
    }

    @Override
    public void run()
    {
        NDC.push(_remoteDomainInfo.toString());
        try {
            CellMessage msg;
            while ((msg = _input.readObject()) != null) {
                getNucleus().sendMessage(msg, true, _allowForwardingOfRemoteMessages, false);
                _messagesToSystem.increment();
            }
        } catch (AsynchronousCloseException | EOFException ignored) {
        } catch (ClassNotFoundException e) {
            _log.warn("Cannot deserialize object. This is most likely due to a version mismatch.");
        } catch (IOException e) {
            _log.warn("Error while reading from tunnel: {}", e.toString());
        } finally {
            kill();
            NDC.pop();
        }
    }

    @Override
    public void messageArrived(MessageEvent me)
    {
        if (me instanceof RoutedMessageEvent) {
            CellMessage msg = me.getMessage();
            try {
                _messagesToTunnel.increment();
                _output.writeObject(msg);
            } catch (IOException e) {
                NDC.push(_remoteDomainInfo.toString());
                try {
                    kill();
                    _log.warn("Error while sending message: {}", e.getMessage());
                    NoRouteToCellException noRoute =
                            new NoRouteToCellException(msg, "Communication failure. Message could not be delivered.");
                    CellMessage envelope = new CellMessage(msg.getSourcePath().revert(), noRoute);
                    envelope.setLastUOID(msg.getUOID());
                    _nucleus.sendMessage(envelope, true, true, true);
                } finally {
                    NDC.pop();
                }
            }
        } else {
            super.messageArrived(me);
        }
    }

    @Override
    public CellTunnelInfo getCellTunnelInfo()
    {
        return new CellTunnelInfo(getNucleus().getThisAddress(), _localDomainInfo, _remoteDomainInfo);
    }

    private String getRemoteDomainName()
    {
        return (_remoteDomainInfo == null)
            ? ""
            : _remoteDomainInfo.getCellDomainName();
    }

    /**
     * Return the zone within which the remote site resides.  Returns empty if
     * the tunnel is disconnected, the handshake has not yet taken place, or the
     * remote domain is dCache v5.1 (or earlier) or is configured not to have
     * a zone.
     * @return the remote domain's zone
     */
    public Optional<String> getRemoteZone()
    {
        return _remoteDomainInfo == null
                ? Optional.empty()
                : _remoteDomainInfo.getZone();
    }

    @Override
    public String toString()
    {
        return "Connected to " + getRemoteDomainName() + getRemoteZone().map(z -> " in zone " + z).orElse("");
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("Tunnel                    : " + getCellName());
        pw.println("Message payload serializer: " + _serializer);
        pw.println("Messages delivered to");
        pw.println("   Peer       : " + _messagesToTunnel);
        pw.println("   Local      : " + _messagesToSystem);
        pw.println("Local domain");
        pw.println("   Name       : " + _localDomainInfo.getCellDomainName());
        pw.println("   Version    : " + _localDomainInfo.getVersion());
        pw.println("   Role       : " + _localDomainInfo.getRole());
        pw.println("   Zone       : " + _localDomainInfo.getZone().orElse("(none)"));
        pw.println("Peer domain");
        pw.println("   Name       : " + _remoteDomainInfo.getCellDomainName());
        pw.println("   Version    : " + _remoteDomainInfo.getVersion());
        pw.println("   Role       : " + _remoteDomainInfo.getRole());
        pw.println("   Zone       : " + _remoteDomainInfo.getZone().orElse("(none)"));
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
         *
         * Routes are automatically removed by the CellGlue when this
         * tunnel is killed.
         */
        public synchronized void add(LocationMgrTunnel tunnel)
                throws InterruptedException
        {
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
            if (_tunnels.remove(tunnel.getRemoteDomainName(), tunnel)) {
                notifyAll();
            }
        }
    }

    private interface ObjectSink
    {
        void writeObject(CellMessage message) throws IOException;
    }

    private static class RawObjectSink implements ObjectSink
    {
        private final SerializationHandler.Serializer serializer;
        private final DataOutputStream out;

        private RawObjectSink(OutputStream out, SerializationHandler.Serializer serializer)
        {
            this.out = new DataOutputStream(out);
            this.serializer = serializer;
        }

        @Override
        public void writeObject(CellMessage message) throws IOException
        {
            // Older versions do not support the new serialization format
            // Due to lack of message versioning support, always use JOS with different dCache versions
            message.ensureEncodedWith(serializer);

            message.writeTo(out);
            out.flush();
        }
    }

    private interface ObjectSource
    {
        CellMessage readObject() throws IOException, ClassNotFoundException;
    }

    private static class RawObjectSource implements ObjectSource
    {
        private final DataInputStream in;

        private RawObjectSource(InputStream in)
        {
            this.in = new DataInputStream(in);
        }

        @Override
        public CellMessage readObject() throws IOException, ClassNotFoundException
        {
            return CellMessage.createFrom(in);
        }
    }
}

package dmg.cells.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import dmg.cells.network.LocationManagerConnector;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.Reply;
import dmg.cells.services.login.LoginManager;
import dmg.util.CommandInterpreter;

import org.dcache.util.Args;

import static com.google.common.base.Preconditions.checkArgument;
import static dmg.cells.services.LocationManager.ServerSetup.*;
import static java.util.stream.Collectors.joining;

public class LocationManager extends CellAdapter
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(LocationManager.class);

    enum ServerSetup
    {
        SETUP_NONE("none"), SETUP_ERROR("error"), SETUP_AUTO("auto"), SETUP_WRITE("rw"), SETUP_RDONLY("rdonly");

        private final String name;

        ServerSetup(String name)
        {
            this.name = name;
        }

        public String getName()
        {
            return name;
        }

        static ServerSetup fromString(String s)
        {
            if (s == null) {
                return SETUP_AUTO;
            }
            for (ServerSetup setup : values()) {
                if (setup.getName().equals(s)) {
                    return setup;
                }
            }
            return SETUP_ERROR;
        }
    }

    private Server _server;
    private Client _client;
    private final Args _args;
    private final CellNucleus _nucleus;

    private static class NodeInfo
    {
        private final String _domainName;
        private final HashSet<String> _connections = new HashSet<>();
        private final boolean _defined;
        private String _default;
        private boolean _listen;
        private String _address;
        private int _port;
        private String _sec;

        public static NodeInfo createDefined(String domainName)
        {
            return new NodeInfo(domainName, true);
        }

        public static NodeInfo createUndefined(String domainName)
        {
            return new NodeInfo(domainName, false);
        }

        private NodeInfo(String domainName, boolean defined)
        {
            _domainName = domainName;
            _defined = defined;
        }

        private boolean isDefined()
        {
            return _defined;
        }

        private String getDomainName()
        {
            return _domainName;
        }

        private synchronized void setDefault(String defaultNode)
        {
            _default = defaultNode;
        }

        private synchronized int getConnectionCount()
        {
            return _connections.size();
        }

        private synchronized void add(String nodeName)
        {
            _connections.add(nodeName);
        }

        private synchronized void remove(String nodeName)
        {
            _connections.remove(nodeName);
        }

        private synchronized void setListenPort(int port)
        {
            _port = port;
        }

        private synchronized void setSecurity(String sec)
        {
            _sec = sec;
        }

        private synchronized void setListen(boolean listen)
        {
            _listen = listen;
        }

        private synchronized void setAddress(String address)
        {
            _listen = true;
            _address = address;
        }

        private synchronized String getAddress()
        {
            return _address;
        }

        private synchronized String getDefault()
        {
            return _default;
        }

        private synchronized Collection<String> connections()
        {
            return new ArrayList<>(_connections);
        }

        private synchronized boolean mustListen()
        {
            return _listen;
        }

        private synchronized String getSecurity()
        {
            return _sec;
        }

        public synchronized String toWhatToDoReply(boolean strict)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(_domainName).append(" ");
            if (_listen) {
                sb.append("\"l:");
                if (_port > 0) {
                    sb.append(_port);
                }
                sb.append(":");
                if (_sec != null) {
                    sb.append(_sec);
                }
                sb.append(":");
                sb.append('"');
                if (!strict && _address != null) {
                    sb.append(" (").append(_address).append(")");
                }
            } else {
                sb.append("nl");
            }
            for (String node : connections()) {
                sb.append(" c:").append(node);
            }
            if (_default != null) {
                sb.append(" d:").append(_default);
            }
            return sb.toString();
        }

        @Override
        public String toString()
        {
            return toWhatToDoReply(false);
        }
    }

    public static class LocationManagerConfig extends CommandInterpreter
    {
        private final ConcurrentMap<String, NodeInfo> _nodes = new ConcurrentHashMap<>();

        private void print(PrintWriter pw)
        {
            pw.println("#");
            pw.println("# This setup was created by the LocationManager at " + (new Date().toString()));
            pw.println("#");
            for (NodeInfo info : _nodes.values()) {
                synchronized (info) {
                    pw.println("define " + info.getDomainName());
                    if (info.mustListen()) {
                        pw.println("listen " + info.getDomainName());
                    }
                    String def = info.getDefault();
                    if (def != null) {
                        pw.println("defaultroute " + info.getDomainName() + " " + def);
                    }
                    for (String node : info.connections()) {
                        pw.println("connect " + info.getDomainName() + " " + node);
                    }
                }
            }
        }

        private NodeInfo get(String nodeName)
        {
            return _nodes.get(nodeName);
        }

        private NodeInfo createDefinedIfAbsent(String nodeName)
        {
            return _nodes.computeIfAbsent(nodeName, NodeInfo::createDefined);
        }

        private NodeInfo createUndefinedIfAbsent(String nodeName)
        {
            return _nodes.computeIfAbsent(nodeName, NodeInfo::createUndefined);
        }

        private Collection<NodeInfo> nodes()
        {
            return _nodes.values();
        }

        public static final String hh_define = "<domainName>";
        public String ac_define_$_1(Args args)
        {
            createDefinedIfAbsent(args.argv(0));
            return "";
        }

        public static final String hh_undefine = "<domainName>";
        public String ac_undefine_$_1(Args args)
        {
            String nodeName = args.argv(0);
            _nodes.remove(nodeName);
            for (NodeInfo nodeInfo : _nodes.values()) {
                nodeInfo.remove(nodeName);
            }
            return "";
        }

        public static final String hh_nodefaultroute = "<sourceDomainName>";
        public String ac_nodefaultroute_$_1(Args args)
        {
            NodeInfo info = get(args.argv(0));
            if (info != null) {
                info.setDefault(null);
            }
            return "";
        }

        public static final String hh_defaultroute = "<sourceDomainName> <destinationDomainName>";
        public String ac_defaultroute_$_2(Args args)
        {
            createDefinedIfAbsent(args.argv(1));
            createDefinedIfAbsent(args.argv(0)).setDefault(args.argv(1));
            return "";
        }

        public static final String hh_connect = "<sourceDomainName> <destinationDomainName>";
        public String ac_connect_$_2(Args args)
        {
            NodeInfo dest = createDefinedIfAbsent(args.argv(1));
            dest.setListen(true);
            createDefinedIfAbsent(args.argv(0)).add(args.argv(1));
            return "";
        }

        public static final String hh_disconnect = "<sourceDomainName> <destinationDomainName>";
        public String ac_disconnect_$_2(Args args)
        {
            NodeInfo info = get(args.argv(0));
            if (info != null) {
                info.remove(args.argv(1));
            }
            return "";
        }

        public static final String hh_listen = "<listenDomainName> [...] [-port=<portNumber>] [-security=<security>]";
        public String ac_listen_$_1_99(Args args)
        {
            int port = args.getIntOption("port", 0);
            String security = args.getOpt("security");
            for (int i = 0; i < args.argc(); i++) {
                NodeInfo info = createDefinedIfAbsent(args.argv(i));
                info.setListen(true);
                if (port > 0) {
                    info.setListenPort(port);
                }
                if (security != null && security.length() > 0 && !security.equalsIgnoreCase("none")) {
                    info.setSecurity(security);
                }
            }
            return "";
        }

        public static final String hh_unlisten = "<listenDomainName> [...]";
        public String ac_unlisten_$_1_99(Args args)
        {
            for (int i = 0; i < args.argc(); i++) {
                NodeInfo info = get(args.argv(i));
                if (info != null) {
                    info.setListen(false);
                }
            }
            return "";
        }

        public static final String hh_clear_server = "";
        public String ac_clear_server(Args args)
        {
            _nodes.clear();
            return "";
        }
    }

    public class Server implements Runnable
    {
        private final int _port;
        private final DatagramSocket _socket;
        private final Thread _worker;
        private final boolean _strict;

        /**
         * Server
         * -strict=yes|no         # 'yes' allows any client to register
         * -setup=<setupFile>     # full path of setupfile
         * -setupmode=rdonly|rw|auto   # write back the setup [def=rw]
         * -perm=<filename>       # store registry information
         */

        private ServerSetup _setupMode = SETUP_NONE;
        private String _setupFileName;
        private File _setupFile;
        private File _permFile;
        private final RemoteCommands _remoteCommands = new RemoteCommands();
        private final LocationManagerConfig _config = new LocationManagerConfig();

        private Server(int port, Args args) throws Exception
        {
            _port = port;
            addCommandListener(this);
            addCommandListener(_remoteCommands);
            addCommandListener(_config);

            String strict = args.getOpt("strict");
            _strict = strict == null || !strict.equals("off") && !strict.equals("no");

            prepareSetup(args.getOpt("setup"), args.getOpt("setupmode"));
            if ((_setupMode == SETUP_WRITE) || (_setupMode == SETUP_RDONLY)) {
                execSetupFile(_setupFile);
            }

            preparePersistentMap(args.getOpt("perm"));

            try {
                loadPersistentMap();
            } catch (Exception dd) {
            }
            _socket = new DatagramSocket(_port);
            _worker = _nucleus.newThread(this, "Server");
        }

        private void preparePersistentMap(String permFileName) throws Exception
        {
            if ((permFileName == null) || (permFileName.length() < 1)) {
                return;
            }

            File permFile = new File(permFileName);

            if (permFile.exists()) {
                if (!permFile.canWrite()) {
                    throw new IllegalArgumentException("Can't write to : " + permFileName);
                }
                _permFile = permFile;
//            loadPersistentMap() ;
            } else {
                if (!permFile.createNewFile()) {
                    throw new IllegalArgumentException("Can't create : " + permFileName);
                }
                _permFile = permFile;
            }
            LOGGER.info("Persistent map file set to : " + _permFile);
        }

        private synchronized void loadPersistentMap() throws Exception
        {
            if (_permFile == null) {
                return;
            }
            try (FileInputStream file = new FileInputStream(_permFile);
                 ObjectInputStream in = new ObjectInputStream(file)) {
                LOGGER.info("Loading persistent map file");
                try {
                    Map<String, String> hm = (HashMap<String, String>) in.readObject();

                    LOGGER.info("Persistent map : " + hm);

                    for (Map.Entry<String, String> nodeAndAddress : hm.entrySet()) {
                        String node = nodeAndAddress.getKey();
                        String address = nodeAndAddress.getValue();
                        _config.createUndefinedIfAbsent(node).setAddress(node);
                        LOGGER.info("Updated : <" + node + "> -> " + address);
                    }

                } catch (Exception ee) {
                    LOGGER.warn("Problem reading persistent map " + ee.getMessage());
                    _permFile.delete();
                }
            }

        }

        private synchronized void savePersistentMap()
        {
            if (_permFile == null) {
                return;
            }

            Map<String, String> hm = new HashMap<>();

            for (NodeInfo node : _config.nodes()) {
                synchronized (node) {
                    String address = node.getAddress();
                    if ((address != null) && node.mustListen()) {
                        hm.put(node.getDomainName(), node.getAddress());
                    }
                }
            }
            try (FileOutputStream file = new FileOutputStream(_permFile);
                 ObjectOutputStream out = new ObjectOutputStream(file )) {
                out.writeObject(hm);
            } catch (Exception e) {
                LOGGER.warn("Problem writing persistent map " + e.getMessage());
                _permFile.delete();
            }
        }

        private void prepareSetup(String setupFile, String setupMode) throws Exception
        {
            if ((_setupFileName = setupFile) == null) {
                _setupMode = SETUP_NONE;
                return;
            }

            _setupMode = ServerSetup.fromString(setupMode);

            if (_setupMode == SETUP_ERROR) {
                throw new IllegalArgumentException(
                        "Setup error, don't understand : " + setupMode);
            }

            _setupFile = new File(_setupFileName);

            boolean fileExists = _setupFile.exists();
            boolean canWrite = _setupFile.canWrite();
            boolean canRead = _setupFile.canRead();
            if (fileExists && !_setupFile.isFile()) {
                throw new IllegalArgumentException("Not a file: " + _setupFileName);
            }

            if (_setupMode == SETUP_AUTO) {
                if (fileExists) {
                    _setupMode = canWrite ? SETUP_WRITE : SETUP_RDONLY;
                } else {
                    try {
                        _setupFile.createNewFile();
                        _setupMode = SETUP_WRITE;
                    } catch (IOException e) {
                     /* This is usually a permission error.
                      */
                        LOGGER.debug("Failed to create {}: {}", _setupFile, e);
                        _setupMode = SETUP_NONE;
                    }
                }
            }

            switch (_setupMode) {
            case SETUP_WRITE:
                if (fileExists) {
                    if (!canWrite) {
                        throw new IllegalArgumentException("File not writeable: " +
                                                           _setupFileName);
                    }
                } else {
                    _setupFile.createNewFile();
                }
                break;
            case SETUP_RDONLY:
                if (!fileExists) {
                    _setupMode = SETUP_NONE;
                } else if (!canRead) {
                    throw new IllegalArgumentException("Setup file not readable: " +
                                                       _setupFileName);
                }
                break;
            }

            if (_setupMode == SETUP_NONE) {
                _setupFileName = null;
            }
        }

        private void execSetupFile(File setupFile) throws Exception
        {
            try (BufferedReader br = new BufferedReader(new FileReader(setupFile))) {
                try {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.length() < 1) {
                            continue;
                        }
                        if (line.charAt(0) == '#') {
                            continue;
                        }
                        LOGGER.info("Exec : {}", line);
                        _config.command(new Args(line));
                    }
                } catch (Exception ef) {
                    LOGGER.warn("Ups: {}", ef);
                }
            }
        }

        public void getInfo(PrintWriter pw)
        {
            pw.println("         Version : $Id: LocationManager.java,v 1.15 2007-10-22 12:30:38 behrmann Exp $");
            pw.println("      # of nodes : " + _config.nodes().size());
        }

        @Override
        public String toString()
        {
            return "Server:Nodes=" + _config.nodes().size();
        }

        @Override
        public void run()
        {
            DatagramPacket packet;
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    packet = new DatagramPacket(new byte[1024], 1024);
                    _socket.receive(packet);
                } catch (SocketException e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        LOGGER.warn("Exception in Server receive loop (exiting)", e);
                    }
                    break;
                } catch (Exception ie) {
                    LOGGER.warn("Exception in Server receive loop (exiting)", ie);
                    break;
                }
                try {
                    process(packet);
                    _socket.send(packet);
                } catch (Exception se) {
                    LOGGER.warn("Exception in send ", se);
                }
            }
            _socket.close();
        }

        public void process(DatagramPacket packet) throws Exception
        {
            byte[] data = packet.getData();
            int datalen = packet.getLength();
            InetAddress address = packet.getAddress();
            if (datalen <= 0) {
                LOGGER.warn("Empty Packet arrived from " + packet.getAddress());
                return;
            }
            String message = new String(data, 0, datalen);
            LOGGER.info("server query : [" + address + "] " + "(" + message.length() + ") " + message);
            Args args = new Args(message);
            message = args.argc() == 0 ? "" : (String) _remoteCommands.command(args);

            LOGGER.info("server reply : " + message);
            data = message.getBytes();
            packet.setData(data);
            packet.setLength(data.length);
        }

        public static final String hh_ls_perm = " # list permanent file";
        public String ac_ls_perm(Args args) throws Exception
        {
            if (_permFile == null) {
                throw new IllegalArgumentException("Permanent file not defined");
            }

            Map<String, String> hm;
            try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(_permFile))) {
                hm = (HashMap<String, String>) in.readObject();
            }

            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> node_and_address : hm.entrySet()) {
                String node = node_and_address.getKey();
                String address = node_and_address.getValue();

                sb.append(node).append(" -> ").append(address).append("\n");
            }
            return sb.toString();

        }

        public static final String hh_setup_define = "<filename> [-mode=rw|rdonly|auto]";
        public String ac_setup_define_$_1(Args args) throws Exception
        {
            String filename = args.argv(0);
            prepareSetup(filename, args.getOpt("mode"));
            return "setupfile (mode=" + _setupMode.getName() + ") : " + filename;
        }

        public static final String hh_setup_read = "";
        public String ac_setup_read(Args args) throws Exception
        {
            if (_setupFileName == null) {
                throw new IllegalArgumentException("Setupfile not defined");
            }

            try {
                execSetupFile(_setupFile);
            } catch (Exception ee) {
                throw new
                        Exception("Problem in setupFile : " + ee.getMessage());
            }
            return "";

        }

        public static final String hh_setup_write = "";
        public String ac_setup_write(Args args) throws Exception
        {
            if (_setupMode != SETUP_WRITE) {
                throw new IllegalArgumentException("Setupfile not in write mode");
            }

            File tmpFile = new File(_setupFile.getParent(), "$-" + _setupFile.getName());
            try (PrintWriter pw = new PrintWriter(new FileWriter(tmpFile))) {
                _config.print(pw);
            }
            if (!tmpFile.renameTo(_setupFile)) {
                throw new IOException("Failed to replace setupFile");
            }

            return "";

        }

        public static final String hh_ls_setup = "";
        public String ac_ls_setup(Args args)
        {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            _config.print(pw);
            pw.flush();
            sw.flush();
            return sw.getBuffer().toString();
        }

        public static final String hh_ls_node = "[<domainName>]";
        public String ac_ls_node_$_0_1(Args args)
        {
            if (args.argc() == 0) {
                return _config.nodes().stream().map(Object::toString).collect(joining("\n"));
            } else {
                NodeInfo info = _config.get(args.argv(0));
                if (info == null) {
                    throw new IllegalArgumentException("Node not found : " + args.argv(0));
                }
                return info.toString();
            }
        }

        public static final String hh_set_address = "<domainname> <address>";
        public String ac_set_address_$_2(Args args)
        {
            NodeInfo info = _config.get(args.argv(0));
            if (info == null) {
                throw new IllegalArgumentException("Domain not defined : " + args.argv(0));
            }

            if (!info.mustListen()) {
                throw new IllegalArgumentException("Domain won't listen : " + args.argv(0));
            }

            info.setAddress(args.argv(1));
            try {
                savePersistentMap();
            } catch (Exception eee) {
            }
            return info.toString();
        }

        public static final String hh_unset_address = "<domainname>";
        public String ac_unset_address_$_1(Args args)
        {
            NodeInfo info = _config.get(args.argv(0));
            if (info == null) {
                throw new IllegalArgumentException("Domain not defined : " + args.argv(0));
            }

            info.setAddress(null);
            try {
                savePersistentMap();
            } catch (Exception eee) {
            }
            return info.toString();
        }

        public void start()
        {
            _worker.start();
        }

        /**
         * Shutdown the server. Notice that the method will not wait
         * for the worker thread to shut down.
         */
        public void shutdown()
        {
            _worker.interrupt();
            _socket.close();
        }

        public class RemoteCommands extends CommandInterpreter
        {
            public static final String hh_whatToDo = "<domainName>";
            public String ac_whatToDo_$_1(Args args)
            {
                NodeInfo info = _config.get(args.argv(0));
                if (info == null) {
                    if (_strict || ((info = _config.get("*")) == null)) {
                        throw new IllegalArgumentException("Domain not defined : " + args.argv(0));
                    }

                }
                String serial = args.getOpt("serial");
                return "do" + (serial != null ? " -serial=" + serial : "") + " " + info.toWhatToDoReply(true);
            }

            public static final String hh_whereIs = "<domainName>";
            public String ac_whereIs_$_1(Args args)
            {
                NodeInfo info = _config.get(args.argv(0));
                if (info == null) {
                    throw new IllegalArgumentException("Domain not defined : " + args.argv(0));
                }
                StringBuilder sb = new StringBuilder();
                sb.append("location");
                String serial = args.getOpt("serial");
                if (serial != null) {
                    sb.append(" -serial=").append(serial);
                }
                sb.append(" ").append(info.getDomainName());
                String out = info.getAddress();
                sb.append(" ").append(out == null ? "none" : out);
                out = info.getSecurity();
                if (out != null) {
                    sb.append(" -security=\"").append(out).append("\"");
                }

                return sb.toString();
            }

            public static final String hh_listeningOn = "<domainName> <address>";
            public String ac_listeningOn_$_2(Args args)
            {
                String nodeName = args.argv(0);
                NodeInfo info = _strict ? _config.get(nodeName) : _config.createUndefinedIfAbsent(nodeName);
                if (info == null) {
                    throw new IllegalArgumentException("Domain not defined : " + nodeName);
                }
                info.setAddress(args.argv(1).equals("none") ? null : args.argv(1));
                try {
                    savePersistentMap();
                } catch (Exception eee) {
                }
                String serial = args.getOpt("serial");
                return "listenOn" + (serial != null ? (" -serial=" + serial) : "") +
                       " " + info.getDomainName() +
                       " " + (info.getAddress() == null ? "none" : info.getAddress());
            }
        }
    }

    private class LocationManagerHandler implements Runnable
    {
        private final DatagramSocket _socket;
        private final ConcurrentMap<Integer, BlockingQueue<String>> _map = new ConcurrentHashMap<>();
        private final AtomicInteger _serial = new AtomicInteger();
        private final InetAddress _address;
        private final int _port;
        private final Thread _thread;
        private final LongAdder _requestsSent = new LongAdder();
        private final LongAdder _repliesReceived = new LongAdder();

        /**
         * Create a client listening on the supplied UDP port
         *
         * @param localPort UDP port number to which the client will bind or 0
         *                  for a random port.
         * @param address   location of the server
         * @param port      port number of the server
         * @throws SocketException if a UDP socket couldn't be created
         */
        private LocationManagerHandler(int localPort, InetAddress address,
                                       int port) throws SocketException
        {
            _port = port;
            _socket = new DatagramSocket(localPort);
            _address = address;
            _thread = _nucleus.newThread(this, "LocationManagerHandler");
        }

        public void start()
        {
            _thread.start();
        }

        public long getRequestsSent()
        {
            return _requestsSent.longValue();
        }

        public long getRepliesReceived()
        {
            return _repliesReceived.longValue();
        }

        @Override
        public void run()
        {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    DatagramPacket packet = new DatagramPacket(new byte[1024], 1024);

                    _socket.receive(packet);

                    byte[] data = packet.getData();
                    int packLen = packet.getLength();

                    if ((data == null) || (packLen == 0)) {
                        LOGGER.warn("Zero packet received");
                        continue;
                    }

                    Args a = new Args(new String(data, 0, packLen));
                    String tmp = a.getOpt("serial");
                    if (tmp == null) {
                        LOGGER.warn("Packet didn't provide a serial number");
                        continue;
                    }

                    Integer s = Integer.valueOf(tmp);
                    BlockingQueue<String> b = _map.get(s);
                    if (b == null) {
                        LOGGER.warn("Not waiting for " + s);
                        continue;
                    }

                    LOGGER.info("Reasonable reply arrived (" + s + ") : " + b);

                    b.offer(a.toString());
                } catch (InterruptedIOException e) {
                    Thread.currentThread().interrupt();
                } catch (SocketException e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        LOGGER.warn("Receiver socket problem : " + e.getMessage());
                    }
                } catch (IOException e) {
                    LOGGER.warn("Receiver IO problem : " + e.getMessage());
                }
            }
            LOGGER.info("Receiver thread finished");
        }

        private String askServer(String message, long waitTime)
                throws IOException, InterruptedException
        {
            _requestsSent.increment();

            int serial = _serial.getAndIncrement();

            String request = message + " -serial=" + serial;

            LOGGER.info("Sending to {}:{}: {}", _address, _port, request);

            BlockingQueue<String> b = new ArrayBlockingQueue<>(1);
            _map.put(serial, b);
            try {
                byte[] data = request.getBytes();
                _socket.send(new DatagramPacket(data, data.length, _address, _port));
                String poll = b.poll(waitTime, TimeUnit.MILLISECONDS);
                if (poll == null) {
                    throw new IOException("Request timed out");
                }
                _repliesReceived.increment();
                return poll;
            } finally {
                _map.remove(serial);
            }
        }

        /**
         * Shutdown the client. Notice that the method will not wait
         * for the worker thread to shut down.
         */
        public void shutdown()
        {
            _thread.interrupt();
            _socket.close();
        }
    }

    public class Client implements Runnable
    {
        private Thread _whatToDo;
        private String _toDo;
        private String _registered;
        private int _state;
        private final LongAdder _requestsReceived = new LongAdder();
        private final LongAdder _repliesSent = new LongAdder();
        private final LongAdder _totalExceptions = new LongAdder();

        private final LocationManagerHandler _lmHandler;

        private Client(InetAddress address, int port, Args args)
                throws SocketException
        {
            addCommandListener(this);
            int clientPort = args.getIntOption("clientPort", 0);
            _lmHandler = new LocationManagerHandler(clientPort, address, port);
        }

        public void start()
        {
            _lmHandler.start();

            if (!_args.hasOption("noboot")) {
                _whatToDo = _nucleus.newThread(this, "WhatToDo");
                _whatToDo.start();
            }
        }

        public void getInfo(PrintWriter pw)
        {
            pw.println("            ToDo : " + (_state > -1 ? ("Still Busy (" + _state + ")") : _toDo));
            pw.println("      Registered : " + (_registered == null ? "no" : _registered));
            pw.println("RequestsReceived : " + _requestsReceived);
            pw.println("    RequestsSent : " + _lmHandler.getRequestsSent());
            pw.println(" RepliesReceived : " + _lmHandler.getRepliesReceived());
            pw.println("     RepliesSent : " + _repliesSent);
            pw.println("     Exceptions  : " + _totalExceptions);
        }

        @Override
        public String toString()
        {
            return "" + (_state > -1 ? ("Client<init>(" + _state + ")") : "ClientReady");
        }

        private class BackgroundServerRequest extends DelayedReply implements Runnable
        {
            private final String _request;

            private BackgroundServerRequest(String request)
            {
                _request = request;
            }

            @Override
            public void run()
            {
                try {
                    reply(_lmHandler.askServer(_request, 4000));
                    _repliesSent.increment();
                } catch (IOException | InterruptedException ee) {
                    LOGGER.warn("Problem in 'whereIs' request : " + ee);
                    _totalExceptions.increment();
                }
            }
        }

        public Reply ac_where_is_$_1(Args args)
        {
            _requestsReceived.increment();
            String domainName = args.argv(0);
            BackgroundServerRequest request = new BackgroundServerRequest("whereIs " + domainName);
            _nucleus.newThread(request, "where-is").start();
            return request;
        }

        //
        //
        //  create dmg.cells.services.LocationManager lm "11111"
        //
        //  create dmg.cells.network.LocationMgrTunnel connect "dCache lm"
        //
        //  create dmg.cells.services.login.LoginManager listen
        //                    "0 dmg.cells.network.LocationMgrTunnel -prot=raw -lm=lm"
        //
        public Reply ac_listening_on_$_2(Args args)
        {
            String portString = args.argv(1);

            try {
                _registered = InetAddress.getLocalHost().getCanonicalHostName() + ":" + portString;
            } catch (UnknownHostException uhe) {
                LOGGER.warn("Couldn't resolve hostname: " + uhe);
                return null;
            }
            _requestsReceived.increment();

            BackgroundServerRequest request = new BackgroundServerRequest(
                    "listeningOn " + getCellDomainName() + " " + _registered);
            _nucleus.newThread(request).start();
            return request;
        }

        private void startListener(int port, String securityContext) throws Exception
        {
            String cellName = "l*";
            String cellClass = "dmg.cells.network.LocationMgrTunnel";
            String protocol;
            if ((securityContext == null) ||
                (securityContext.length() == 0) ||
                (securityContext.equalsIgnoreCase("none"))) {
                protocol = "-prot=raw";
            } else {
                protocol = securityContext;
            }
            String cellArgs = port + " " + cellClass + " " + protocol + " -lm=" + getCellName();
            LOGGER.info(" LocationManager starting acceptor with {}", cellArgs);
            LoginManager c = new LoginManager(cellName, cellArgs);
            c.start();
            LOGGER.info("Created : {}", c);
        }

        private void startConnector(final String remoteDomain)
                throws Exception
        {
            String cellName = "c-" + remoteDomain + "*";
            String clientKey = _args.getOpt("clientKey");
            clientKey = (clientKey != null) && (clientKey.length() > 0) ? ("-clientKey=" + clientKey) : "";
            String clientName = _args.getOpt("clientUserName");
            clientName = (clientName != null) && (clientName.length() > 0) ? ("-clientUserName=" + clientName) : "";

            String cellArgs =
                    "-domain=" + remoteDomain + " "
                    + "-lm=" + getCellName() + " "
                    + clientKey + " "
                    + clientName;

            LOGGER.info("LocationManager starting connector with {}", cellArgs);
            LocationManagerConnector c = new LocationManagerConnector(cellName, cellArgs);
            c.start();
            LOGGER.info("Created : {}", c);
        }

        private void setDefaultRoute(String domain)
        {
            _nucleus.routeAdd(new CellRoute(null, "*@" + domain, CellRoute.DEFAULT));
        }

        @Override
        public void run()
        {
            if (Thread.currentThread() == _whatToDo) {
                runWhatToDo();
            }
        }

        /**
         * loop until it gets a reasonable 'what to do' list.
         */
        private void runWhatToDo()
        {
            String request = "whatToDo " + getCellDomainName();

            while (true) {
                _state++;

                try {
                    String reply = _lmHandler.askServer(request, 5000);
                    LOGGER.info("whatToDo got : " + reply);

                    Args args = new Args(reply);
                    if (args.argc() < 2) {
                        throw new IllegalArgumentException("No enough arg. : " + reply);
                    }

                    if ((!args.argv(0).equals("do")) ||
                        (!(args.argv(1).equals(getCellDomainName()) ||
                           args.argv(1).equals("*")))) {
                        throw new IllegalArgumentException("Not a 'do' or not for us : " + reply);
                    }

                    if (args.argc() == 2) {
                        LOGGER.info("Nothing to do for us");
                        return;
                    }

                    executeToDoList(args);

                    _toDo = reply;
                    _state = -1;

                    LOGGER.info("whatToDo finished");
                    return;
                } catch (InterruptedException ie) {
                    LOGGER.warn(_toDo = "whatToDo : interrupted");
                    break;
                } catch (InterruptedIOException ie) {
                    LOGGER.warn(_toDo = "whatToDo : interrupted(io)");
                    break;
                } catch (Exception ee) {
                    LOGGER.warn(_toDo = "whatToDo : exception : " + ee);
                }
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException iie) {
                    LOGGER.warn(_toDo = "whatToDo : interrupted sleep");
                    break;
                }
            }
        }

        /**
         * Gets the reply from the 'server' and can
         * i) create a connector
         * ii) listens to a given port
         * iii) sets a default route
         * <p>
         * or all of it.
         */
        private void executeToDoList(Args args) throws Exception
        {
            for (int i = 2; i < args.argc(); i++) {
                String arg = args.argv(i);

                try {
                    //
                    // expected formats
                    //   l:[<portNumber>]:[<securityContext>]
                    //   c:<DomainName>
                    //   d:<DomainName>
                    //
                    if (arg.startsWith("l")) {
                        int port = 0;
                        StringTokenizer st = new StringTokenizer(arg, ":");
                        //
                        // get rid of the 'l'
                        //
                        st.nextToken();
                        //
                        // get the port if availble
                        //
                        if (st.hasMoreTokens()) {
                            String tmp = st.nextToken();
                            if (tmp.length() > 0) {
                                try {
                                    port = Integer.parseInt(tmp);
                                } catch (Exception e) {
                                    LOGGER.warn("Got illegal port numnber <" + arg + ">, using random");
                                }
                            }
                        }
                        //
                        // get the security context
                        //
                        String securityContext = null;
                        if (st.hasMoreTokens()) {
                            securityContext = st.nextToken();
                        }

                        startListener(port, securityContext);
                    } else if ((arg.length() > 2) && arg.startsWith("c:")) {

                        startConnector(arg.substring(2));
                    } else if ((arg.length() > 2) && arg.startsWith("d:")) {
                        setDefaultRoute(arg.substring(2));
                    }
                } catch (InterruptedIOException | InterruptedException ioee) {
                    throw ioee;
                } catch (Exception ee) {
                    LOGGER.warn("Command >" + arg + "< received : " + ee);
                }
            }

        }

        /**
         * Shutdown the client. Notice that the method will not wait
         * for the worker thread to shut down.
         */
        public void shutdown()
        {
            _lmHandler.shutdown();
        }
    }

    /**
     * Usage : ... [<host>] <port> -noclient [-clientPort=<UDP port number> | random]
     * Server Options : -strict=[yes|no] -perm=<helpFilename> -setup=<setupFile>
     */
    public LocationManager(String name, String args)
    {
        super(name, "System", args);
        _args = getArgs();
        _nucleus = getNucleus();
    }

    @Override
    protected void startUp() throws Exception
    {
        int port;
        InetAddress host;
        checkArgument(_args.argc() >= 1, "Usage : ... [<host>] <port> [-noclient] [-clientPort=<UDP port number>]");

        if (_args.argc() == 1) {
            //
            // we are a server and a client
            //
            port = Integer.parseInt(_args.argv(0));
            host = InetAddress.getLoopbackAddress();
            _server = new Server(port, _args);
            LOGGER.info("Server Setup Done");
        } else {
            port = Integer.parseInt(_args.argv(1));
            host = InetAddress.getByName(_args.argv(0));
        }
        if (!_args.hasOption("noclient")) {
            _client = new Client(host, port, _args);
            LOGGER.info("Client started");
        }
    }

    @Override
    protected void started()
    {
        if (_server != null) {
            _server.start();
        }

        if (_client != null) {
            _client.start();
        }
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        if (_client != null) {
            pw.println("Client\n--------");
            _client.getInfo(pw);
        }
        if (_server != null) {
            pw.println("Server\n--------");
            _server.getInfo(pw);
        }
    }

    @Override
    public void cleanUp()
    {
        if (_server != null) {
            _server.shutdown();
        }
        if (_client != null) {
            _client.shutdown();
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        if (_client != null) {
            sb.append(_client.toString()).
                    append(_server != null ? ";" : "");
        }
        if (_server != null) {
            sb.append(_server.toString());
        }
        return sb.toString();
    }

    static class XXClient
    {
        XXClient(InetAddress address, int port, String message) throws Exception
        {
            byte[] data = message.getBytes();
            DatagramPacket packet =
                    new DatagramPacket(data, data.length, address, port);

            DatagramSocket socket = new DatagramSocket();

            socket.send(packet);
            packet = new DatagramPacket(new byte[1024], 1024);
            socket.receive(packet);
            data = packet.getData();
            System.out.println(new String(data, 0, data.length));
        }
    }

    public static void main(String[] args) throws Exception
    {
        if (args.length < 3) {
            throw new
                    IllegalArgumentException("Usage : ... <host> <port> <message>");
        }
        InetAddress address = InetAddress.getByName(args[0]);
        int port = Integer.parseInt(args[1]);
        String message = args[2];

        new XXClient(address, port, message);
        System.exit(0);
    }
}

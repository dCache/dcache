package dmg.cells.services.login;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import javatunnel.UserValidatable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellEvent;
import dmg.cells.nucleus.CellEventListener;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.KeepAliveListener;

import org.dcache.util.Args;
import org.dcache.util.NDC;
import org.dcache.util.Subnet;
import org.dcache.util.Version;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.net.InetAddresses.toUriString;
import static org.dcache.util.ByteUnit.KiB;

/**
 * *
 *
 * @author Patrick Fuhrmann
 * @version 0.1, 15 Feb 1998
 */
public class LoginManager
        extends CellAdapter
        implements UserValidatable
{
    private static final Object DEAD_CELL = new Object();

    private static final Logger LOGGER = LoggerFactory.getLogger(LoginManager.class);

    private final CellNucleus _nucleus;
    private final Args _args;
    private ListenThread _listenThread;
    private final AtomicInteger _connectionDeniedCounter = new AtomicInteger();
    private final AtomicInteger _loginCounter = new AtomicInteger();
    private final AtomicInteger _loginFailures = new AtomicInteger();
    private CellVersion _version;
    private ScheduledExecutorService _scheduledExecutor;
    private ConcurrentMap<String, Object> _children = new ConcurrentHashMap<>();
    private CellPath _authenticator;
    private KeepAliveTask _keepAlive;
    private LoginBrokerPublisher _loginBrokerPublisher;
    private LoginCellFactory _loginCellFactory;

    private volatile boolean _sending = true;
    private volatile int _maxLogin = -1;

    private final Set<Subnet> _allowed;

    /**
     * Tagging interface that a CellMessage payload implements to indicate
     * the notification should be forwarded to all children.
     */
    public interface OfInterestToChildren
    {
    }

    /**
     * <pre>
     *   usage   &lt;listenPort&gt; &lt;loginCellFactoryClass&gt;
     *
     *         all residual arguments and all options are sent to
     *         the &lt;loginCellClass&gt; :
     *            &lt;init&gt;(String name , StreamEngine engine , Args args )
     *
     *         and to the Authentication module (class)
     *
     *            &lt;init&gt;(CellNucleus nucleus , Args args )
     *
     *         Both get their own copy.
     * </pre>
     */
    public LoginManager(String name, String argString)
    {
        this(name, "Generic", argString);
    }

    public LoginManager(String name, String cellType, String argString)
    {
        super(name, cellType, argString);
        _nucleus = getNucleus();
        _args = getArgs();

        //allowed subnets
        _allowed = allowedSubnets(_args.getOption("netmask", ""));
    }

    @Override
    protected void starting() throws Exception
    {
        if (_args.argc() < 2) {
            throw new
                    IllegalArgumentException(
                    "USAGE : ... <listenPort> <loginCell>" +
                    " [-maxLogin=<n>|-1]" +
                    " [-keepAlive=<seconds>]" +
                    " [-acceptErrorWait=<msecs>]" +
                    " [args givenToLoginClass]");
        }

        int listenPort = Integer.parseInt(_args.argv(0));
        String loginCell = _args.argv(1);

        Args childArgs = new Args(_args.toString()
                                          .replaceFirst("(^|\\s)-consume=\\S*", "")
                                          .replaceFirst("(^|\\s)-subscribe=\\S*", ""));
        childArgs.shift();
        childArgs.shift();

        // get the authentication
        _authenticator = new CellPath(_args.getOption("authenticator", "pam"));

        String maxLogin = _args.getOpt("maxLogin");
        if (maxLogin != null) {
            try {
                _maxLogin = Integer.parseInt(maxLogin);
            } catch (NumberFormatException ee) {/* bad values ignored */}
        }

        if (_maxLogin < 0) {
            LOGGER.info("Maximum login feature disabled");
        } else {
            LOGGER.info("Maximum logins set to: {}", _maxLogin);
        }

        _scheduledExecutor = Executors.newSingleThreadScheduledExecutor(_nucleus);

        // keep alive
        long keepAlive = TimeUnit.SECONDS.toMillis(_args.getLongOption("keepAlive", 0L));
        LOGGER.info("Keep alive set to {} ms", keepAlive);
        _keepAlive = new KeepAliveTask();
        _keepAlive.schedule(keepAlive);


        _loginCellFactory = new LoginCellFactoryBuilder()
                .setName(loginCell)
                .setCellEndpoint(this)
                .setLoginManagerName(getCellName())
                .setArgs(childArgs)
                .build();
        _version = new CellVersion(Version.of(_loginCellFactory));

        String topic = _args.getOpt("brokerTopic");
        if (topic != null) {
            Splitter byComma = Splitter.on(",").omitEmptyStrings();
            Splitter byColon = Splitter.on(":").omitEmptyStrings();
            _loginBrokerPublisher = new LoginBrokerPublisher();
            _loginBrokerPublisher.setExecutor(_scheduledExecutor);
            _loginBrokerPublisher.setTopic(topic);
            _loginBrokerPublisher.setCellEndpoint(this);
            _loginBrokerPublisher.setCellAddress(_nucleus.getThisAddress());
            _loginBrokerPublisher.setTags(byComma.splitToList(_args.getOption("brokerTags")));
            _loginBrokerPublisher.setProtocolEngine(_loginCellFactory.getName());
            _loginBrokerPublisher.setProtocolFamily(_args.getOption("protocolFamily", ""));
            _loginBrokerPublisher.setProtocolVersion(_args.getOption("protocolVersion", "1.0"));
            _loginBrokerPublisher.setUpdateTime(_args.getLongOption("brokerUpdateTime"));
            _loginBrokerPublisher.setUpdateTimeUnit(TimeUnit.valueOf(_args.getOption("brokerUpdateTimeUnit")));
            _loginBrokerPublisher.setUpdateThreshold(_args.getDoubleOption("brokerUpdateOffset"));
            _loginBrokerPublisher.setRoot(Strings.emptyToNull(_args.getOption("brokerRoot", _args.getOption("root"))));
            _loginBrokerPublisher.setReadPaths(byColon.splitToList(_args.getOption("brokerReadPaths", "/")));
            _loginBrokerPublisher.setWritePaths(byColon.splitToList(_args.getOption("brokerWritePaths", "/")));
            _loginBrokerPublisher.setAddress(Strings.emptyToNull(_args.getOption("brokerAddress")));
            _loginBrokerPublisher.setPort(_args.getIntOption("brokerPort", 0));

            addCommandListener(_loginBrokerPublisher);
            addCellEventListener(_loginBrokerPublisher);

            if (_maxLogin < 0) {
                _maxLogin = 100000;
            }
        } else {
            _loginBrokerPublisher = null;
        }

        addCellEventListener(new LoginEventListener());

        _listenThread = new ListenThread(listenPort);
    }

    @Override
    protected void started()
    {
        _nucleus.newThread(_listenThread, getCellName() + "-listen").start();
        if (_loginBrokerPublisher != null) {
            _loginBrokerPublisher.afterStart();
        }
    }

    @Override
    public void messageArrived(CellMessage envelope)
    {
        Serializable message = envelope.getMessageObject();
        if (_loginBrokerPublisher != null) {
            if (message instanceof NoRouteToCellException) {
                _loginBrokerPublisher.messageArrived((NoRouteToCellException) message);
            } else if (message instanceof LoginBrokerInfoRequest) {
                _loginBrokerPublisher.messageArrived((LoginBrokerInfoRequest) message);
            }
        }

        // Try delivering message to all children.
        if (message instanceof OfInterestToChildren) {
            for (String child : _children.keySet()) {
                CellMessage msg = envelope.clone();
                msg.getDestinationPath().add(child);
                sendMessage(msg);
            }
        }
    }

    @Override
    public CellVersion getCellVersion()
    {
        return _version;
    }

    public int getListenPort()
    {
        return _listenThread.getListenPort();
    }

    public static final String hh_get_children = "[-binary]";
    public Object ac_get_children(Args args)
    {
        boolean binary = args.hasOption("binary");
        if (binary) {
            /* Important: Do not try to allocate a sized array as _children may be
             * updated in between creating the array and copying the keys.
             */
            String[] list = _children.keySet().toArray(new String[0]);
            return new LoginManagerChildrenInfo(getCellName(), getCellDomainName(), list);
        } else {
            StringBuilder sb = new StringBuilder();
            for (String child : _children.keySet()) {
                sb.append(child).append('\n');
            }
            return sb.toString();
        }
    }

    private class LoginEventListener implements CellEventListener
    {
        @Override
        public void cellDied(CellEvent ce)
        {
            String removedCell = ce.getSource().toString();
            if (!removedCell.startsWith(getCellName())) {
                return;
            }

            /*  while in some cases remove may be issued prior cell is inserted into _children
       	     *  following trick is used:
       	     *  if there is no mapping for this cell, we create a 'dead' mapping, which will
       	     *  allow following put to identify it as a 'dead' and remove it.
       	     */
            Object cell = _children.putIfAbsent(removedCell, DEAD_CELL);
            if (cell != null) {
                _children.remove(removedCell, cell);
            }
            LOGGER.info("LoginEventListener : removing : {}", removedCell);
            loadChanged();
        }
    }

    private class KeepAliveTask implements Runnable
    {
        private ScheduledFuture<?> _future;
        private long _keepAlive;

        @Override
        public void run()
        {
            try {
                for (Object o : _children.values()) {
                    if (o instanceof KeepAliveListener) {
                        try {
                            ((KeepAliveListener) o).keepAlive();
                        } catch (Throwable t) {
                            LOGGER.warn("Problem reported by : {} : {}", o, t);
                        }
                    }
                }
            } catch (Throwable t) {
                LOGGER.warn("runKeepAlive reported : {}", t.toString());
            }
        }

        public synchronized void schedule(long keepAlive)
        {
            _keepAlive = keepAlive;
            if (_future != null) {
                _future.cancel(false);
            }
            if (_keepAlive > 0) {
                _future = _scheduledExecutor.scheduleWithFixedDelay(this, _keepAlive, _keepAlive,
                                                                    TimeUnit.MILLISECONDS);
            } else {
                _future = null;
            }
            LOGGER.info("Keep Alive value changed to {}", _keepAlive);
        }

        public synchronized long getKeepAlive()
        {
            return _keepAlive;
        }
    }

    public static final String hh_set_keepalive = "<keepAliveValue/seconds>";
    public String ac_set_keepalive_$_1(Args args)
    {
        long keepAlive = Long.parseLong(args.argv(0));
        _keepAlive.schedule(keepAlive * 1000L);
        return "keepAlive value set to " + keepAlive + " seconds";
    }

    // the cell implementation
    @Override
    public String toString()
    {
        ListenThread listenThread = _listenThread;
        LoginCellFactory loginCellFactory = _loginCellFactory;
        return "p=" + (listenThread == null ? "" : String.valueOf(listenThread.getListenPort())) +
               ";c=" + (loginCellFactory == null ? "" : loginCellFactory.getName());
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("--- Login Manager ---");
        pw.println("  Listen Port    : " + _listenThread.getListenPort());
        pw.println("  Protocol engine: " + _loginCellFactory.getName());
        pw.println("  NioChannel     : " + (_listenThread._serverSocket.getChannel() != null));
        pw.println("  Logins created : " + _loginCounter);
        pw.println("  Logins failed  : " + _loginFailures);
        pw.println("  Logins denied  : " + _connectionDeniedCounter);
        pw.println("  KeepAlive      : " + (_keepAlive.getKeepAlive() / 1000L));

        if (_maxLogin > -1) {
            pw.println("  Logins/max     : " + _children.size() + '/' + _maxLogin);
        }

        pw.println();
        pw.println("--- Login cell factory ---");
        _loginCellFactory.getInfo(pw);

        if (_loginBrokerPublisher != null) {
            pw.println();
            pw.println("--- Login broker info ---");
            _loginBrokerPublisher.getInfo(pw);
        }

    }

    public static final String hh_set_max_logins = "<maxNumberOfLogins>|-1";
    public String ac_set_max_logins_$_1(Args args)
    {
        int n = Integer.parseInt(args.argv(0));
        checkArgument(n == -1 || _maxLogin >= 0, "Can't switch off maxLogin feature");
        checkArgument(n >= 0 || _maxLogin == -1, "Can't switch on maxLogin feature");
        _maxLogin = n;
        loadChanged();
        return "";
    }

    @Override
    protected void stopping()
    {
        LOGGER.info("cleanUp requested by nucleus, closing listen socket");
        if (_loginBrokerPublisher != null) {
            _loginBrokerPublisher.beforeStop();
        }
        /* Shut down the listen thread in stopping to shut down login cell factories
         * before cell nucleus cancels pending messages with a timeout. Otherwise
         * some components used by the factories - such as the pool manager handler
         * subscriber - enter an eager retry loop.
         */
        if (_listenThread != null) {
            _listenThread.shutdown();
        }
    }

    @Override
    public void stopped()
    {
        if (_scheduledExecutor != null) {
            _scheduledExecutor.shutdown();
        }
        LOGGER.info("Bye Bye");
    }

    private class ListenThread implements Runnable
    {
        private static final int SHUTDOWN_TIMEOUT = 60000;

        private final InetSocketAddress _socketAddress;
        private final Constructor<?> _ssfConstructor;
        private final String _factoryArgs;
        private final long _acceptErrorTimeout;

        private volatile boolean _shutdown;
        private ServerSocket _serverSocket;

        private ListenThread(int listenPort) throws Exception
        {
            long timeout;
            try {
                timeout = Long.parseLong(_args.getOpt("acceptErrorWait"));
            } catch (NumberFormatException e) {
                /* bad values ignored */
                timeout = 0;
            }
            _acceptErrorTimeout = timeout;

            String listen = _args.getOpt("listen");
            if (Strings.isNullOrEmpty(listen)) {
                _socketAddress = new InetSocketAddress(listenPort);
            } else {
                _socketAddress = new InetSocketAddress(InetAddress.getByName(listen), listenPort);
            }

            String ssf = _args.getOpt("socketfactory");
            if (ssf != null) {
                Args args = new Args(ssf);
                checkArgument(args.argc() >= 1 , "Invalid Arguments for 'socketfactory'");
                String tunnelFactoryClass = args.argv(0);

                /*
                 * the rest is passed to factory constructor
                 */
                args.shift();
                _factoryArgs = args.toString();

                Class<?> ssfClass = Class.forName(tunnelFactoryClass);
                Constructor<?> constructor;
                try {
                    constructor = ssfClass.getConstructor(String.class, Map.class);
                } catch (Exception ee) {
                    constructor = ssfClass.getConstructor(String.class);
                }
                _ssfConstructor = constructor;
            } else {
                _ssfConstructor = null;
                _factoryArgs = null;
            }

            openPort();
        }

        private void openPort() throws Exception
        {
            if (_ssfConstructor == null) {
                _serverSocket = ServerSocketChannel.open().socket();
            } else {
                Object obj;
                try {
                    if (_ssfConstructor.getParameterTypes().length == 2) {
                        Map<String, Object> map = newHashMap(getDomainContext());
                        map.put("UserValidatable", LoginManager.this);
                        obj = _ssfConstructor.newInstance(_factoryArgs, map);
                    } else {
                        obj = _ssfConstructor.newInstance(_factoryArgs);
                    }
                } catch (InvocationTargetException e) {
                    Throwables.propagateIfPossible(e.getCause(), Exception.class);
                    throw new RuntimeException(e);
                }

                try {
                    Method meth = obj.getClass().getMethod("createServerSocket");
                    _serverSocket = (ServerSocket) meth.invoke(obj);
                } catch (NoSuchMethodException | SecurityException e) {
                    LOGGER.info("Method createServerSocket not found {}", e.getCause());
                    Throwables.propagateIfPossible(e.getCause(), Exception.class);
                    throw new RuntimeException(e);
                }
                LOGGER.info("ListenThread : got serverSocket class : {}", _serverSocket.getClass().getName());
            }
            _serverSocket.bind(_socketAddress);

            if (_loginBrokerPublisher != null) {
                /* Synchronize to make update atomic. */
                synchronized (_loginBrokerPublisher) {
                    _loginBrokerPublisher.setSocketAddress((InetSocketAddress) _serverSocket.getLocalSocketAddress());
                    String address = Strings.emptyToNull(_args.getOption("brokerAddress"));
                    if (address != null) {
                        _loginBrokerPublisher.setAddress(address);
                    }
                    int port = _args.getIntOption("brokerPort", 0);
                    if (port != 0) {
                        _loginBrokerPublisher.setPort(port);
                    }
                }
            }

            LOGGER.info("Listening on {}", _serverSocket.getLocalSocketAddress());
            LOGGER.trace("Nio Socket Channel : {}", (_serverSocket.getChannel() != null));
        }

        public int getListenPort()
        {
            return _serverSocket.getLocalPort();
        }

        @Override
        public void run()
        {
            ExecutorService executor = Executors.newCachedThreadPool(_nucleus);
            try {
                _loginCellFactory.startAsync().awaitRunning();
                while (!_serverSocket.isClosed()) {
                    try {
                        Socket socket = _serverSocket.accept();
                        InetSocketAddress remoteAddress = (InetSocketAddress)socket.getRemoteSocketAddress();


                        if (!remoteAddress.getAddress().isAnyLocalAddress() &&
                            !remoteAddress.getAddress().isLoopbackAddress() &&
                            !_allowed.isEmpty() &&
                            _allowed.stream()
                                    .noneMatch(s -> s.contains(remoteAddress.getAddress()))) {
                            throw new IOException("Remote Host ("
                                                    + remoteAddress.getAddress()
                                                    + ") not in the list of allowed subnets");
                        }

                        socket.setKeepAlive(true);
                        socket.setTcpNoDelay(true);
                        LOGGER.debug("Socket OPEN (ACCEPT) remote = {} local = {}",
                                socket.getRemoteSocketAddress(), socket.getLocalSocketAddress());
                        LOGGER.info("Nio Channel (accept) : {}", (socket.getChannel() != null));

                        int currentChildCount = _children.size();
                        LOGGER.info("New connection : {}", currentChildCount);
                        if ((_maxLogin > -1) && (currentChildCount >= _maxLogin)) {
                            _connectionDeniedCounter.incrementAndGet();
                            LOGGER.warn("Connection denied: Number of allowed logins exceeded ({} > {}).", currentChildCount, _maxLogin);
                            executor.execute(new ShutdownEngine(socket));
                        } else {
                            LOGGER.info("Connection request from {}", socket.getInetAddress());
                            executor.execute(new RunEngineThread(socket));
                        }
                    } catch (InterruptedIOException ioe) {
                        LOGGER.debug("Listen thread interrupted");
                        try {
                            _serverSocket.close();
                        } catch (IOException ignored) {
                        }
                    } catch (IOException ioe) {
                        if (!_serverSocket.isClosed()) {
                            LOGGER.error("I/O error: {}", ioe.toString());
                            try {
                                _serverSocket.close();
                            } catch (IOException ignored) {
                            }
                            if (_acceptErrorTimeout > 0L) {
                                synchronized (this) {
                                    while (!_shutdown && _serverSocket.isClosed()) {
                                        LOGGER.warn("Sleeping {} ms before reopening server socket", _acceptErrorTimeout);
                                        wait(_acceptErrorTimeout);
                                        if (!_shutdown) {
                                            try {
                                                openPort();
                                                LOGGER.warn("Resuming operation");
                                            } catch (Exception ee) {
                                                LOGGER.warn("Failed to open socket: {}", ee.toString());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (InterruptedException ignored) {
            } finally {
                // Initiate shutdown of children as early as possible
                terminateChildren();
                shutdownAndAwaitTermination(executor);
                // At this point we know that no new children will be added
                terminateChildren();
                awaitTerminationOfChildren();
                // Now that children should be terminated, we release any shared
                // resources managed by the factory.
                _loginCellFactory.stopAsync();
                LOGGER.trace("Listen thread finished");
            }
        }

        private void shutdownAndAwaitTermination(ExecutorService executor)
        {
            executor.shutdown();
            try {
                executor.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void awaitTerminationOfChildren()
        {
            try {
                for (Object child : _children.values()) {
                    if (child instanceof CellAdapter) {
                        getNucleus().join(((CellAdapter) child).getCellName());
                    }
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        private void terminateChildren()
        {
            for (Object child : _children.values()) {
                if (child instanceof CellAdapter) {
                    getNucleus().kill(((CellAdapter) child).getCellName());
                }
            }
        }

        public void shutdown()
        {
            LOGGER.info("Listen thread shutdown requested");

            synchronized (this) {
                //
                // it is still hard to stop an Pending I/O call.
                //
                if (_shutdown || (_serverSocket == null)) {
                    return;
                }
                _shutdown = true;

                try {
                    LOGGER.debug("Socket SHUTDOWN local = {}", _serverSocket.getLocalSocketAddress());
                    _serverSocket.close();
                } catch (IOException ee) {
                    LOGGER.warn("ServerSocket close: {}", ee.toString());
                }

                notifyAll();
            }

            _loginCellFactory.awaitTerminated();

            LOGGER.info("Shutdown sequence done");
        }
    }

    /**
     * Class that closes the output half of a TCP socket, drains any pending input and closes the input once drained.
     * After creation, the {@link #start} method must be called.  The activity occurs on a separate thread, allowing
     * the start method to be non-blocking.
     */
    public static class ShutdownEngine implements Runnable
    {
        private final Socket _socket;

        public ShutdownEngine(Socket socket)
        {
            _socket = socket;
        }

        @Override
        public void run()
        {
            InputStream inputStream;
            OutputStream outputStream;
            try {
                inputStream = _socket.getInputStream();
                outputStream = _socket.getOutputStream();
                outputStream.close();
                byte[] buffer = new byte[KiB.toBytes(1)];
                    /*
                     * eat the outstanding date from socket and close it
                     */
                while (inputStream.read(buffer, 0, buffer.length) > 0) {
                }
                inputStream.close();
            } catch (IOException ee) {
                LOGGER.warn("Shutdown : {}", ee.getMessage());
            } finally {
                try {
                    LOGGER.debug("Socket CLOSE (ACCEPT) remote = {} local = {}",
                            _socket.getRemoteSocketAddress(), _socket.getLocalSocketAddress());
                    _socket.close();
                } catch (IOException e) {
                    // ignore
                }
            }

            LOGGER.info("Shutdown : done");
        }
    }

    private class RunEngineThread implements Runnable
    {
        private Socket _socket;

        private RunEngineThread(Socket socket)
        {
            _socket = socket;
        }

        @Override
        public void run()
        {
            Thread t = Thread.currentThread();
            InetSocketAddress remoteSocketAddress = (InetSocketAddress) _socket.getRemoteSocketAddress();
            NDC.push(toUriString(remoteSocketAddress.getAddress()) + ':' + remoteSocketAddress.getPort());
            try {
                LOGGER.info("acceptThread ({}): creating protocol engine", t);

                Object cell = _loginCellFactory.newCell(_socket);
                try {
                    Method m = cell.getClass().getMethod("getCellName");
                    String cellName = (String) m.invoke(cell);
                    LOGGER.info("Invoked cell name : {}", cellName);
                    if (_children.putIfAbsent(cellName, cell) == DEAD_CELL) {
                        /*  while cell may be already gone do following trick:
                         *  if put return an old cell, then it's a dead cell and we
                         *  have to remove it. Dead cell is inserted by cleanup procedure:
                         *  if a remove for non existing cells issued, then cells is dead, and
                         *  we put it into _children.
                         */
                        _children.remove(cellName, DEAD_CELL);
                    }
                    loadChanged();
                } catch (IllegalAccessException | IllegalArgumentException | NoSuchMethodException | SecurityException | InvocationTargetException ee) {
                    LOGGER.warn("Can't determine child name", ee);
                }
                _loginCounter.incrementAndGet();

            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof Error) {
                    throw (Error) cause;
                }
                if (cause instanceof RuntimeException) {
                    LOGGER.warn("Bug detected in dCache; please report this to <support@dcache.org>", cause);
                } else {
                    LOGGER.warn("Exception (ITE) in secure protocol: {}", cause.getMessage());
                }
                try {
                    _socket.close();
                } catch (IOException ee) {/* dead any way....*/}
                _loginFailures.incrementAndGet();
            } catch (Exception e) {
                LOGGER.warn("Exception in secure protocol : {}", e.toString());
                try {
                    _socket.close();
                } catch (IOException ee) {/* dead any way....*/}
                _loginFailures.incrementAndGet();
            } finally {
                NDC.pop();
            }
        }
    }

    private void loadChanged()
    {
        int children = _children.size();
        LOGGER.info("New child count : {}", children);
        if (_loginBrokerPublisher != null) {
            _loginBrokerPublisher.setLoad(children, _maxLogin);
        }
    }

    @Override
    public boolean validateUser(String userName, String password)
    {
        String[] request = { "request", userName, "check-password", userName, password };

        try {
            CellMessage msg = new CellMessage(_authenticator, request);
            msg = getNucleus().sendAndWait(msg, (long) 10000);
            if (msg == null) {
                LOGGER.warn("Pam request timed out {}", Thread.currentThread().getStackTrace());
                return false;
            }

            Object[] r = (Object[]) msg.getMessageObject();

            return (Boolean) r[5];

        } catch (NoRouteToCellException e) {
            LOGGER.warn(e.getMessage());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            LOGGER.warn(e.getCause().getMessage());
            return false;
        }
    }

    public static Set<Subnet> allowedSubnets(String netmask)
    {
        return StreamSupport.stream(Splitter.on(CharMatcher.whitespace())
                                            .trimResults()
                                            .omitEmptyStrings()
                                            .split(netmask)
                                            .spliterator(),
                false)
                            .filter(s -> !s.isEmpty())
                            .map(LoginManager::validateCreateSubnet)
                            .collect(Collectors.toSet());
    }

    public static Subnet validateCreateSubnet(String subnet)
    {
        try {
            return Subnet.create(subnet);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException(
                    String.format("Invalid IP/subnet '%s': %s\n.", subnet, iae.getMessage()));
        }
    }
}

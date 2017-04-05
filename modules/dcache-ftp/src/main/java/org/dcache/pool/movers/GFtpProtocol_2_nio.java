package org.dcache.pool.movers;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.ProtocolFamily;
import java.net.UnknownHostException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.file.FileSystems;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.vehicles.GFtpProtocolInfo;
import diskCacheV111.vehicles.GFtpTransferStartedMessage;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellArgsAware;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.util.Exceptions;

import org.dcache.ftp.data.BlockLog;
import org.dcache.ftp.data.ConnectionMonitor;
import org.dcache.ftp.data.DigestThread;
import org.dcache.ftp.data.DirectDigestThread;
import org.dcache.ftp.data.FTPException;
import org.dcache.ftp.data.Mode;
import org.dcache.ftp.data.ModeE;
import org.dcache.ftp.data.ModeS;
import org.dcache.ftp.data.ModeX;
import org.dcache.ftp.data.Multiplexer;
import org.dcache.ftp.data.Role;
import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.FileRepositoryChannel;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Args;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.NetworkUtils;
import org.dcache.util.PortRange;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.dcache.util.ByteUnit.*;

/**
 * FTP mover. Supports both mover protocols GFtp/1 and GFtp/2.
 */
public class GFtpProtocol_2_nio implements ConnectionMonitor,
        MoverProtocol, ChecksumMover, CellArgsAware
{
    private static final Logger _log =
            LoggerFactory.getLogger(GFtpProtocol_2_nio.class);
    private static final Logger _logSpaceAllocation =
            LoggerFactory.getLogger("logger.dev.org.dcache.poolspacemonitor." +
                                            GFtpProtocol_2_nio.class.getName());

    /** The minimum number of bytes to increment the space allocation. */
    public static final long SPACE_INC = MiB.toBytes(50);

    /** Key used to extract the read ahead from the domain context. */
    public static final String READ_AHEAD_KEY = "gsiftpReadAhead";

    /**
     * Default block size for mode S. Although mode S is not a block
     * protocol, the block size parameter defines the largest amount
     * of data we will try to transfer in a single iteration of the
     * transfer loop.
     */
    public static final int MODE_S_DEFAULT_BLOCK_SIZE = KiB.toBytes(512);

    /**
     * Default block size for mode E.
     */
    public static final int MODE_E_DEFAULT_BLOCK_SIZE = KiB.toBytes(128);

    /**
     * Default block size for mode X.
     */
    public static final int MODE_X_DEFAULT_BLOCK_SIZE = KiB.toBytes(128);

    /** The cell owning this mover. Log messages are sent to it. */
    protected final CellEndpoint  _cell;

    /** A channel to the file we read from or write to. */
    protected RepositoryChannel  _fileChannel;

    /**
     * A BlockLog keeping tracks of which parts of a file we have
     * received.
     */
    protected BlockLog     _blockLog;

    /**
     * If a checksum is requested, this points to the checksum factory
     * to use.
     */
    protected Set<ChecksumFactory> _checksumFactories;

    /**
     * If a checksum is requested, this points to the algorithm used.
     */
    protected Map<ChecksumType,MessageDigest> _digests = Collections.emptyMap();

    /**
     * The role of this transfer in the transaction. Either Sender or
     * Receiver.
     */
    protected Role         _role;

    /**
     * The number of bytes that have been transferred.
     */
    protected long         _bytesTransferred;

    /**
     * The number of bytes reserved in the space allocator.
     */
    protected long         _reservedSpace;

    /**
     * The number of bytes of the reservation actually used. This is
     * less than or equal to _reservedSpace and bigger than or equal
     * to _bytesTransferred. It may differ from _bytesTransferred
     * because data can be received out of order.
     */
    protected long         _spaceUsed;

    /**
     * The time stamp according to System.currentTimeMillis() for when
     * the last transfer was started.
     */
    protected long         _transferStarted;

    /**
     * The time stamp according to System.currentTimeMillis() for when
     * the last piece of data was transferred.
     */
    protected long         _lastTransferred;

    /**
     * The space allocator is used to preallocate space when receiving
     * data.
     */
    protected Allocator _allocator;

    /**
     * All communication is asynchronous.
     */
    protected Multiplexer  _multiplexer;

    /**
     * Status of space reservation request.
     */
    protected String       _status;

    /**
     * Port range for passive transfers.
     */
    protected final PortRange _portRange;

    /**
     * The chunk size used when transferring files.
     *
     * Large blocks will reduce the overhead. However, it case of
     * multible concurrent streams, large blocks will make disk access
     * less sequential on both the sending and receiving side.
     *
     * Default values will be used when null.
     */
    protected Integer _blockSize;

    /**
     * Whether true passive mode is allowed, i.e. whether clients are
     * allowed to connect directly to the pool. Do not enable this if
     * the pool does not have inbound connectivity.
     */
    protected boolean      _allowPassivePool;

    /**
     * True while the transfer is in progress.
     */
    protected boolean      _inProgress;

    public GFtpProtocol_2_nio(CellEndpoint cell)
    {
        _cell = cell;

        String range = System.getProperty("org.globus.tcp.port.range");
        if (range != null) {
            _portRange = PortRange.valueOf(range);
        } else {
            _portRange = new PortRange(0);
        }
    }

    /**
     * Factory method for creating the Mode object.
     */
    protected Mode createMode(String mode, Role role, RepositoryChannel fileChannel)
            throws IOException
    {
        int blockSize;
        switch (Character.toUpperCase(mode.charAt(0))) {
        case 'S':
            blockSize =
                    (_blockSize == null) ? MODE_S_DEFAULT_BLOCK_SIZE : _blockSize;
            return new ModeS(role, fileChannel, this, blockSize);
        case 'E':
            blockSize =
                    (_blockSize == null) ? MODE_E_DEFAULT_BLOCK_SIZE : _blockSize;
            return new ModeE(role, fileChannel, this, blockSize);
        case 'X':
            blockSize =
                    (_blockSize == null) ? MODE_X_DEFAULT_BLOCK_SIZE : _blockSize;
            return new ModeX(role, fileChannel, this, blockSize);
        default:
            throw new IllegalArgumentException("Unknown mode");
        }
    }

    /**
     * Factory for creating a digest thread. May return null if no
     * checksum type is defined.
     */
    protected DigestThread createDigestThread()
    {
        if (_digests.isEmpty()) {
            return null;
        }
        return new DirectDigestThread(_fileChannel, _blockLog, _digests);
    }

    @Override
    public String toString() {
        return "SU=" + _spaceUsed + ";SA=" + _reservedSpace + ";S=" + _status;
    }

    /**
     * Receive a file.
     */
    public void transfer(RepositoryChannel fileChannel, Role role,
                         Mode mode, Allocator allocator)
            throws Exception
    {
        /* Initialise transfer parameters.
         */
        _role             = role;
        _bytesTransferred = 0;
        _blockLog         = new BlockLog();
        _fileChannel      = fileChannel;
        _allocator        = allocator;
        _reservedSpace    = 0;
        _spaceUsed        = 0;
        _status           = "None";
        DigestThread digestThread = null;

        /* Startup the transfer. The transfer is performed on a single
         * thread, no matter the number of streams.
         *
         * Checksum computation is performed on a different
         * thread. The checksum computation thread is not allowed to
         * overtake the transfer thread, but we also ensure that the
         * checksum thread does not fall too far behind. This is to
         * increase the chance that data has not yet been evicted from
         * the cache.
         */
        _multiplexer = new Multiplexer();
        try {
            _inProgress = true;

            digestThread = createDigestThread();
            if (digestThread != null) {
                Object o = _cell.getDomainContext().get(READ_AHEAD_KEY);
                if (o != null && ((String)o).length() > 0) {
                    try {
                        digestThread.setReadAhead(Long.parseLong((String)o));
                    } catch (NumberFormatException e) {
                        _log.error("Failed parsing read ahead: {}", e.getMessage());
                    }
                }

                _log.debug("Initiated checksum computation thread");
                digestThread.start();
            }

            _multiplexer.add(mode);

            _log.trace("Entering event loop");
            _multiplexer.loop();
        } catch (ClosedByInterruptException e) {
            /* Many NIO operations throw a ClosedByInterruptException
             * rather than InterruptedException. We rethrow this as an
             * InterruptedException and clear the interrupt flag on
             * the thread.
             */
            Thread.interrupted();
            throw new InterruptedException();
        } catch (BindException | ConnectException | NoRouteToHostException |
                PortUnreachableException | UnknownHostException e) {
            throw Exceptions.wrap("Failed to connect " +
                    mode.getRemoteAddressDescription() + ": " + e.getMessage(),
                    e, IOException.class);
        } catch (IOException e) {
            throw Exceptions.wrap("Problem while connected to " +
                    mode.getRemoteAddressDescription() + ": " + e.getMessage(),
                    e, IOException.class);
        } finally {
            _inProgress = false;

            /* It is important that this is done before joining the
             * digest thread, since otherwise the digest thread would
             * not terminate.
             */
            _blockLog.setEof();

            /* Close all open channels.
             */
            _log.trace("Left event loop and closing channels");
            _multiplexer.close();

            /* Wait for checksum computation to finish before
             * returning. Otherwise getActualChecksum() could
             * possibly return an incomplete checksum.
             *
             * REVISIT: If the mover gets killed here, we break out
             * with an InterruptedException. This is as such not a
             * major problem, since everything after this point is not
             * essential for clean up. It is however unfortunate that
             * the job gets killed because we wait for checksum
             * computation (in particular because the checksum
             * computation may be the cause of the timeout if it is
             * very slow).
             */
            if (digestThread != null) {
                digestThread.join();
            }

            /* Log some useful information about the transfer.
             */
            long amount = getBytesTransferred();
            long time = getTransferTime();
            if (time > 0) {
                _log.info("Transfer finished: {} bytes transferred in {} seconds = {} MB/s",
                                          amount, time / 1000.0, BYTES.toMiB(1000.0 * amount / time));
            } else {
                _log.info("Transfer finished: {} bytes transferred in less than 1 ms", amount);
            }
        }

        /* REVISIT: Error reporting from the digest thread is not
         * optimal. In case of errors, they are not detected until
         * here. It would be better if digestThread could shutdown the
         * multiplexer. Maybe we should simply embed the DigestThread
         * class into the Mover.
         */
        if (digestThread != null && digestThread.getLastError() != null) {
            _log.error(digestThread.getLastError().toString());
            throw digestThread.getLastError();
        }

        /* Check that we receive the whole file.
         */
        if (!_blockLog.isComplete()) {
            throw new CacheException(44, "Incomplete file detected");
        }
    }

    /** Part of the MoverProtocol interface. */
    @Override
    public void runIO(FileAttributes fileAttributes,
                      RepositoryChannel fileChannel,
                      ProtocolInfo protocol,
                      Allocator    allocator,
                      IoMode          access)
            throws Exception
    {
        if (!(protocol instanceof GFtpProtocolInfo)) {
            throw new CacheException(44, "Protocol info not of type GFtpProtocolInfo");
        }
        GFtpProtocolInfo gftpProtocolInfo    = (GFtpProtocolInfo)protocol;

        Role role = access == IoMode.WRITE ? Role.Receiver : Role.Sender;
        int    version     = gftpProtocolInfo.getMajorVersion();
        InetSocketAddress address = gftpProtocolInfo.getSocketAddress();
        int    bufferSize  = gftpProtocolInfo.getBufferSize();
        int    parallelism = gftpProtocolInfo.getParallelStart();
        long   offset      = gftpProtocolInfo.getOffset();
        long   size        = gftpProtocolInfo.getSize();
        boolean passive    = gftpProtocolInfo.getPassive() && _allowPassivePool;
        ProtocolFamily protocolFamily = gftpProtocolInfo.getProtocolFamily();

        _log.debug("version={}, role={}, mode={}, host={} buffer={}, passive={}, parallelism={}",
                  version, role, gftpProtocolInfo.getMode(),
                  address, bufferSize, passive, parallelism);

        /* Sanity check the parameters.
         */
        if (gftpProtocolInfo.getPassive() && version == 1) {
            /* In passive mode we need to be able to send the port we
             * listen on to the client. With GFtp/1, we cannot send
             * this information back to the door.
             */
            throw new CacheException(44, "Internal error: Cannot do passive transfer with mover protocol version 1.");
        }

        /* If on transfer checksum calculation is enabled, check if
         * we have a protocol specific preferred algorithm.
         */
        if (_checksumFactories != null) {
            ChecksumFactory factory = getChecksumFactory(gftpProtocolInfo);
            if (factory != null) {
                _checksumFactories = new HashSet<>(Arrays.asList(factory));
            }
            _digests = _checksumFactories.stream().collect(Collectors.toMap(f -> f.getType(),
                                                                            f -> f.create()));
        }

        /* We initialise these things early, as the job timeout
         * manager will not kill the job otherwise.
         */
        _transferStarted  = System.currentTimeMillis();
        _lastTransferred  = _transferStarted;

        Mode mode = createMode(gftpProtocolInfo.getMode(), role, fileChannel);
        mode.setBufferSize(bufferSize);

        /* For GFtp/2, the FTP door expects a
         * GFtpTransferStartedMessage when the mover is ready to
         * transfer the data.
         */
        if (version == 2) {
            /* When in passive mode, the door passes us the host
             * from which the control channel was created. It
             * seems like a safe assumption that the data channel
             * will be established from the same network.
             */
            InetAddress localAddress = null;
            if (passive) {
                InetAddress clientAddress =
                        InetAddress.getByName(gftpProtocolInfo.getClientAddress());
                localAddress = NetworkUtils.getLocalAddress(clientAddress, protocolFamily);
                if (localAddress == null) {
                    passive = false;
                }
            }

            GFtpTransferStartedMessage message;
            if (passive) {
                assert localAddress != null;

                /* When using true passive mode, we open a server
                 * socket and send a message containing the port
                 * number back to the door.
                 */
                ServerSocketChannel channel = ServerSocketChannel.open();
                if (bufferSize > 0) {
                    channel.socket().setReceiveBufferSize(bufferSize);
                }
                _portRange.bind(channel.socket(), localAddress, 128);

                message =
                        new GFtpTransferStartedMessage(fileAttributes.getPnfsId().getId(),
                                                       channel.socket().getInetAddress().getHostAddress(),
                                                       channel.socket().getLocalPort());
                mode.setPassive(channel);
            } else {
                /* If passive mode is disabled, then fall back to
                 * active mode.  When notified about this, the door
                 * will fall back to proxy mode.
                 */
                message = new GFtpTransferStartedMessage(fileAttributes.getPnfsId().getId());
            }
            CellPath path = new CellPath(gftpProtocolInfo.getDoorCellName(),
                                         gftpProtocolInfo.getDoorCellDomainName());
            _cell.sendMessage(new CellMessage(path, message));
        }

        if (!passive) {
            /* We use PROXY or ACTIVE mode. In proxy mode, host and
             * port identify the SocketAdapter running at the door. In
             * Active mode, host and port identify the client. Either
             * way, we do not really care.
             */
            mode.setActive(address);
        }

        /* - Parallel transfers in stream mode are not defined.
         *
         * - Reception in E mode must be passive (incoming). If the
         *   connection is outgoing, it means we use a proxy at the door.
         *   This proxy is limited to one connection from the mover.
         *
         * In either case, set the parallelism to one.
         */
        switch (Character.toUpperCase(gftpProtocolInfo.getMode().charAt(0))) {
        case 'E':
            if (role == Role.Receiver && !passive) {
                parallelism = 1;
            }
            break;
        case 'S':
            parallelism = 1;
            break;
        }
        mode.setParallelism(parallelism);

        /* Setup partial retrieve parameters. These settings have
         * already been checked by the door, but better safe than
         * sorry...
         */
        if (role == Role.Sender) {
            long fileSize   = fileChannel.size();
            if (offset < 0) {
                String err = "prm_offset is " + offset;
                _log.error(err);
                throw new IllegalArgumentException(err);
            }
            if (size < 0) {
                String err = "prm_offset is " + size;
                _log.error(err);
                throw new IllegalArgumentException(err);
            }
            if (offset + size > fileSize) {
                String err = "invalid prm_offset=" + offset + " and prm_size "
                        + size + " for file of size " + fileSize;
                _log.error(err);
                throw new IllegalArgumentException(err);
            }
            mode.setPartialRetrieveParameters(offset, size);
        }

        try {
            transfer(fileChannel, role, mode, allocator);
        } finally {
            /* Log some useful information about the transfer. This
             * will be send back to the door by the pool cell.
             */
            gftpProtocolInfo.setBytesTransferred(getBytesTransferred());
            gftpProtocolInfo.setTransferTime(getTransferTime());
            if (passive) {
                gftpProtocolInfo.setSocketAddress(
                        Iterables.getFirst(mode.getRemoteAddresses(), gftpProtocolInfo.getSocketAddress()));
            }
        }
    }

    /** Part of the MoverProtocol interface. */
    @Override
    public long getBytesTransferred()
    {
        return _bytesTransferred;
    }

    /** Part of the MoverProtocol interface. */
    @Override
    public long getTransferTime()
    {
        return (_inProgress ? System.currentTimeMillis() : _lastTransferred)
                - _transferStarted;
    }

    /** Part of the MoverProtocol interface. */
    @Override
    public long getLastTransferred()
    {
        return _lastTransferred;
    }

    /** Part of the ChecksumMover interface. */
    private ChecksumFactory getChecksumFactory(GFtpProtocolInfo protocol)
    {
        String type = protocol.getChecksumType();
        if (type == null || type.equals("Unknown")) {
            return null;
        }

        try {
            return ChecksumFactory.getFactory(ChecksumType.getChecksumType(type));
        } catch (NoSuchAlgorithmException e) {
            _log.error("CRC Algorithm is not supported: {}", type);
        }

        return null;
    }

    @Override
    public void enableTransferChecksum(ChecksumType suggestedAlgorithm)
            throws NoSuchAlgorithmException
    {
        _checksumFactories = Sets.newHashSet(ChecksumFactory.getFactory(suggestedAlgorithm));
    }

    /** Part of the ChecksumMover interface. */
    @Override
    public Checksum getExpectedChecksum()
    {
        return null;
    }

    /** Part of the ChecksumMover interface. */
    @Nonnull
    @Override
    public Set<Checksum> getActualChecksums()
    {
        return (_digests.isEmpty())
                        ? Collections.emptySet() :
                          _checksumFactories.stream()
                                            .map(f -> f.create(_digests.get(f.getType()).digest()))
                                            .collect(Collectors.toSet());
    }

    /** Part of the ConnectionMonitor interface. */
    @Override
    public void receivedBlock(long position, long size) throws FTPException
    {
        checkState(_role == Role.Receiver, "Only receivers can receive");
        checkArgument(position >= 0, "Position must be non-negative");
        checkArgument(size >= 0, "Size must be non-negative");
        checkState(position + size <= _spaceUsed, "Must call preallocate before receiving data");

        _log.trace("received {} {}", position, size);

        _blockLog.addBlock(position, size);
        _bytesTransferred += size;
        _lastTransferred = System.currentTimeMillis();
    }

    /** Part of the ConnectionMonitor interface. */
    @Override
    public void sentBlock(long position, long size) throws FTPException
    {
        checkState(_role == Role.Sender, "Only senders can send");
        checkArgument(position >= 0, "Position must be non-negative");
        checkArgument(size >= 0, "Size must be non-negative");

        _log.trace("send {} {}", position, size);

        _blockLog.addBlock(position, size);
        _bytesTransferred += size;
        _lastTransferred = System.currentTimeMillis();
    }

    /**
     * Part of the ConnectionMonitor interface. This is the only call
     * used inside the event loop that may block. This may happen when
     * we run out of space. In principle, other streams receiving data
     * placed earlier in the file may continue, however if we are
     * about to run out of disk space, it may actually be a good idea
     * to block all streams.
     */
    @Override
    public void preallocate(long position) throws InterruptedException
    {
        if (_role != Role.Receiver) {
            throw new IllegalStateException("Only receivers can allocate space");
        }
        if (position < 0) {
            throw new IllegalArgumentException("Position must be positive");
        }

        if (position > _reservedSpace) {
            long additional = Math.max(position - _reservedSpace, SPACE_INC);
            _status = "WaitingForSpace(" + additional + ")";
            _logSpaceAllocation.debug("ALLOC: " + additional );
            _allocator.allocate(additional);
            _status = "None";
            _reservedSpace += additional;
        }
        _spaceUsed = Math.max(_spaceUsed, position);
    }

    /**
     * Returns the value of an option, or a default value if the
     * option has not been set.
     */
    public static String getOption(Args args, String name, String defaultValue)
    {
        String value = args.getOpt(name);
        return value == null ? defaultValue : value;
    }

    /**
     * Prints help information for the test utility to stdout.
     */
    public static void help()
    {
        System.out.println("Usage: mover -l [OPTION]... ROLE FILE");
        System.out.println("       mover [OPTION]... ROLE FILE HOSTNAME");
        System.out.println("where ROLE is either -s or -r");
        System.out.println("  -port=PORT");
        System.out.println("  -buffer=SIZE");
        System.out.println("  -streams=NUMBER");
        System.out.println("  -offset=BYTES");
        System.out.println("  -size=BYTES");
        System.out.println("  -mode=(S|E|X)");
        System.out.println("  -digest=ALGORITHM");
        System.exit(1);
    }

    /**
     * Test program for this class.
     */
    public static void main(String a[]) {
        try {
            Args args = new Args(a);
            int port        = Integer.parseInt(getOption(args, "port", "2288"));
            int bufferSize  = Integer.parseInt(getOption(args, "buffer", "0"));
            int parallelism = Integer.parseInt(getOption(args, "streams", "1"));
            long offset     = Long.parseLong(getOption(args, "offset", "0"));
            long size       = Long.parseLong(getOption(args, "size", "0"));
            String digest   = getOption(args, "digest", "");

            Role role = Role.Receiver;
            if (args.isOneCharOption('r')) {
                role = Role.Receiver;
            } else if (args.isOneCharOption('s')) {
                role = Role.Sender;
            } else {
                help();
            }

            GFtpProtocol_2_nio mover =
                    new GFtpProtocol_2_nio(null);

            RepositoryChannel fileChannel =
                    new FileRepositoryChannel(FileSystems.getDefault().getPath(args.argv(0)),
                                              role == Role.Sender ? "r" : "rw");

            Mode mode =
                    mover.createMode(getOption(args, "mode", "S"), role, fileChannel);

            if (args.isOneCharOption('l')) {
                if (args.argc() != 1) {
                    help();
                }
                ServerSocketChannel channel = ServerSocketChannel.open();
                if (bufferSize > 0) {
                    channel.socket().setReceiveBufferSize(bufferSize);
                }
                channel.socket().bind(new InetSocketAddress(port));
                mode.setPassive(channel);
            } else {
                if (args.argc() != 2) {
                    help();
                }
                mode.setActive(new InetSocketAddress(args.argv(1), port));
            }

            if (digest.length() > 0 && role != Role.Receiver) {
                System.err.println("Digest can only be computed on receive");
                System.exit(1);
            }

            if (size == 0) {
                size = fileChannel.size() - offset;
            }

            mode.setParallelism(parallelism);
            mode.setPartialRetrieveParameters(offset, size);

            if (digest.length() > 0) {
                mover.enableTransferChecksum(ChecksumType.getChecksumType(digest));
            }

            mover.transfer(fileChannel, role, mode, null);
            if (digest.length() > 0) {
                System.out.println(mover.getActualChecksums());
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void setCellArgs(Args args)
    {
        _allowPassivePool = args.getBooleanOption("ftpAllowIncomingConnections");

        if (args.hasOption("gsiftpBlockSize")) {
            _blockSize = args.getIntOption("gsiftpBlockSize");
        }
    }
}

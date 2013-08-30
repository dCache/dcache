package org.dcache.pool.movers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.UUID;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.DCapProrocolChallenge;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.util.Args;

import org.dcache.net.ProtocolConnectionPool;
import org.dcache.net.ProtocolConnectionPoolFactory;
import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.NetworkUtils;
import org.dcache.vehicles.FileAttributes;


public class DCapProtocol_3_nio implements MoverProtocol, ChecksumMover {

    private static Logger _log = LoggerFactory.getLogger(DCapProtocol_3_nio.class);
    private static Logger _logSocketIO = LoggerFactory.getLogger("logger.dev.org.dcache.io.socket");
    private final static Logger _logSpaceAllocation = LoggerFactory.getLogger("logger.dev.org.dcache.poolspacemonitor." + DCapProtocol_3_nio.class.getName());
    private static final int INC_SPACE  =  (50*1024*1024);

    private final Args          _args   ;
    private final Map<String,Object> _context;
    private final CellEndpoint     _cell;

    private long _bytesTransferred   = -1;
    private long _transferStarted;
    private long _transferTime       = -1;
    private long _lastTransferred    = System.currentTimeMillis();

    private ByteBuffer _bigBuffer;
    private String  _status          = "None";
    private boolean _io_ok           = true;
    private long    _ioError         = -1;
    private PnfsId  _pnfsId;
    private int     _sessionId       = -1;
    private boolean _wasChanged;

    private  Checksum  _clientChecksum;
    private ChecksumFactory _checksumFactory;
    private MessageDigest _digest;

    private final MoverIoBuffer _defaultBufferSize = new MoverIoBuffer(256 * 1024, 256 * 1024, 256 * 1024);
    private final MoverIoBuffer _maxBufferSize     = new MoverIoBuffer(1024 * 1024, 1024 * 1024, 1024 * 1024);

    private SpaceMonitorHandler _spaceMonitorHandler;


    // bind passive dcap to port defined as org.dcache.dcap.port
    private static ProtocolConnectionPoolFactory protocolConnectionPoolFactory;

    static {
        int port = 0;

        try {
            port = Integer.parseInt(System.getProperty("org.dcache.dcap.port"));
        }catch(NumberFormatException e){ /* bad values are ignored */}

        protocolConnectionPoolFactory =
            new ProtocolConnectionPoolFactory(port, new DCapChallengeReader());

    }

    private void initialiseBuffer(MoverIoBuffer bufferSize) {
        try {
            _bigBuffer = _bigBuffer
                    == null ? ByteBuffer.allocate(bufferSize.getIoBufferSize()) :
                        _bigBuffer;
        } catch (OutOfMemoryError om) {
            _bigBuffer = ByteBuffer.allocate(32 * 1024);
        }
    }

    private MoverIoBuffer prepareBufferSize(StorageInfo storage) {
        MoverIoBuffer bufferSize = new MoverIoBuffer(_defaultBufferSize);
        String tmp;
        try {
            tmp = storage.getKey("send");
            if (tmp != null) {
                bufferSize.setSendBufferSize(Math.min(Integer.parseInt(tmp), _maxBufferSize.getSendBufferSize()));
            }
        } catch (NumberFormatException e) { /* bad values are ignored */

        }
        try {
            tmp = storage.getKey("receive");
            if (tmp != null) {
                bufferSize.setRecvBufferSize(Math.min(Integer.parseInt(tmp), _maxBufferSize.getRecvBufferSize()));
            }
        } catch (NumberFormatException e) { /* bad values are ignored */

        }
        try {
            tmp = storage.getKey("bsize");
            if (tmp != null) {
                bufferSize.setIoBufferSize(Math.min(Integer.parseInt(tmp), _maxBufferSize.getIoBufferSize()));
            }
        } catch (NumberFormatException e) { /* bad values are ignored */

        }
        return bufferSize;
    }

    private class SpaceMonitorHandler {

        private final Allocator _allocator;
        private long         _spaceAllocated;
        private long          _allocationSpace = INC_SPACE;
        private long         _spaceUsed;
        private SpaceMonitorHandler(Allocator allocator){
            _allocator = allocator;
        }
        private void setAllocationSpace(long allocSpace){
            _allocationSpace = allocSpace;
        }
        private void setInitialSpace(long space){
            _spaceAllocated = space;
            _spaceUsed      = space;
        }

        @Override
        public String toString(){
            return "{a="+_spaceAllocated+";u="+_spaceUsed+"}";
        }
        private void getSpace(long newEof) throws InterruptedException{
            if (_allocator == null) {
                return;
            }

            while(newEof > _spaceAllocated){
                _status = "WaitingForSpace("+_allocationSpace+")";
                _log.debug("Allocating new space : {}", _allocationSpace);
                _logSpaceAllocation.debug("ALLOC: {} : {}", _pnfsId, _allocationSpace);
                _allocator.allocate(_allocationSpace);
                _spaceAllocated += _allocationSpace;
                _log.debug("Allocated new space : {}", _allocationSpace);
                _status = "";
            }
        }
        private void newFilePosition(long newPosition){
            _spaceUsed = Math.max(newPosition, _spaceUsed);
        }

    }
    //
    //   helper class to use nio channels for input requests.
    //
    private class RequestBlock {

        private ByteBuffer _buffer;
        private int _commandSize;
        private int _commandCode;

        private RequestBlock(){
            _buffer = ByteBuffer.allocate(16384);
        }
        private void read(SocketChannel channel) throws Exception {

            _commandSize = _commandCode = 0;

            _buffer.clear().limit(4);
            fillBuffer(channel);
            _buffer.rewind();
            _commandSize = _buffer.getInt();

            if(_commandSize < 4) {
                throw new
                        CacheException(44, "Protocol Violation (cl<4)");
            }

            try {
        	_buffer.clear().limit(_commandSize);
            }catch(IllegalArgumentException iae) {
        	_log.error("Command size excided command block size : " + _commandSize + "/" + _buffer.capacity());
        	throw iae;
            }
            fillBuffer(channel);
            _buffer.rewind();
            _commandCode = _buffer.getInt();
        }
        private int remaining(){ return _buffer.remaining(); }
        private int getCommandCode(){ return _commandCode; }
        private int nextInt(){ return _buffer.getInt(); }
        private long nextLong(){ return _buffer.getLong(); }
        private void fillBuffer( SocketChannel channel) throws Exception{
            while(_buffer.hasRemaining()){
                if(channel.read(_buffer) < 0) {
                    throw new
                            EOFException("EOF on input socket (fillBuffer)");
                }
            }
        }
        private void skip(int skip){
            _buffer.position(_buffer.position()+skip);
        }
        private void get(byte [] array){ _buffer.get(array); }

        @Override
        public String toString(){
            return "RequestBlock [Size="+_commandSize+
                " Code="+_commandCode+
                " Buffer="+_buffer;
        }
    }
    public DCapProtocol_3_nio(CellEndpoint cell){

        _cell    = cell;
        _args    = _cell.getArgs();
        _context = _cell.getDomainContext();
        //
        _log.info("DCapProtocol_3 (nio) created $Id: DCapProtocol_3_nio.java,v 1.17 2007-10-02 13:35:52 tigran Exp $");
        //
        // we are created for each request. So our data
        // is not shared.
        //
        _defaultBufferSize.setBufferSize(
                                         getParameterInt("defaultSendBufferSize", _defaultBufferSize.getSendBufferSize()),
                                         getParameterInt("defaultRecvBufferSize", _defaultBufferSize.getRecvBufferSize()),
                                         getParameterInt("defaultIoBufferSize"  , _defaultBufferSize.getIoBufferSize())
                                        );
        _maxBufferSize.setBufferSize(
                                     getParameterInt("maxSendBufferSize", _maxBufferSize.getSendBufferSize()),
                                     getParameterInt("maxRecvBufferSize", _maxBufferSize.getRecvBufferSize()),
                                     getParameterInt("maxIoBufferSize"  , _maxBufferSize.getIoBufferSize())
                                    );
        _log.info("Setup : Defaults Buffer Sizes  : {}", _defaultBufferSize);
        _log.info("Setup : Max Buffer Sizes       : {}", _maxBufferSize);

    }
    private synchronized int getParameterInt(String name, int defaultValue){
        String stringValue = (String)_context.get("dCap3-"+name);
        stringValue = stringValue == null ? _args.getOpt(name) : stringValue;
        try{
            return stringValue == null ? defaultValue : Integer.parseInt(stringValue);
        }catch(NumberFormatException e){
            return defaultValue;
        }
    }

    @Override
    public String toString(){
        return "SM="+_spaceMonitorHandler+";S="+_status;
    }

    protected String getCellName()
    {
        return _cell.getCellInfo().getCellName();
    }

    protected String getCellDomainName()
    {
        return _cell.getCellInfo().getDomainName();
    }

    @Override
    public void runIO(FileAttributes fileAttributes,
                      RepositoryChannel  fileChannel,
                      ProtocolInfo protocol,
                      Allocator    allocator,
                      IoMode          access  )
        throws Exception
    {
        Exception ioException         = null;

        if(! (protocol instanceof DCapProtocolInfo)) {
            throw new
                    CacheException(44, "protocol info not DCapProtocolInfo");
        }
        DCapProtocolInfo dcapProtocolInfo = (DCapProtocolInfo)protocol;

        StorageInfo storage = fileAttributes.getStorageInfo();
        _pnfsId              = fileAttributes.getPnfsId();
        _spaceMonitorHandler = new SpaceMonitorHandler(allocator);

        ////////////////////////////////////////////////////////////////////////
        //                                                                    //
        //    Prepare the tunable parameters                                  //
        //                                                                    //

        try{
            String allocation = storage.getKey("alloc-size");
            if(allocation != null){
                long allocSpace = Long.parseLong(allocation);
                if(allocSpace <= 0) {
                    // negative allocation requested....Ignoring
                    _log.info("Options : alloc-space = {} ....Ignoring", allocSpace);
                }else{
                    _spaceMonitorHandler.setAllocationSpace(allocSpace);
                    _log.info("Options : alloc-space = {}", allocSpace);
                }
            }
        }catch(NumberFormatException e){ /* bad values are ignored */}

        try{
            String io = storage.getKey("io-error");
            if(io != null) {
                _ioError = Long.parseLong(io);
            }
        }catch(NumberFormatException e){ /* bad values are ignored */}
        _log.info("ioError = {}", _ioError);
//        gets the buffervalues of the storageInfo keys
        MoverIoBuffer bufferSize = prepareBufferSize(storage);
        _log.info("Client : Buffer Sizes : {}", bufferSize);
//        allocates the _bigBuffer
        initialiseBuffer(bufferSize);

        SocketChannel socketChannel = null;
        DCapOutputByteBuffer cntOut = new DCapOutputByteBuffer(1024);

        _sessionId  = dcapProtocolInfo.getSessionId();

        if(! dcapProtocolInfo.isPassive()) {

            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(true);

            Socket socket = socketChannel.socket();
            socket.setKeepAlive(true);
            socket.setTcpNoDelay(true);
            if (bufferSize.getRecvBufferSize() > 0) {
                socket.setReceiveBufferSize(bufferSize
                        .getRecvBufferSize());
            }
            if (bufferSize.getSendBufferSize() > 0) {
                socket.setSendBufferSize(bufferSize
                        .getSendBufferSize());
            }

            socketChannel.connect(dcapProtocolInfo.getSocketAddress());

            if (_logSocketIO.isDebugEnabled()) {
                _logSocketIO.debug("Socket OPEN remote = {}:{} local = {}:{}", socket.getInetAddress(), socket.getPort(),
                                   socket.getLocalAddress(), socket.getLocalPort());
            }
            _log.info("Using : Buffer Sizes (send/recv/io) : {}/{}/{}", socket.getSendBufferSize(), socket.getReceiveBufferSize(),
                      _bigBuffer.capacity());
            _log.info("Connected to {}", dcapProtocolInfo.getSocketAddress());

            //
            // send the sessionId and our (for now) 0 byte security challenge.
            //
            _bigBuffer.clear();
            _bigBuffer.putInt(_sessionId).putInt(0);
            _bigBuffer.flip();
            socketChannel.write(_bigBuffer);
        }else{ // passive connection
            ProtocolConnectionPool pcp =
                protocolConnectionPoolFactory.getConnectionPool(bufferSize.getRecvBufferSize());

            InetAddress localAddress = NetworkUtils.
                    getLocalAddress(dcapProtocolInfo.getSocketAddress().getAddress());
            InetSocketAddress socketAddress =
                new  InetSocketAddress(localAddress,
                                       pcp.getLocalPort());

            byte[] challenge = UUID.randomUUID().toString().getBytes();
            PoolPassiveIoFileMessage<byte[]> msg = new PoolPassiveIoFileMessage<>("pool",
                    socketAddress, challenge);
            msg.setId(dcapProtocolInfo.getSessionId());
            _log.info("waiting for client to connect ({}:{})",
                      localAddress, pcp.getLocalPort());

            CellPath cellpath = dcapProtocolInfo.door();
            _cell.sendMessage (new CellMessage(cellpath, msg));
            DCapProrocolChallenge dcapChallenge = new DCapProrocolChallenge(_sessionId, challenge);
            socketChannel = pcp.getSocket(dcapChallenge);

            Socket socket = socketChannel.socket();
            socket.setKeepAlive(true);
            socket.setTcpNoDelay(true);
            if (bufferSize.getSendBufferSize() > 0) {
                socket.setSendBufferSize(bufferSize.getSendBufferSize());
            }
        }

        //
        //
        _transferStarted  = System.currentTimeMillis();
        _bytesTransferred = 0;
        _lastTransferred  = _transferStarted;

        _spaceMonitorHandler.setInitialSpace(fileChannel.size());

        boolean      notDone      = true;
        RequestBlock requestBlock = new RequestBlock();

        try{
            while(notDone && _io_ok){

                if(Thread.interrupted()) {
                    throw new
                            InterruptedException("Interrupted By Operator");
                }

                //
                // get size of next command
                //
                try{

                    requestBlock.read(socketChannel);

                    _log.debug("Request Block : {}", requestBlock);

                }catch(EOFException eofe){
                    _log.debug("Dataconnection closed by peer : {}", eofe.toString());
                    throw eofe;
                }catch(BufferUnderflowException bue){
                    throw new
                        CacheException(43,"Protocol Violation (csl<4)");
                }

                _lastTransferred    = System.currentTimeMillis();

                switch(requestBlock.getCommandCode()){
                    //-------------------------------------------------------------
                    //
                    //                     The Write
                    //
                case DCapConstants.IOCMD_WRITE :
                    //
                    // no further arguments (yet)
                    //
                    if(! _io_ok){

                        String errmsg = "WRITE denied (IO not ok)";
                        _log.error(errmsg);
                        cntOut.writeACK(DCapConstants.IOCMD_WRITE,CacheException.ERROR_IO_DISK,errmsg);
                        socketChannel.write(cntOut.buffer());

                    }else if(access == IoMode.WRITE){

                        //
                        //   The 'REQUEST ACK'
                        //
                        cntOut.writeACK(DCapConstants.IOCMD_WRITE);
                        socketChannel.write(cntOut.buffer());
                        //
                        doTheWrite(fileChannel,
                                    cntOut,
                                    socketChannel);
                        //
                        //
                        if(_io_ok){
                            cntOut.writeFIN(DCapConstants.IOCMD_WRITE);
                            socketChannel.write(cntOut.buffer());
                        }else{
                            _log.error("Reporting IO problem to client");
                            cntOut.writeFIN(DCapConstants.IOCMD_WRITE,CacheException.ERROR_IO_DISK,
                                            "[2]Problem in writing");
                            socketChannel.write(cntOut.buffer());
                        }

                    }else{

                        String errmsg = "WRITE denied (not allowed)";
                        _log.error(errmsg);
                        cntOut.writeACK(DCapConstants.IOCMD_WRITE,CacheException.ERROR_IO_DISK,errmsg);
                        socketChannel.write(cntOut.buffer());

                    }
                    break;
                    //-------------------------------------------------------------
                    //
                    //                     The Read
                    //
                case DCapConstants.IOCMD_READ :
                    //
                    //
                    _digest = null;

                    long blockSize = requestBlock.nextLong();

                    _log.debug("READ byte={}", blockSize);

                    if(_io_ok){

                        cntOut.writeACK(DCapConstants.IOCMD_READ);
                        socketChannel.write(cntOut.buffer());

                        doTheRead(fileChannel, cntOut, socketChannel,  blockSize);

                        if(_io_ok){
                            cntOut.writeFIN(DCapConstants.IOCMD_READ);
                            socketChannel.write(cntOut.buffer());
                        }else{
                            String errmsg = "FIN : READ failed (IO not ok)";
                            _log.error(errmsg);
                            cntOut.writeFIN(DCapConstants.IOCMD_READ,CacheException.ERROR_IO_DISK,errmsg);
                            socketChannel.write(cntOut.buffer());
                        }
                    }else{

                        String errmsg = "ACK : READ denied (IO not ok)";
                        _log.error(errmsg);
                        cntOut.writeACK(DCapConstants.IOCMD_READ,CacheException.ERROR_IO_DISK,errmsg);
                        socketChannel.write(cntOut.buffer());

                    }

                    break;
                    //-------------------------------------------------------------
                    //
                    //                     The Seek
                    //
                case DCapConstants.IOCMD_SEEK :

                    _digest = null;

                    long offset = requestBlock.nextLong();
                    int  whence = requestBlock.nextInt();

                    doTheSeek(fileChannel , whence, offset, (access == IoMode.WRITE) );

                    if(_io_ok){

                        cntOut.writeACK(fileChannel.position());
                        socketChannel.write(cntOut.buffer());

                    }else{

                        String errmsg = "SEEK failed : IOError ";
                        _log.error(errmsg);
                        cntOut.writeACK(DCapConstants.IOCMD_SEEK,6,errmsg);
                        socketChannel.write(cntOut.buffer());

                    }

                    break;
                    //-------------------------------------------------------------
                    //
                    //                     The IOCMD_SEEK_AND_READ
                    //
                case DCapConstants.IOCMD_SEEK_AND_READ :

                    _digest = null;

                    offset    = requestBlock.nextLong();
                    whence    = requestBlock.nextInt();
                    blockSize = requestBlock.nextLong();

                    if(_io_ok){

                        cntOut.writeACK(DCapConstants.IOCMD_SEEK_AND_READ);
                        socketChannel.write(cntOut.buffer());

                        doTheSeek(fileChannel, whence, offset, (access == IoMode.WRITE) );

                        if(_io_ok) {
                            doTheRead(fileChannel, cntOut, socketChannel, blockSize);
                        }

                        if(_io_ok){
                            cntOut.writeFIN(DCapConstants.IOCMD_SEEK_AND_READ);
                            socketChannel.write(cntOut.buffer());
                        }else{
                            String errmsg = "FIN : SEEK_READ failed (IO not ok)";
                            _log.error(errmsg);
                            cntOut.writeFIN(DCapConstants.IOCMD_SEEK_AND_READ,CacheException.ERROR_IO_DISK,errmsg);
                            socketChannel.write(cntOut.buffer());
                        }

                    }else{
                        String errmsg = "SEEK_AND_READ denied : IOError " ;
                        _log.error(errmsg);
                        cntOut.writeACK(DCapConstants.IOCMD_SEEK_AND_READ,CacheException.ERROR_IO_DISK,errmsg);
                        socketChannel.write(cntOut.buffer());
                    }
                    break;
                    //-------------------------------------------------------------
                    //
                    //                     The IOCMD_SEEK_AND_WRITE
                    //
                case DCapConstants.IOCMD_SEEK_AND_WRITE :

                    _digest = null;
                    offset    = requestBlock.nextLong();
                    whence    = requestBlock.nextInt();

                    if(!_io_ok) {
                        String errmsg = "SEEK_AND_WRITE denied : IOError";
                        _log.error(errmsg);
                        cntOut.writeACK(DCapConstants.IOCMD_SEEK_AND_WRITE, CacheException.ERROR_IO_DISK, errmsg);
                        socketChannel.write(cntOut.buffer());
                    } else if (access == IoMode.WRITE) {
                        String errmsg = "SEEK_AND_WRITE denied (not allowed)";
                        _log.error(errmsg);
                        cntOut.writeACK(DCapConstants.IOCMD_SEEK_AND_WRITE, CacheException.ERROR_IO_DISK, errmsg);
                        socketChannel.write(cntOut.buffer());
                    } else {

                        cntOut.writeACK(DCapConstants.IOCMD_SEEK_AND_WRITE);
                        socketChannel.write(cntOut.buffer());

                        doTheSeek(fileChannel, whence, offset, (access == IoMode.WRITE) );

                        if(_io_ok) {
                            doTheWrite(fileChannel,
                                    cntOut,
                                    socketChannel);
                        }

                        if(_io_ok){
                            cntOut.writeFIN(DCapConstants.IOCMD_SEEK_AND_WRITE);
                            socketChannel.write(cntOut.buffer());
                        }else{
                            String errmsg = "SEEK_AND_WRITE failed : IOError";
                            _log.error(errmsg);
                            cntOut.writeFIN(DCapConstants.IOCMD_SEEK_AND_WRITE,CacheException.ERROR_IO_DISK,errmsg);
                            socketChannel.write(cntOut.buffer());
                        }

                    }
                    break;
                    //-------------------------------------------------------------
                    //
                    //                     The IOCMD_CLOSE
                    //
                case DCapConstants.IOCMD_CLOSE :

                    if(_io_ok){
                        cntOut.writeACK(DCapConstants.IOCMD_CLOSE);
                        socketChannel.write(cntOut.buffer());

                        try{
                            while(requestBlock.remaining() > 4){
                                scanCloseBlock(requestBlock,storage);
                            }
                        }catch(Exception ee){
                            _log.error("Problem in close block {}", ee.toString());
                        }
                    }else{
                        cntOut.writeACK(DCapConstants.IOCMD_CLOSE,CacheException.ERROR_IO_DISK,"IOError");
                        socketChannel.write(cntOut.buffer());
                    }
                    notDone = false;
                    break;
                    //-------------------------------------------------------------
                    //
                    //                     The IOCMD_LOCATE
                    //
                case DCapConstants.IOCMD_LOCATE :

                    try{
                        long size     = fileChannel.position();
                        long location = fileChannel.size();
                        _log.debug("LOCATE : size={};position={}", size, location);
                        cntOut.writeACK(location, size);
                        socketChannel.write(cntOut.buffer());
                    }catch(Exception e){
                        cntOut.writeACK(DCapConstants.IOCMD_LOCATE,-1,e.toString());
                        socketChannel.write(cntOut.buffer());
                    }
                    break;
                    //-------------------------------------------------------------
                    //
                    //                     The IOCMD_READV (vector read)
                    //
                case DCapConstants.IOCMD_READV :

                    try{

                        if(_io_ok){

                            cntOut.writeACK(DCapConstants.IOCMD_READV);
                            socketChannel.write(cntOut.buffer());

                            doTheReadv(fileChannel, cntOut, socketChannel, requestBlock);

                            if(_io_ok){
                                cntOut.writeFIN(DCapConstants.IOCMD_READV);
                                socketChannel.write(cntOut.buffer());
                            }else{
                                String errmsg = "FIN : READV failed (IO not ok)";
                                _log.error(errmsg);
                                cntOut.writeFIN(DCapConstants.IOCMD_READV,CacheException.ERROR_IO_DISK,errmsg);
                                socketChannel.write(cntOut.buffer());
                            }
                        }else{

                            String errmsg = "ACK : READV denied (IO not ok)";
                            _log.error(errmsg);
                            cntOut.writeACK(DCapConstants.IOCMD_READV,CacheException.ERROR_IO_DISK,errmsg);
                            socketChannel.write(cntOut.buffer());

                        }

                    }catch(Exception e){
                        cntOut.writeACK(DCapConstants.IOCMD_READV,-1,e.toString());
                        socketChannel.write(cntOut.buffer());
                    }
                    break;
                default :
                    cntOut.writeACK(666, 9,"Invalid mover command : "+requestBlock);
                    socketChannel.write(cntOut.buffer());


                }

            }
        }catch(RuntimeException e){
            _log.error("Problem in command block : "+requestBlock, e);
            ioException = e;
        }catch(Exception e){
            ioException = e;
        }finally{

            try{
                _logSocketIO.debug("Socket CLOSE remote = {}:{} local {}:{}",
                        socketChannel.socket().getInetAddress(), socketChannel.socket().getPort(),
                        socketChannel.socket().getLocalAddress(), socketChannel.socket().getLocalPort());
                socketChannel.close();
            }catch(Exception xe){}

            dcapProtocolInfo.setBytesTransferred(_bytesTransferred);

            _transferTime = System.currentTimeMillis() -
                _transferStarted;
            dcapProtocolInfo.setTransferTime(_transferTime);

            _log.info("(Transfer finished : {} bytes in {} seconds) ",
                    _bytesTransferred, _transferTime/1000);

            //
            // if we got an EOF from the inputstream
            // we cancel the request but we don't want to
            // disable the pool, unless client is gone while
            // got an IO error report from pool.
            //

            if(! _io_ok) {
                throw new
                        DiskErrorCacheException(
                                   "Disk I/O Error " +
                                   (ioException!=null?ioException.toString():""));
            }else{
                if (ioException != null && !(ioException instanceof EOFException)) {
                     _log.warn("Problem in command block : {} {}", requestBlock, ioException.toString());
                    throw ioException;
                }
            }


        }

    }
    private void doTheReadv(RepositoryChannel fileChannel, DCapOutputByteBuffer cntOut,
                            SocketChannel socketChannel, RequestBlock requestBLock) throws Exception {


        cntOut.writeDATA_HEADER();
        socketChannel.write(cntOut.buffer());

        int blocks = requestBLock.nextInt();
        _log.debug("READV: {} to read", blocks);
        final int maxBuffer = _bigBuffer.capacity() - 4;
        for(int i = 0; i < blocks; i++) {


            long offset = requestBLock.nextLong();
            int count = requestBLock.nextInt();
            int len = count;

            _log.debug("READV: offset/len: {}/{}", offset, count);

            while(count > 0) {

                int bytesToRead = maxBuffer > count ? count : maxBuffer;
                int rc;
                try{
                    _bigBuffer.clear().limit(bytesToRead+4);
                    _bigBuffer.position(4);
                    rc = fileChannel.read(_bigBuffer, offset + (len - count));
                    if(rc <= 0) {
                        break;
                    }
                }catch(IOException ee){
                    _io_ok = false;
                    break;
                }

                _bigBuffer.flip();
                _bigBuffer.putInt(rc).rewind();
                _log.debug("READV: sending: {} bytes", _bigBuffer.limit());
                socketChannel.write(_bigBuffer);

                count -= rc;
                _bytesTransferred += rc;

            }
        }

    }
    private void scanCloseBlock(RequestBlock requestBlock, StorageInfo storage) {

        //
        //    Close Block Format :
        //        Size          Purpose
        //          4       (Size following)
        //          4        sub block type  (1=crc)
        //
        //   if crc
        //          4        crc type (1=adler32)
        //          n        checksum
        //
        int blockSize = requestBlock.nextInt();
        if(blockSize < 4){
            _log.error("Not a valid block size in close");
            throw new
                IllegalArgumentException("Not a valid block size in close");
        }

        int blockMode = requestBlock.nextInt();
        if(blockMode != 1){ // crc block
            _log.error("Unknown block mode ({}) in close", blockMode);
            requestBlock.skip(blockSize-4);
            return ;
        }
        int crcType = requestBlock.nextInt();

        byte [] array = new byte[blockSize-8];

        requestBlock.get(array);

        _clientChecksum =
            new Checksum(ChecksumType.getChecksumType(crcType), array);
        storage.setKey("flag-c",_clientChecksum.toString());

    }
    private void doTheSeek(RepositoryChannel fileChannel, int whence, long offset,
                            boolean writeAllowed)
    {

        try{
            long eofSize   = fileChannel.size();
            long position  = fileChannel.position();
            long newOffset;
            switch(whence){

            case DCapConstants.IOCMD_SEEK_SET :

                _log.debug("SEEK {} SEEK_SET", offset);
                //
                // this should reset the io state
                //
                if(offset == 0L) {
                    _io_ok = true;
                }
                //
                newOffset = offset;

                break;

            case DCapConstants.IOCMD_SEEK_CURRENT :

                _log.debug("SEEK {} SEEK_CURRENT", offset);
                newOffset = position + offset;

                break;
            case DCapConstants.IOCMD_SEEK_END :

                _log.debug("SEEK {} SEEK_END", offset);
                newOffset = eofSize + offset;

                break;
            default :

                throw new
                    IllegalArgumentException("Invalid seek mode : "+whence);

            }
            if((newOffset > eofSize) && ! writeAllowed) {
                throw new
                        IOException("Seek beyond EOF not allowed (write not allowed)");
            }

            //
            //  allocate the space if necessary
            //
            _spaceMonitorHandler.getSpace(newOffset);
            //
            // set the new file offset
            //
            fileChannel.position(newOffset);
            //
            //
            //  Because the seek beyond the EOF doesn't change
            //  the eof, must not call newFilePosition.
            //
            // _spaceMonitorHandler.newFilePosition(newOffset);
            //
        }catch(Exception ee){
            //
            //          don't disable pools because of this.
            //
            //         _io_ok = false;
            _log.error("Problem in seek : {}", ee.toString());
        }


    }
    private void doTheWrite(RepositoryChannel          fileChannel,
                             DCapOutputByteBuffer cntOut,
                             SocketChannel        socketChannel ) throws Exception{

        int     rest;
        int     size, rc;

        RequestBlock requestBlock = new RequestBlock();
        requestBlock.read(socketChannel);

        if(requestBlock.getCommandCode() != DCapConstants.IOCMD_DATA) {
            throw new
                    IOException("Expecting : " + DCapConstants.IOCMD_DATA + "; got : " + requestBlock
                    .getCommandCode());
        }

        while(! Thread.currentThread().isInterrupted()){

            _status = "WaitingForSize";

            _bigBuffer.clear().limit(4);
            while(_bigBuffer.hasRemaining()){
                if(socketChannel.read(_bigBuffer) < 0) {
                    throw new
                            EOFException("EOF on input socket");
                }
            }
            _bigBuffer.rewind();

            rest = _bigBuffer.getInt();
            _log.debug("Next data block : {} bytes", rest);
            //
            // if there is a space monitor, we use it
            //
            long position = fileChannel.position();
            //
            // allocate the space
            //
            _spaceMonitorHandler.getSpace(position + rest);
            //
            // we take whatever we get from the client
            // and at the end we tell'em that something went
            // terribly wrong.
            //
            long bytesAdded = 0L;
            if(rest == 0) {
                continue;
            }
            if(rest < 0) {
                break;
            }
            _wasChanged = true;
            while(rest > 0 ){

                size = _bigBuffer.capacity() > rest ?
                    rest : _bigBuffer.capacity();

                _status = "WaitingForInput";

                _bigBuffer.clear().limit(size);

                rc = socketChannel.read(_bigBuffer);

                if(rc <= 0) {
                    break;
                }

                if(_io_ok){

                    _status = "WaitingForWrite";

                    try{

                        _bigBuffer.flip();
                        bytesAdded += fileChannel.write(_bigBuffer);
                        updateChecksum(_bigBuffer);

                    }catch(Exception ioe){
                        _log.error("IOException in writing data to disk : {}", ioe.toString());
                        _io_ok = false;
                    }
                }
                rest -= rc;
                _bytesTransferred += rc;
                if((_ioError > 0L) &&
                    (_bytesTransferred > _ioError)){ _io_ok = false; }
            }
            _spaceMonitorHandler.newFilePosition(position + bytesAdded);

            _log.debug("Block Done");
        }
        _status = "Done";

    }
    private void updateChecksum(ByteBuffer buffer){
        if (_digest != null) {
            buffer.rewind();
            _digest.update(buffer);
        }
    }

    private void doTheRead(RepositoryChannel           fileChannel,
                            DCapOutputByteBuffer  cntOut,
                            SocketChannel         socketChannel,
                            long                  blockSize) throws Exception{

        //
        // REQUEST WRITE
        //
        cntOut.writeDATA_HEADER();
        socketChannel.write(cntOut.buffer());
        //
        //
        if(blockSize == 0){
            cntOut.writeEND_OF_BLOCK();
            socketChannel.write(cntOut.buffer());
            return;
        }
        long    rest = blockSize;
        int     size, rc;

        final int maxBuffer = _bigBuffer.capacity() - 4;

        while(! Thread.currentThread().isInterrupted()){

            size = maxBuffer > rest ? (int)rest : maxBuffer;

            try{
                _bigBuffer.clear().limit(size+4);
                _bigBuffer.position(4);
                rc = fileChannel.read(_bigBuffer);
                if(rc <= 0) {
                    break;
                }
            }catch(IOException ee){
                _io_ok = false;
                break;
            }
            _bigBuffer.flip();
            _bigBuffer.putInt(rc).rewind();
            socketChannel.write(_bigBuffer);
            rest -= rc;
            _bytesTransferred += rc;
            if((_ioError > 0L) && (_bytesTransferred > _ioError)){
                _io_ok = false;
                break;
            }
            if(rest <= 0) {
                break;
            }
        }
        //
        // data chain delimiter
        //
        cntOut.writeDATA_TRAILER();
        socketChannel.write(cntOut.buffer());

    }
    @Override
    public long getLastTransferred() { return _lastTransferred; }
    @Override
    public long getBytesTransferred(){ return _bytesTransferred ; }
    @Override
    public long getTransferTime(){
        return _transferTime < 0 ?
            System.currentTimeMillis() - _transferStarted :
            _transferTime ;
    }

    @Override
    public void enableTransferChecksum(ChecksumType suggestedAlgorithm)
            throws NoSuchAlgorithmException
    {
        _checksumFactory = ChecksumFactory.getFactory(suggestedAlgorithm);
        _digest = _checksumFactory.create();
    }

    @Override
    public Checksum getExpectedChecksum(){
        return  _clientChecksum ;
    }

    @Override
    public Checksum getActualChecksum(){
        return (_digest == null) ? null : _checksumFactory.create(_digest.digest());
    }

}

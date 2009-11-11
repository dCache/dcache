// $Id: DCapProtocol_3_nio.java,v 1.17 2007-10-02 13:35:52 tigran Exp $

package org.dcache.pool.movers;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.util.Map;

import org.apache.log4j.Logger;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.util.Args;

import org.dcache.net.ProtocolConnectionPool;
import org.dcache.net.ProtocolConnectionPoolFactory;
import org.dcache.pool.repository.Allocator;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DCapProrocolChallenge;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.util.Checksum;
import diskCacheV111.util.ChecksumFactory;
import java.util.UUID;


public class DCapProtocol_3_nio implements MoverProtocol, ChecksumMover {

    private static Logger _log = Logger.getLogger(DCapProtocol_3_nio.class);
    private static Logger _logSocketIO = Logger.getLogger("logger.dev.org.dcache.io.socket");
    private final static Logger _logSpaceAllocation = Logger.getLogger("logger.dev.org.dcache.poolspacemonitor." + DCapProtocol_3_nio.class.getName());
    private static final int INC_SPACE  =  (50*1024*1024);

    private final Args          _args   ;
    private final Map<String,Object> _context;
    private final CellEndpoint     _cell;

    private long _bytesTransferred   = -1;
    private long _transferStarted    = 0;
    private long _transferTime       = -1;
    private long _lastTransferred    = System.currentTimeMillis();

    private ByteBuffer _bigBuffer    = null;
    private boolean _debug           = false;
    private String  _status          = "None";
    private long    _crash           = -1;
    private String  _crashType       = null;
    private boolean _io_ok           = true;
    private long    _ioError         = -1;
    private PnfsId  _pnfsId          = null;
    private int     _sessionId       = -1;
    private boolean _wasChanged      = false;

    private  Checksum  _clientChecksum        = null;
    private  Checksum  _transferChecksum      = null;

    private final MoverIoBuffer _defaultBufferSize = new MoverIoBuffer(256 * 1024, 256 * 1024, 256 * 1024);
    private final MoverIoBuffer _maxBufferSize     = new MoverIoBuffer(1024 * 1024, 1024 * 1024, 1024 * 1024);

    private SpaceMonitorHandler _spaceMonitorHandler = null;


    // bind passive dcap to port defined as org.dcache.dcap.port
    private static ProtocolConnectionPoolFactory protocolConnectionPoolFactory = null;

    static {
        int port = 0;

        try {
            port = Integer.parseInt(System.getProperty("org.dcache.dcap.port"));
        }catch(NumberFormatException e){ /* bad values are ignored */}

        protocolConnectionPoolFactory =
            new ProtocolConnectionPoolFactory(port, new DCapChallengeReader());

    }

    private class SpaceMonitorHandler {

        private final Allocator _allocator;
        private long         _spaceAllocated  = 0;
        private long          _allocationSpace = INC_SPACE;
        private long         _spaceUsed       = 0;
        private long         _initialSpace    = 0L;
        private SpaceMonitorHandler(Allocator allocator){
            _allocator = allocator;
        }
        private void setAllocationSpace(long allocSpace){
            _allocationSpace = allocSpace;
        }
        private void setInitialSpace(long space){
            _spaceAllocated = space;
            _spaceUsed      = space;
            _initialSpace   = space;
        }
        public String toString(){
            return "{a="+_spaceAllocated+";u="+_spaceUsed+"}";
        }
        private void getSpace(long newEof) throws InterruptedException{
            if (_allocator == null)return;

            while(newEof > _spaceAllocated){
                _status = "WaitingForSpace("+_allocationSpace+")";
                debug("Allocating new space : "+_allocationSpace);
                _logSpaceAllocation.debug("ALLOC: " + _pnfsId + " : " + _allocationSpace);
                _allocator.allocate(_allocationSpace);
                _spaceAllocated += _allocationSpace;
                debug("Allocated new space : "+_allocationSpace);
                _status = "";
            }
        }
        private void newFilePosition(long newPosition){
            _spaceUsed = Math.max(newPosition, _spaceUsed);
        }
        private boolean fileChanged(){
            return _initialSpace != _spaceUsed;
        }
        private void close(long realFileSize)
            throws IllegalStateException,
                   InterruptedException {

        }
    }
    //
    //   helper class to use nio channels for input requests.
    //
    private class RequestBlock {

        private ByteBuffer _buffer = null;
        private int _commandSize = 0;
        private int _commandCode = 0;

        private RequestBlock(){
            _buffer = ByteBuffer.allocate(16384);
        }
        private void read(SocketChannel channel) throws Exception {

            _commandSize = _commandCode = 0;

            _buffer.clear().limit(4);
            fillBuffer(channel);
            _buffer.rewind();
            _commandSize = _buffer.getInt();

            if(_commandSize < 4)
                throw new
                    CacheException(44,"Protocol Violation (cl<4)");

            try {
        	_buffer.clear().limit(_commandSize);
            }catch(IllegalArgumentException iae) {
        	esay("Command size excided command block size : " + _commandSize + "/" + _buffer.capacity());
        	throw iae;
            }
            fillBuffer(channel);
            _buffer.rewind();
            _commandCode = _buffer.getInt();
        }
        private int remaining(){ return _buffer.remaining(); }
        private int getCommandSize(){ return _commandSize; }
        private int getCommandCode(){ return _commandCode; }
        private int nextInt(){ return _buffer.getInt(); }
        private long nextLong(){ return _buffer.getLong(); }
        private void fillBuffer( SocketChannel channel) throws Exception{
            while(_buffer.position() < _buffer.limit()){
                if(channel.read(_buffer) < 0)
                    throw new
                        EOFException("EOF on input socket (fillBuffer)");
            }
        }
        private void skip(int skip){
            _buffer.position(_buffer.position()+skip);
        }
        private void get(byte [] array){ _buffer.get(array); }
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
        say("DCapProtocol_3 (nio) created $Id: DCapProtocol_3_nio.java,v 1.17 2007-10-02 13:35:52 tigran Exp $");
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
        say("Setup : Defaults Buffer Sizes  : "+_defaultBufferSize);
        say("Setup : Max Buffer Sizes       : "+_maxBufferSize);

        String debugEnabled = (String)_context.get("dCap3-debug");
        _debug = (debugEnabled != null) &&  ! debugEnabled.equals("");

    }
    private synchronized int getParameterInt(String name, int defaultValue){
        String stringValue = (String)_context.get("dCap3-"+name);
        stringValue = stringValue == null ? (String)_args.getOpt(name) : stringValue;
        try{
            return stringValue == null ? defaultValue : Integer.parseInt(stringValue);
        }catch(NumberFormatException e){
            return defaultValue;
        }
    }
    private synchronized boolean getParameterBoolean(String name, boolean defaultValue){
        String stringValue = (String)_context.get("dCap3-"+name);
        stringValue = stringValue == null ? (String)_args.getOpt(name) : stringValue;
        return stringValue == null ? defaultValue :
            stringValue.equals("")     ? true  :
            stringValue.equals("true") ? true  :
            stringValue.equals("on")   ? true  :
            stringValue.equals("yes")  ? true  : false;
    }
    private void debug(String str){
        _log.debug("["+_pnfsId+":"+_sessionId+"] "+str);
    }
    private void say(String str){
        _log.info(str);
    }
    private void esay(String str){
        _log.error(str);
    }
    private void esay(Exception e){
        _log.error(e);
    }
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

    public void runIO(RandomAccessFile  diskFile,
                      ProtocolInfo protocol,
                      StorageInfo  storage,
                      PnfsId       pnfsId,
                      Allocator    allocator,
                      int          access  )

        throws Exception {

        Exception ioException         = null;

        if(! (protocol instanceof DCapProtocolInfo))
            throw new
                CacheException(44, "protocol info not DCapProtocolInfo");

        _pnfsId              = pnfsId;
        _spaceMonitorHandler = new SpaceMonitorHandler(allocator);

        ////////////////////////////////////////////////////////////////////////
        //                                                                    //
        //    Prepare the tunable parameters                                  //
        //                                                                    //
        try{
            String crash = storage.getKey("crash");
            if(crash != null){
                _crash     = crash.length() == 0 ? 654321L : Long.parseLong(crash);
                _crashType = storage.getKey("crashType");
                say("Options : crash = "+crash+"; type = "+ _crashType);
            }
        }catch(NumberFormatException e){ /* bad values are ignored */}
        say("crash       = "+_crash);
        say("crashType   = "+_crashType);

        try{
            String allocation = storage.getKey("alloc-size");
            if(allocation != null){
                long allocSpace = Long.parseLong(allocation);
                if(allocSpace <= 0) {
                    // negative allocation requested....Ignoring
                    say("Options : alloc-space = "+allocSpace + "....Ignoring");
                }else{
                    _spaceMonitorHandler.setAllocationSpace(allocSpace);
                    say("Options : alloc-space = "+allocSpace);
                }
            }
        }catch(NumberFormatException e){ /* bad values are ignored */}

        try{
            String debug = storage.getKey("debug");
            if(debug != null){
                say("Options : debug = "+debug);
                if(debug.length() == 0)_debug = true;
                else{
                    _debug =  Integer.parseInt(debug) > 0;
                }
            }
        }catch(NumberFormatException e){ /* bad values are ignored */}

        say("debug = "+_debug);

        try{
            String io = storage.getKey("io-error");
            if(io != null)_ioError = Long.parseLong(io);
        }catch(NumberFormatException e){ /* bad values are ignored */}
        say("ioError = "+_ioError);


        MoverIoBuffer bufferSize = new MoverIoBuffer(_defaultBufferSize);

        {
            String tmp     = null;

            try{
                tmp = storage.getKey("send");
                if(tmp != null)
                    bufferSize.setSendBufferSize(
                                                 Math.min(Integer.parseInt(tmp),_maxBufferSize.getSendBufferSize())
                                                );
            }catch(NumberFormatException e){ /* bad values are ignored */}
            try{
                tmp = storage.getKey("receive");
                if(tmp != null)
                    bufferSize.setRecvBufferSize(
                                                 Math.min(Integer.parseInt(tmp),_maxBufferSize.getRecvBufferSize())
                                                );
            }catch(NumberFormatException e){ /* bad values are ignored */}
            try{
                tmp = storage.getKey("bsize");
                if(tmp != null)
                    bufferSize.setIoBufferSize(
                                               Math.min(Integer.parseInt(tmp),_maxBufferSize.getIoBufferSize())
                                              );
            }catch(NumberFormatException e){ /* bad values are ignored */}

        }

        say("Client : Buffer Sizes : "+bufferSize);
        //                                                                    //
        //                                                                    //
        ////////////////////////////////////////////////////////////////////////
        //                                                                    //
        //      get a buffer                                                  //
        //                                                                    //

        try{
            _bigBuffer =
                _bigBuffer == null ?
                ByteBuffer.allocateDirect(bufferSize.getIoBufferSize()) :
                _bigBuffer;
        }catch(OutOfMemoryError om){
            _bigBuffer = ByteBuffer.allocateDirect(32*1024);
        }


        DCapProtocolInfo dcap = (DCapProtocolInfo)protocol;

        SocketChannel socketChannel = null;
        FileChannel   fileChannel   = diskFile.getChannel();
        DCapOutputByteBuffer cntOut = new DCapOutputByteBuffer(1024);

        _sessionId  = dcap.getSessionId();

        if(! dcap.isPassive()) {
            int        port       = dcap.getPort();
            String []  hosts      = dcap.getHosts();
            String     host       = null;
            Exception  se         = null;

            //
            // try to connect to the client, scan the list.
            //
            for(int i  = 0; i < hosts.length; i++){
                try{
                    host = hosts[i];

                    socketChannel = SocketChannel.open(
                                                       new InetSocketAddress(
                                                                             InetAddress.getByName(host),
                                                                             port        )
                                                      );

                    socketChannel.configureBlocking(true);

                }catch(Exception ee){
                    esay("Can't connect to "+host);
                    se = ee;
                    continue;
                }
                break;
            }
            if(socketChannel == null)throw se;

            {
                Socket dataSocket = socketChannel.socket();
              	if(_logSocketIO.isDebugEnabled()) {
                    _logSocketIO.debug("Socket OPEN remote = " + dataSocket.getInetAddress() + ":" + dataSocket.getPort() +
                                       " local = " + dataSocket.getLocalAddress() + ":" + dataSocket.getLocalPort());
            	}
                dataSocket.setReceiveBufferSize(bufferSize.getRecvBufferSize());
                dataSocket.setSendBufferSize(bufferSize.getSendBufferSize());

                say("Using : Buffer Sizes (send/recv/io) : "+
                    dataSocket.getSendBufferSize()+"/"+
                    dataSocket.getReceiveBufferSize()+"/"+
                    _bigBuffer.capacity());

            }

            say("Connected to "+host+"("+port+")");
            //
            // send the sessionId and our (for now) 0 byte security challenge.
            //
            _bigBuffer.clear();
            _bigBuffer.putInt(_sessionId).putInt(0);
            _bigBuffer.limit(_bigBuffer.position()).position(0);
            socketChannel.write(_bigBuffer);
        }else{ // passive connection

            ProtocolConnectionPool pcp = protocolConnectionPoolFactory.getConnectionPool();

            InetSocketAddress socketAddress =
                new  InetSocketAddress(InetAddress.getLocalHost(),
                                       pcp.getLocalPort());

            byte[] challenge = UUID.randomUUID().toString().getBytes();
            PoolPassiveIoFileMessage msg = new PoolPassiveIoFileMessage("pool", socketAddress, challenge);
            msg.setId(dcap.getSessionId());
            say("waiting for client to connect ("+
                 InetAddress.getLocalHost()  +
                 pcp.getLocalPort() +
                 ")");

            CellPath cellpath = dcap.door();
            _cell.sendMessage (new CellMessage(cellpath, msg));
            DCapProrocolChallenge dcapChallenge = new DCapProrocolChallenge(_sessionId, challenge);
            socketChannel = pcp.getSocket(dcapChallenge);

        }

        //
        //
        _transferStarted  = System.currentTimeMillis();
        _bytesTransferred = 0;
        _lastTransferred  = _transferStarted;

        _spaceMonitorHandler.setInitialSpace(diskFile.length());

        boolean      notDone      = true;
        RequestBlock requestBlock = new RequestBlock();

        try{
            while(notDone && _io_ok){

                if(Thread.interrupted())
                    throw new
                        InterruptedException("Interrupted By Operator");

                //
                // get size of next command
                //
                try{

                    requestBlock.read(socketChannel);

                    debug("Request Block : "+requestBlock);

                }catch(EOFException eofe){
                    esay("Dataconnection closed by peer : "+eofe);
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
                        esay(errmsg);
                        cntOut.writeACK(DCapConstants.IOCMD_WRITE,CacheException.ERROR_IO_DISK,errmsg);
                        socketChannel.write(cntOut.buffer());

                    }else if((access & MoverProtocol.WRITE) != 0){

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
                            esay("Reporting IO problem to client");
                            cntOut.writeFIN(DCapConstants.IOCMD_WRITE,CacheException.ERROR_IO_DISK,
                                            "[2]Problem in writing");
                            socketChannel.write(cntOut.buffer());
                        }

                    }else{

                        String errmsg = "WRITE denied (not allowed)";
                        esay(errmsg);
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
                    _transferChecksum= null;

                    long blockSize = requestBlock.nextLong();

                    debug("READ byte="+blockSize);

                    if(_io_ok){

                        cntOut.writeACK(DCapConstants.IOCMD_READ);
                        socketChannel.write(cntOut.buffer());

                        doTheRead(fileChannel, cntOut, socketChannel,  blockSize);

                        if(_io_ok){
                            cntOut.writeFIN(DCapConstants.IOCMD_READ);
                            socketChannel.write(cntOut.buffer());
                        }else{
                            String errmsg = "FIN : READ failed (IO not ok)";
                            esay(errmsg);
                            cntOut.writeFIN(DCapConstants.IOCMD_READ,CacheException.ERROR_IO_DISK,errmsg);
                            socketChannel.write(cntOut.buffer());
                        }
                    }else{

                        String errmsg = "ACK : READ denied (IO not ok)";
                        esay(errmsg);
                        cntOut.writeACK(DCapConstants.IOCMD_READ,CacheException.ERROR_IO_DISK,errmsg);
                        socketChannel.write(cntOut.buffer());

                    }

                    break;
                    //-------------------------------------------------------------
                    //
                    //                     The Seek
                    //
                case DCapConstants.IOCMD_SEEK :

                    _transferChecksum = null;

                    long offset = requestBlock.nextLong();
                    int  whence = requestBlock.nextInt();

                    doTheSeek(fileChannel , whence, offset,
                               (access & MoverProtocol.WRITE) != 0 );

                    if(_io_ok){

                        cntOut.writeACK(diskFile.getFilePointer());
                        socketChannel.write(cntOut.buffer());

                    }else{

                        String errmsg = "SEEK failed : IOError ";
                        esay(errmsg);
                        cntOut.writeACK(DCapConstants.IOCMD_SEEK,6,errmsg);
                        socketChannel.write(cntOut.buffer());

                    }

                    break;
                    //-------------------------------------------------------------
                    //
                    //                     The IOCMD_SEEK_AND_READ
                    //
                case DCapConstants.IOCMD_SEEK_AND_READ :

                    _transferChecksum = null;

                    offset    = requestBlock.nextLong();
                    whence    = requestBlock.nextInt();
                    blockSize = requestBlock.nextLong();

                    if(_io_ok){

                        cntOut.writeACK(DCapConstants.IOCMD_SEEK_AND_READ);
                        socketChannel.write(cntOut.buffer());

                        doTheSeek(fileChannel, whence, offset,
                                   (access & MoverProtocol.WRITE) != 0 );

                        if(_io_ok)doTheRead(fileChannel, cntOut, socketChannel, blockSize);

                        if(_io_ok){
                            cntOut.writeFIN(DCapConstants.IOCMD_SEEK_AND_READ);
                            socketChannel.write(cntOut.buffer());
                        }else{
                            String errmsg = "FIN : SEEK_READ failed (IO not ok)";
                            esay(errmsg);
                            cntOut.writeFIN(DCapConstants.IOCMD_SEEK_AND_READ,CacheException.ERROR_IO_DISK,errmsg);
                            socketChannel.write(cntOut.buffer());
                        }

                    }else{
                        String errmsg = "SEEK_AND_READ denied : IOError " ;
                        esay(errmsg);
                        cntOut.writeACK(DCapConstants.IOCMD_SEEK_AND_READ,CacheException.ERROR_IO_DISK,errmsg);
                        socketChannel.write(cntOut.buffer());
                    }
                    break;
                    //-------------------------------------------------------------
                    //
                    //                     The IOCMD_SEEK_AND_WRITE
                    //
                case DCapConstants.IOCMD_SEEK_AND_WRITE :

                    _transferChecksum = null;
                    offset    = requestBlock.nextLong();
                    whence    = requestBlock.nextInt();

                    if(_io_ok){

                        cntOut.writeACK(DCapConstants.IOCMD_SEEK_AND_WRITE);
                        socketChannel.write(cntOut.buffer());

                        doTheSeek(fileChannel, whence, offset,
                                   (access & MoverProtocol.WRITE) != 0);

                        if(_io_ok)
                            doTheWrite(fileChannel,
                                        cntOut,
                                        socketChannel );

                        if(_io_ok){
                            cntOut.writeFIN(DCapConstants.IOCMD_SEEK_AND_WRITE);
                            socketChannel.write(cntOut.buffer());
                        }else{
                            String errmsg = "SEEK_AND_WRITE failed : IOError";
                            esay(errmsg);
                            cntOut.writeFIN(DCapConstants.IOCMD_SEEK_AND_WRITE,CacheException.ERROR_IO_DISK,errmsg);
                            socketChannel.write(cntOut.buffer());
                        }

                    }else{
                        String errmsg = "SEEK_AND_WRITE denied : IOError";
                        esay(errmsg);
                        cntOut.writeACK(DCapConstants.IOCMD_SEEK_AND_WRITE,CacheException.ERROR_IO_DISK,errmsg);
                        socketChannel.write(cntOut.buffer());
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
                            esay("Problem in close block "+ee);
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
                        long size     = diskFile.getFilePointer();
                        long location = diskFile.length();
                        debug("LOCATE : size="+size+";position="+location);
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
                                esay(errmsg);
                                cntOut.writeFIN(DCapConstants.IOCMD_READV,CacheException.ERROR_IO_DISK,errmsg);
                                socketChannel.write(cntOut.buffer());
                            }
                        }else{

                            String errmsg = "ACK : READV denied (IO not ok)";
                            esay(errmsg);
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
        }catch(Exception e){
            //
            // this is an error
            //
            esay("Problem in command block : "+requestBlock);
            esay(e);
            ioException = e;

        }finally{

            try{
           	if(_logSocketIO.isDebugEnabled()) {
                    _logSocketIO.debug("Socket CLOSE remote = " + socketChannel.socket().getInetAddress() + ":" + socketChannel.socket().getPort() +
                                       " local = " + socketChannel.socket().getLocalAddress() + ":" + socketChannel.socket().getLocalPort());
           	}

                socketChannel.close();
            }catch(Exception xe){}

            dcap.setBytesTransferred(_bytesTransferred);

            _transferTime = System.currentTimeMillis() -
                _transferStarted;
            dcap.setTransferTime(_transferTime);

            say("(Transfer finished : "+
                 _bytesTransferred+" bytes in "+
                 (_transferTime/1000) +" seconds) ");

            long diskFileSize = diskFile.length();

            try{

                _spaceMonitorHandler.close(diskFileSize);

            }catch(IllegalStateException ise){
                esay("Space monitor detected disk I/O problems : "+ise);
                ioException = ise;
                _io_ok = false;
            }

            //
            // if we got an EOF from the inputstream
            // we cancel the request but we don't want to
            // disable the pool, unless client is gone while
            // got an IO error report from pool.
            //

            if(! _io_ok) {
                throw new
                    CacheException(
                                   CacheException.ERROR_IO_DISK,
                                   "Disk I/O Error " +
                                   (ioException!=null?ioException.toString():"")    );
            }else{
                if (ioException instanceof EOFException)  throw ioException;
            }


        }

    }
    private void doTheReadv(FileChannel fileChannel, DCapOutputByteBuffer cntOut,
                            SocketChannel socketChannel, RequestBlock requestBLock) throws Exception {


        cntOut.writeDATA_HEADER();
        socketChannel.write(cntOut.buffer());

        int blocks = requestBLock.nextInt();
        say("READV: " + blocks + " to read");
        final int maxBuffer = _bigBuffer.capacity() - 4;
        for(int i = 0; i < blocks; i++) {


            long offset = requestBLock.nextLong();
            int count = requestBLock.nextInt();
            int len = count;

            say("READV: offset/len: " + offset +"/" + count);

            while(count > 0) {

                int bytesToRead = maxBuffer > count ? count : maxBuffer;
                int rc;
                try{
                    _bigBuffer.clear().limit(bytesToRead+4);
                    _bigBuffer.position(4);
                    rc = fileChannel.read(_bigBuffer, offset + (len - count));
                    if(rc <= 0)break;
                }catch(IOException ee){
                    _io_ok = false;
                    break;
                }

                _bigBuffer.limit(_bigBuffer.position()).rewind();
                _bigBuffer.putInt(rc).rewind();
                say("READV: sending: " + _bigBuffer.limit() +" bytes");
                socketChannel.write(_bigBuffer);

                count -= rc;
                _bytesTransferred += rc;

            }


        }

        return;

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
            esay("Not a valid block size in close");
            throw new
                IllegalArgumentException("Not a valid block size in close");
        }

        int blockMode = requestBlock.nextInt();
        if(blockMode != 1){ // crc block
            esay("Unknown block mode ("+blockMode+") in close");
            requestBlock.skip(blockSize-4);
            return ;
        }
        int crcType = requestBlock.nextInt();

        byte [] array = new byte[blockSize-8];

        requestBlock.get(array);

        _clientChecksum = new Checksum(crcType, array);
        storage.setKey("flag-c",_clientChecksum.toString());

        return;
    }
    private void doTheSeek(FileChannel fileChannel, int whence, long offset,
                            boolean writeAllowed)
        throws Exception {

        try{
            long eofSize   = fileChannel.size();
            long position  = fileChannel.position();
            long newOffset = 0L;
            switch(whence){

            case DCapConstants.IOCMD_SEEK_SET :

                debug("SEEK "+offset+" SEEK_SET");
                //
                // this should reset the io state
                //
                if(offset == 0L)_io_ok = true;
                //
                newOffset = offset;

                break;

            case DCapConstants.IOCMD_SEEK_CURRENT :

                debug("SEEK "+offset+" SEEK_CURRENT");
                newOffset = position + offset;

                break;
            case DCapConstants.IOCMD_SEEK_END :

                debug("SEEK "+offset+" SEEK_END");
                newOffset = eofSize + offset;

                break;
            default :

                throw new
                    IllegalArgumentException("Invalid seek mode : "+whence);

            }
            if((newOffset > eofSize) && ! writeAllowed)
                throw new
                    IOException("Seek beyond EOF not allowed (write not allowed)");

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
            esay("Problem in seek : "+ee);
        }


    }
    private void doTheWrite(FileChannel          fileChannel,
                             DCapOutputByteBuffer cntOut,
                             SocketChannel        socketChannel ) throws Exception{

        int     rest;
        int     size = 0, rc = 0;

        RequestBlock requestBlock = new RequestBlock();
        requestBlock.read(socketChannel);

        if(requestBlock.getCommandCode() != DCapConstants.IOCMD_DATA)
            throw new
                IOException("Expecting : "+DCapConstants.IOCMD_DATA+"; got : "+requestBlock.getCommandCode());

        while(! Thread.currentThread().isInterrupted()){

            _status = "WaitingForSize";

            _bigBuffer.clear().limit(4);
            while(_bigBuffer.position()<_bigBuffer.limit()){
                if(socketChannel.read(_bigBuffer) < 0)
                    throw new
                        EOFException("EOF on input socket");
            }
            _bigBuffer.rewind();

            rest = _bigBuffer.getInt();
            debug("Next data block : "+rest+" bytes");
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
            if(rest == 0)continue;
            if(rest < 0)break;
            _wasChanged = true;
            while(rest > 0 ){

                size = _bigBuffer.capacity() > rest ?
                    rest : _bigBuffer.capacity();

                _status = "WaitingForInput";

                _bigBuffer.clear().limit(size);

                rc = socketChannel.read(_bigBuffer);

                if(rc <= 0)break;

                if(_io_ok){

                    _status = "WaitingForWrite";

                    try{

                        _bigBuffer.limit(_bigBuffer.position()).rewind();
                        bytesAdded += fileChannel.write(_bigBuffer);
                        updateChecksum(_bigBuffer);

                    }catch(Exception ioe){
                        esay("IOException in writing data to disk : "+ioe);
                        _io_ok = false;
                    }
                }
                rest -= rc;
                _bytesTransferred += rc;
                if((_ioError > 0L) &&
                    (_bytesTransferred > _ioError)){ _io_ok = false; }
            }
            _spaceMonitorHandler.newFilePosition(position + bytesAdded);

            debug("Block Done");
        }
        _status = "Done";

        return;

    }
    private void updateChecksum(ByteBuffer buffer){
        if(_transferChecksum == null)return;
        buffer.rewind();
        _transferChecksum.getMessageDigest().update(buffer);
    }

    private void doTheRead(FileChannel           fileChannel,
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
        int     size = 0, rc = 0 ;

        final int maxBuffer = _bigBuffer.capacity() - 4;

        while(! Thread.currentThread().isInterrupted()){

            size = maxBuffer > rest ? (int)rest : maxBuffer;

            try{
                _bigBuffer.clear().limit(size+4);
                _bigBuffer.position(4);
                rc = fileChannel.read(_bigBuffer);
                if(rc <= 0)break;
            }catch(IOException ee){
                _io_ok = false;
                break;
            }
            _bigBuffer.limit(_bigBuffer.position()).rewind();
            _bigBuffer.putInt(rc).rewind();
            socketChannel.write(_bigBuffer);
            rest -= rc;
            _bytesTransferred += rc;
            if((_ioError > 0L) && (_bytesTransferred > _ioError)){
                _io_ok = false;
                break;
            }
            if(rest <= 0)break;
        }
        //
        // data chain delimiter
        //
        cntOut.writeDATA_TRAILER();
        socketChannel.write(cntOut.buffer());

        return;
    }
    public long getLastTransferred() { return _lastTransferred; }
    public long getBytesTransferred(){ return _bytesTransferred ; }
    public long getTransferTime(){
        return _transferTime < 0 ?
            System.currentTimeMillis() - _transferStarted :
            _transferTime ;
    }
    //
    //   attributes
    //
    public synchronized void setAttribute(String name, Object attribute){
        if(name.equals("allocationSpace")){
            _spaceMonitorHandler.setAllocationSpace(
                                                    Integer.parseInt(attribute.toString())
                                                   );
        }
    }
    public synchronized Object getAttribute(String name){
        throw new
            IllegalArgumentException("Couldn't find "+name);
    }
    public boolean wasChanged(){ return _wasChanged; }

    public ChecksumFactory getChecksumFactory(ProtocolInfo protocolInfo) { return null; }

    public void  setDigest(Checksum checksum){
        _transferChecksum      =  checksum;
    }
    public Checksum getClientChecksum(){
        return  _clientChecksum ;
    }
    public Checksum getTransferChecksum(){
        return _transferChecksum;
    }

}

// $Id: RemoteGsiftpTransferProtocol_1.java,v 1.12 2007-10-08 20:43:29 abaranov Exp $

/*
  COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


  Distribution of the software available from this server is free of
  charge subject to the user following the terms of the Fermitools
  Software Legal Information.

  Redistribution and/or modification of the software shall be accompanied
  by the Fermitools Software Legal Information  (including the copyright
  notice).

  The user is asked to feed back problems, benefits, and/or suggestions
  about the software to the Fermilab Software Providers.


  Neither the name of Fermilab, the  URA, nor the names of the contributors
  may be used to endorse or promote products derived from this software
  without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
*/

package org.dcache.pool.movers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.nio.ByteBuffer;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfos;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpTransferProtocolInfo;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;

import org.dcache.ftp.client.Buffer;
import org.dcache.ftp.client.exception.ClientException;
import org.dcache.ftp.client.exception.ServerException;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.srm.util.GridftpClient;
import org.dcache.srm.util.GridftpClient.IDiskDataSourceSink;
import org.dcache.ssl.SslContextFactory;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.PortRange;
import org.dcache.util.URIs;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.util.Exceptions.messageOrClassName;

public class RemoteGsiftpTransferProtocol
    implements MoverProtocol,ChecksumMover,DataBlocksRecipient
{
    private static final Logger LOGGER =
        LoggerFactory.getLogger(RemoteGsiftpTransferProtocol.class);
    //timeout after 5 minutes if credentials not delegated
    private static final int SERVER_SOCKET_TIMEOUT = 60 * 5 *1000;

    private static final CellPath PNFS_MANAGER =
        new CellPath("PnfsManager");

    private final CellEndpoint _cell;
    private long _starttime;
    private long _timeout_time;
    private PnfsId _pnfsId;

    private RepositoryChannel _fileChannel;
    private GridftpClient _client;
    private Consumer<Checksum> _integrityChecker;

    static {
        ChecksumType[] types = ChecksumType.values();
        String[] names = new String[types.length];
        for (int i = 0; i < types.length; i++) {
            names[i] = types[i].getName();
        }

        // set checksum type set for further cksm negotiations
        GridftpClient.setSupportedChecksumTypes(names);
    }

    private final PortRange _portRange;
    private final String[] _bannedCiphers;
    private final SslContextFactory _sslContextFactory;

    public RemoteGsiftpTransferProtocol(CellEndpoint cell, PortRange portRange, String[] bannedCiphers,
                                        SslContextFactory sslContextFactory)
    {
        _cell = cell;
        _portRange = portRange;
        _bannedCiphers = bannedCiphers;
        _sslContextFactory = sslContextFactory;
    }

    private void createFtpClient(RemoteGsiftpTransferProtocolInfo protocolInfo)
            throws ServerException, ClientException, IOException, KeyStoreException, URISyntaxException
    {
        if (_client != null) {
            return;
        }

        URI url = new URI(protocolInfo.getGsiftpUrl());
        _client = new GridftpClient(url.getHost(), URIs.portWithDefault(url), 0, _portRange, protocolInfo.getCredential(),
                                    _bannedCiphers, _sslContextFactory);
        _client.setStreamsNum(protocolInfo.getNumberOfStreams());
    }

    @Override
    public void acceptIntegrityChecker(Consumer<Checksum> integrityChecker)
    {
        _integrityChecker = integrityChecker;
    }

    @Override
    public void runIO(FileAttributes fileAttributes,
                      RepositoryChannel fileChannel,
                      ProtocolInfo protocol,
                      Set<? extends OpenOption> access)
            throws CacheException, IOException,
            ServerException, ClientException, KeyStoreException, URISyntaxException
    {
        _pnfsId = fileAttributes.getPnfsId();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("runIO()\n\tprotocol={},\n\tStorageInfo={},\n\tPnfsId={},\n\taccess ={}",
                    protocol, StorageInfos.extractFrom(fileAttributes), _pnfsId, access );
        }
        if (!(protocol instanceof RemoteGsiftpTransferProtocolInfo)) {
            throw new CacheException("protocol info is not RemoteGsiftpransferProtocolInfo");
        }
        _fileChannel = fileChannel;
        _starttime = System.currentTimeMillis();

        RemoteGsiftpTransferProtocolInfo remoteGsiftpProtocolInfo
            = (RemoteGsiftpTransferProtocolInfo) protocol;

        fileChannel.optionallyAs(ChecksumChannel.class).ifPresent(c -> {
                    remoteGsiftpProtocolInfo.getDesiredChecksum().ifPresent(t -> {
                                try {
                                    c.addType(t);
                                } catch (IOException e) {
                                    LOGGER.warn("Unable to calculate checksum {}: {}",
                                            t, messageOrClassName(e));
                                }
                            });
                });

        createFtpClient(remoteGsiftpProtocolInfo);

        if (access.contains(StandardOpenOption.WRITE)) {
            discoverRemoteChecksum(remoteGsiftpProtocolInfo).ifPresent(_integrityChecker);
            gridFTPRead(remoteGsiftpProtocolInfo);
        } else {
            gridFTPWrite(remoteGsiftpProtocolInfo);
        }
        LOGGER.debug(" runIO() done");
    }

    @Override
    public long getLastTransferred()
    {
        return (_client == null ? 0 : _client.getLastTransferTime());
    }

    private synchronized void setTimeoutTime(long t)
    {
        _timeout_time = t;
    }

    private synchronized long getTimeoutTime()
    {
        return _timeout_time;
    }

    @Override
    public long getBytesTransferred()
    {
        return (_client == null ? 0 : _client.getTransfered());
    }

    @Override
    public long getTransferTime()
    {
        return System.currentTimeMillis() - _starttime;
    }

    public void gridFTPRead(RemoteGsiftpTransferProtocolInfo protocolInfo)
        throws CacheException
    {
        try {
            URI src_url = new URI(protocolInfo.getGsiftpUrl());
            boolean emode = protocolInfo.isEmode();
            DiskDataSourceSink sink =
                new DiskDataSourceSink(protocolInfo.getBufferSize(),
                                       false);
            try {
                _client.gridFTPRead(src_url.getPath(),sink, emode);
            } finally {
                _client.close();
            }
        } catch (Exception e) {
            throw new CacheException(e.toString());
        }
    }

    public void gridFTPWrite(RemoteGsiftpTransferProtocolInfo protocolInfo)
        throws CacheException
    {
        LOGGER.debug("gridFTPWrite started");

        try {
            PnfsHandler pnfs = new PnfsHandler(_cell, PNFS_MANAGER);
            FileAttributes attributes =
                pnfs.getFileAttributes(_pnfsId, EnumSet.of(FileAttribute.CHECKSUM));
            Set<Checksum> checksums = attributes.getChecksums();

            if (!checksums.isEmpty()){
                Checksum checksum = checksums.iterator().next();
                LOGGER.debug("Will use {} for transfer verification of {}", checksum, _pnfsId);
                _client.setChecksum(checksum.getType().getName(), null);
            } else {
                LOGGER.debug("PnfsId {} does not have checksums", _pnfsId);
            }

            URI dst_url =  new URI(protocolInfo.getGsiftpUrl());
            boolean emode = protocolInfo.isEmode();

            try {
                DiskDataSourceSink source =
                    new DiskDataSourceSink(protocolInfo.getBufferSize(),
                                           true);
                _client.gridFTPWrite(source, dst_url.getPath(), emode,  true);
            } finally {
                _client.close();
            }
        } catch (Exception e) {
            throw new CacheException(e.toString());
        }
    }

    private Optional<Checksum> discoverRemoteChecksum(RemoteGsiftpTransferProtocolInfo info)
    {
        try {
            String path = new URI(info.getGsiftpUrl()).getPath();
            GridftpClient.Checksum checksum = _client.negotiateCksm(path);
            return Optional.of(new Checksum(ChecksumType.valueOf(checksum.type), checksum.value));
        } catch (GridftpClient.ChecksumNotSupported e) {
            LOGGER.error("Checksum algorithm is not supported: {}", e.getMessage());
        } catch (IOException e) {
            LOGGER.error("I/O failure talking to FTP server: {}", messageOrClassName(e));
        } catch (ServerException e) {
            LOGGER.error("GridFTP server failure: {}", e.getMessage());
        } catch (URISyntaxException e) {
            LOGGER.error("Invalid GridFTP URL: {}", e.getMessage());
        } catch (ClientException e) {
            LOGGER.error("GridFTP client failure: {}", e.getMessage());
        }
        return Optional.empty();
    }

    @Override
    public synchronized void receiveEBlock(byte[] array,
                                           int offset,
                                           int length,
                                           long offsetOfArrayInFile)
        throws IOException
    {
        if (array == null) {
            /* REVISIT: Why do we need this?
             */
            return;
        }

        ByteBuffer bb = ByteBuffer.wrap(array, offset, length);
        _fileChannel.write(bb, offsetOfArrayInFile);
    }

    private class DiskDataSourceSink implements IDiskDataSourceSink
    {
        private final int _buf_size;
        private final boolean _source;
        private long _last_transfer_time = System.currentTimeMillis();
        private long _transferred;

        public DiskDataSourceSink(int buf_size, boolean source)
        {
            _buf_size = buf_size;
            _source = source;
        }

        @Override
        public synchronized void write(Buffer buffer) throws IOException
        {
            if (_source) {
                String error = "DiskDataSourceSink is source and write is called";
                LOGGER.error(error);
                throw new IllegalStateException(error);
            }

            _last_transfer_time = System.currentTimeMillis();
            int read = buffer.getLength();
            long offset = buffer.getOffset();
            if (offset >= 0) {
                receiveEBlock(buffer.getBuffer(),
                              0, read,
                              buffer.getOffset());
            } else {
                //this is the case when offset is not supported
                // for example reading from a stream
                receiveEBlock(buffer.getBuffer(),
                              0, read,
                              _transferred);

            }
            _transferred += read;
        }

        @Override
        public synchronized void close()
        {
            LOGGER.debug("DiskDataSink.close() called");
            _last_transfer_time    = System.currentTimeMillis();
        }

        /** Specified in org.globus.ftp.DataSource. */
        @Override
        public long totalSize() throws IOException
        {
            return _source ? _fileChannel.size() : -1;
        }

        /** Getter for property last_transfer_time.
         * @return Value of property last_transfer_time.
         *
         */
        @Override
        public synchronized long getLast_transfer_time()
        {
            return _last_transfer_time;
        }

        /** Getter for property transferred.
         * @return Value of property transferred.
         *
         */
        @Override
        public synchronized long getTransfered()
        {
            return _transferred;
        }

        @Override
        public synchronized Buffer read() throws IOException
        {
            if (!_source) {
                String error = "DiskDataSourceSink is sink and read is called";
                LOGGER.error(error);
                throw new IllegalStateException(error);
            }

            _last_transfer_time = System.currentTimeMillis();

            byte[] bytes = new byte[_buf_size];
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            int read = _fileChannel.read(bb);
            if (read == -1) {
                return null;
            }
            Buffer buffer = new Buffer(bytes, read, _transferred);
            _transferred  += read;
            return buffer;
        }

        @Override
        public String getCksmValue(String type)
                throws IOException,NoSuchAlgorithmException
        {
            try {
                PnfsHandler pnfs = new PnfsHandler(_cell, PNFS_MANAGER);
                FileAttributes attributes =
                        pnfs.getFileAttributes(_pnfsId, EnumSet.of(FileAttribute.CHECKSUM));

                ChecksumType t = ChecksumType.getChecksumType(type);
                Optional<String> checksum = attributes.getChecksums().stream()
                        .filter(c -> c.getType() == t)
                        .map(Checksum::getValue)
                        .findFirst();

                if (checksum.isPresent()) {
                    return checksum.get();
                }
            } catch (CacheException e) {
                LOGGER.warn("Failed to fetch checksum information: {}", e.getMessage());
            }

            String value = GridftpClient.getCksmValue(_fileChannel, type);
            _fileChannel.position(0);
            return value;
        }

        @Override
        public long getAdler32() throws IOException
        {
            try {
                String hexValue = getCksmValue("adler32");
                return Long.parseLong(hexValue,16);
            } catch ( NoSuchAlgorithmException ex){
                throw new IOException("adler 32 is not supported:"+ ex.toString());
            }
        }

        @Override
        public long length() throws IOException
        {
            return _fileChannel.size();
        }
    }
}

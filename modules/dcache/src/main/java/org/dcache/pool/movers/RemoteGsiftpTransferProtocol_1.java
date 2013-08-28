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

import org.dcache.pool.repository.Allocator;
import diskCacheV111.util.CacheException;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpTransferProtocolInfo;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.EnumSet;
import java.util.Set;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.srm.util.GridftpClient.IDiskDataSourceSink;
import org.dcache.srm.util.GridftpClient;
import org.dcache.srm.security.SslGsiSocketFactory;
import org.dcache.util.NetworkUtils;
import org.globus.ftp.Buffer;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.ServerException;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.net.impl.GSIGssSocket;
import org.globus.gsi.CredentialException;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;

public class RemoteGsiftpTransferProtocol_1
    implements MoverProtocol,ChecksumMover,DataBlocksRecipient
{
    private final static org.slf4j.Logger _log =
        org.slf4j.LoggerFactory.getLogger(RemoteGsiftpTransferProtocol_1.class);
    //timeout after 5 minutes if credentials not delegated
    private final static int SERVER_SOCKET_TIMEOUT = 60 * 5 *1000;

    private final static CellPath PNFS_MANAGER =
        new CellPath("PnfsManager");

    private final CellEndpoint _cell;
    private long _starttime;
    private long _timeout_time;
    private PnfsId _pnfsId;

    private ChecksumFactory _checksumFactory;
    private MessageDigest _transferMessageDigest;

    private long _previousUpdateEndOffset = 0;

    private RepositoryChannel _fileChannel;
    private GridftpClient _client;
    private org.dcache.srm.util.GridftpClient.Checksum _ftpCksm;

    static {
        ChecksumType[] types = ChecksumType.values();
        String[] names = new String[types.length];
        for (int i = 0; i < types.length; i++) {
            names[i] = types[i].getName();
        }

        // set checksum type set for further cksm negotiations
        org.dcache.srm.util.GridftpClient.setSupportedChecksumTypes(names);
    }

    public RemoteGsiftpTransferProtocol_1(CellEndpoint cell)
    {
        _cell = cell;
    }

    private void createFtpClient(RemoteGsiftpTransferProtocolInfo protocolInfo)
        throws ServerException, ClientException,
               CredentialException, GSSException, IOException
    {
        if (_client != null)
            return;

        GlobusURL url = new GlobusURL(protocolInfo.getGsiftpUrl());
        _client = new GridftpClient(url.getHost(), url.getPort(),
                                    protocolInfo.getTcpBufferSize(),
                                    protocolInfo.getCredential());
        _client.setStreamsNum(protocolInfo.getNumberOfStreams());
        _client.setTcpBufferSize(protocolInfo.getTcpBufferSize());
    }

    public void runIO(RepositoryChannel fileChannel,
                      ProtocolInfo protocol,
                      StorageInfo storage,
                      PnfsId pnfsId,
                      Allocator allocator,
                      IoMode access)
        throws CacheException, IOException,
               NoRouteToCellException,
               ServerException, ClientException,
               CredentialException, GSSException
    {
        _pnfsId = pnfsId;
        _log.debug("runIO()\n\tprotocol="
            + protocol + ",\n\tStorageInfo=" + storage + ",\n\tPnfsId="
            + pnfsId + ",\n\taccess ="
            + access );
        if (!(protocol instanceof RemoteGsiftpTransferProtocolInfo)) {
            throw new CacheException("protocol info is not RemoteGsiftpransferProtocolInfo");
        }
        _fileChannel = fileChannel;
        _starttime = System.currentTimeMillis();

        RemoteGsiftpTransferProtocolInfo remoteGsiftpProtocolInfo
            = (RemoteGsiftpTransferProtocolInfo) protocol;

        createFtpClient(remoteGsiftpProtocolInfo);

        if ( access == IoMode.WRITE) {
            gridFTPRead(remoteGsiftpProtocolInfo,
                        storage,
                        allocator);
        } else {
            gridFTPWrite(remoteGsiftpProtocolInfo,
                         storage);
        }
        _log.debug(" runIO() done");
    }

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

    public long getBytesTransferred()
    {
        return (_client == null ? 0 : _client.getTransfered());
    }

    public long getTransferTime()
    {
        return System.currentTimeMillis() - _starttime;
    }

    public boolean wasChanged()
    {
        return _client == null;
    }

    public void gridFTPRead(RemoteGsiftpTransferProtocolInfo protocolInfo,
                            StorageInfo storage,
                            Allocator allocator)
        throws CacheException
    {
        try {
            GlobusURL src_url = new GlobusURL(protocolInfo.getGsiftpUrl());
            boolean emode = protocolInfo.isEmode();
            long size = _client.getSize(src_url.getPath());
            _log.debug(" received a file size info: " + size +
                " allocating space on the pool");
            _log.debug("ALLOC: " + _pnfsId + " : " + size );
            allocator.allocate(size);
            _log.debug(" allocated space " + size);
            DiskDataSourceSink sink =
                new DiskDataSourceSink(protocolInfo.getBufferSize(),
                                       false);
            try {
                _client.gridFTPRead(src_url.getPath(),sink, emode);
            } finally {
                try {
                    _client.close();
                } catch (IOException e) {
                    /* JGlobus is not happy when pre-1.8.0-14 dCaches
                     * send an empty line at the end of the
                     * session. Therefore we ignore this exception.
                     */
                }
            }
        } catch (Exception e) {
            _log.error(e.toString());
            throw new CacheException(e.toString());
        }
    }

    public void gridFTPWrite(RemoteGsiftpTransferProtocolInfo protocolInfo,
                             StorageInfo storage)
        throws CacheException
    {
        _log.debug("gridFTPWrite started");

        try {
            PnfsHandler pnfs = new PnfsHandler(_cell, PNFS_MANAGER);
            FileAttributes attributes =
                pnfs.getFileAttributes(_pnfsId, EnumSet.of(FileAttribute.CHECKSUM));
            Set<Checksum> checksums = attributes.getChecksums();

            if (!checksums.isEmpty()){
                Checksum checksum = checksums.iterator().next();
                _log.debug("Will use " + checksum + " for transfer verification of "+_pnfsId);
                _client.setChecksum(checksum.getType().getName(), null);
            } else {
                _log.debug("PnfsId "+_pnfsId+" does not have checksums");
            }

            GlobusURL dst_url =  new GlobusURL(protocolInfo.getGsiftpUrl());
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
            _log.error(e.toString());
            throw new CacheException(e.toString());
        }
    }

    public Checksum getClientChecksum()
    {
        try {
            if (_ftpCksm != null ){
                return ChecksumFactory.getFactory(ChecksumType.getChecksumType(_ftpCksm.type)).create(_ftpCksm.value);
            }
        } catch (NoSuchAlgorithmException e) {
            _log.error("Checksum algorithm is not supported: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            _log.error("Checksum algorithm is not supported: " + e.getMessage());
        }
        return null;
    }

    public Checksum getTransferChecksum()
    {
        try {
            if (_transferMessageDigest == null) {
                return null;
            }

            ByteBuffer buffer = ByteBuffer.allocate(128*1024);
            _fileChannel.position(_previousUpdateEndOffset);
            while (_fileChannel.read(buffer) >= 0) {
                buffer.flip();
                _transferMessageDigest.update(buffer);
                buffer.clear();
            }

            return _checksumFactory.create(_transferMessageDigest.digest());
        } catch (IOException e) {
            _log.error(e.toString());
            return null;
        }
    }

    public ChecksumFactory getChecksumFactory(ProtocolInfo protocol)
    {
        if (protocol instanceof RemoteGsiftpTransferProtocolInfo) {

            RemoteGsiftpTransferProtocolInfo remoteGsiftpProtocolInfo =  (RemoteGsiftpTransferProtocolInfo) protocol;
            try {
                createFtpClient(remoteGsiftpProtocolInfo);
                GlobusURL src_url =  new GlobusURL(remoteGsiftpProtocolInfo.getGsiftpUrl());
                _ftpCksm = _client.negotiateCksm(src_url.getPath());
                return ChecksumFactory.getFactory(ChecksumType.getChecksumType(_ftpCksm.type));
            } catch (NoSuchAlgorithmException e) {
                _log.error("Checksum algorithm is not supported: " + e.getMessage());
            } catch (IllegalArgumentException e) {
                _log.error("Checksum algorithm is not supported: " + e.getMessage());
            } catch (CredentialException e) {
                _log.error("Failed to authenticate with FTP server: " + e.getMessage());
            } catch (GSSException e) {
                _log.error("Failed to authenticate with FTP server: " + e.getMessage());
            } catch (IOException e) {
                _log.error("I/O failure talking to FTP server: " + e.getMessage());
            } catch (Exception e) {
                _log.error("Failed to negotiate checksum with FTP server: " + e.getMessage());
            }
        }
        return null;
    }

    @Override
    public void setDigest(ChecksumFactory factory)
    {
        _checksumFactory = factory;
        _transferMessageDigest =
            (factory != null) ? factory.create() : null;
    }

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
        if (_transferMessageDigest != null
            && _previousUpdateEndOffset == offsetOfArrayInFile) {
            _previousUpdateEndOffset += length;
            _transferMessageDigest.update(array, offset, length);
        }
    }

    private class DiskDataSourceSink implements IDiskDataSourceSink
    {
        private final int _buf_size;
        private final boolean _source;
        private long _last_transfer_time = System.currentTimeMillis();
        private long _transferred = 0;

        public DiskDataSourceSink(int buf_size, boolean source)
        {
            _buf_size = buf_size;
            _source = source;
        }

        public synchronized void write(Buffer buffer) throws IOException
        {
            if (_source) {
                String error = "DiskDataSourceSink is source and write is called";
                _log.error(error);
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

        public synchronized void close()
        {
            _log.debug("DiskDataSink.close() called");
            _last_transfer_time    = System.currentTimeMillis();
        }

        /** Specified in org.globus.ftp.DataSource. */
        public long totalSize() throws IOException
        {
            return _source ? _fileChannel.size() : -1;
        }

        /** Getter for property last_transfer_time.
         * @return Value of property last_transfer_time.
         *
         */
        public synchronized long getLast_transfer_time()
        {
            return _last_transfer_time;
        }

        /** Getter for property transferred.
         * @return Value of property transferred.
         *
         */
        public synchronized long getTransfered()
        {
            return _transferred;
        }

        public synchronized Buffer read() throws IOException
        {
            if (!_source) {
                String error = "DiskDataSourceSink is sink and read is called";
                _log.error(error);
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

        public String getCksmValue(String type)
            throws IOException,NoSuchAlgorithmException
        {
            try {
                PnfsHandler pnfs = new PnfsHandler(_cell, PNFS_MANAGER);
                FileAttributes attributes =
                    pnfs.getFileAttributes(_pnfsId, EnumSet.of(FileAttribute.CHECKSUM));
                Checksum pnfsChecksum =
                    ChecksumFactory.getFactory(ChecksumType.getChecksumType(type)).find(attributes.getChecksums());
                if ( pnfsChecksum != null ){
                    String hexValue = pnfsChecksum.getValue();
                    _log.debug(type+" read from pnfs for file "+_pnfsId+" is "+hexValue);
                    return hexValue;
                }
            }
            catch(Exception e){
                _log.error("could not get "+type+" from pnfs:");
                _log.error(e.toString());
                _log.error("ignoring this error");

            }

            String hexValue = org.dcache.srm.util.GridftpClient.getCksmValue(_fileChannel,type);
            _fileChannel.position(0);
            return hexValue;
        }

        public long getAdler32() throws IOException
        {
            try {
                String hexValue = getCksmValue("adler32");
                return Long.parseLong(hexValue,16);
            } catch ( java.security.NoSuchAlgorithmException ex){
                throw new IOException("adler 32 is not supported:"+ ex.toString());
            }
        }

        public long length() throws IOException
        {
            return _fileChannel.size();
        }
    }
}

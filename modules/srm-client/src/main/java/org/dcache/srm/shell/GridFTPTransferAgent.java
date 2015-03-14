package org.dcache.srm.shell;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import eu.emi.security.authn.x509.X509Credential;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.axis.types.URI;

import org.dcache.ftp.client.Buffer;
import org.dcache.ftp.client.ChecksumAlgorithm;
import org.dcache.ftp.client.DataChannelAuthentication;
import org.dcache.ftp.client.DataSinkStream;
import org.dcache.ftp.client.DataSourceStream;
import org.dcache.ftp.client.GridFTPClient;
import org.dcache.ftp.client.GridFTPSession;
import org.dcache.ftp.client.RetrieveOptions;
import org.dcache.ftp.client.exception.ClientException;
import org.dcache.ftp.client.exception.ServerException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.dcache.dss.ClientGsiEngineDssContextFactory;
import org.dcache.ssl.CanlContextFactory;

import static com.google.common.base.Preconditions.checkArgument;


/**
 * A FileTransferAgent that supports the {@literal gsiftp} protocol.
 */
public class GridFTPTransferAgent extends AbstractFileTransferAgent implements CredentialAware
{
    private static final int MAX_CONCURRENT_TRANSFERS = 10;
    private static final ChecksumAlgorithm ADLER32 = new ChecksumAlgorithm("ADLER32");

    private final ExecutorService _executor = Executors.newFixedThreadPool(MAX_CONCURRENT_TRANSFERS);

    private Entity _dataInitiator = Entity.CLIENT;
    private TransferMode _transferMode = TransferMode.STREAM;
    private ChecksumMode _checksumHandling = ChecksumMode.IF_AVAILABLE;
    private X509Credential _credential;
    private String _caPath = "/etc/grid-security/certificates";
    private CrlCheckingMode _crlChecking = CrlCheckingMode.IF_VALID;
    private NamespaceCheckingMode _namespace = NamespaceCheckingMode.EUGRIDPMA_GLOBUS;
    private OCSPCheckingMode _ocsp = OCSPCheckingMode.IF_AVAILABLE;
    private CanlContextFactory _sslContextFactory;
    private ClientGsiEngineDssContextFactory _dssContextFactory;

    enum TransferMode {
        EMODE, STREAM;
    }

    enum Entity {
        CLIENT, SERVER;
    }

    enum ChecksumMode {
        IF_AVAILABLE, REQUIRE, IGNORE;
    }

    private void updateCanlContextFactory()
    {
        _sslContextFactory = CanlContextFactory.custom()
                .withCertificateAuthorityPath(_caPath)
                .withCrlCheckingMode(_crlChecking)
                .withNamespaceMode(_namespace)
                .withOcspCheckingMode(_ocsp)
                .withLazy(true)
                .build();
        updateDssContextFactory();
    }

    private void updateDssContextFactory()
    {
        _dssContextFactory = new ClientGsiEngineDssContextFactory(_sslContextFactory,
                _credential, new String[0], true, true);
    }

    @Override
    public void setCredential(X509Credential credential)
    {
        _credential = credential;
    }

    @Override
    public String getTransportName()
    {
        return "gridftp";
    }

    @Override
    public Map<String,String> getOptions()
    {
        ImmutableMap.Builder<String,String> builder = ImmutableMap.builder();

        builder.put("data.connection-initiator", _dataInitiator.name());
        builder.put("data.mode", _transferMode.name());
        builder.put("checksum-verification", _checksumHandling.name());
        builder.put("security.ca-path", _caPath);
        builder.put("security.crl-checking", _crlChecking.name());
        builder.put("security.OCSP", _ocsp.name());
        builder.put("security.ca-namespace", _namespace.name());

        return builder.build();
    }

    @Override
    public void setOption(String key, String value)
    {
        switch (key) {
        case "data.connection-initiator":
            _dataInitiator = Entity.valueOf(value);
            break;

        case "data.mode":
            _transferMode = TransferMode.valueOf(value);
            break;

        case "checksum-verification":
            _checksumHandling = ChecksumMode.valueOf(value);
            break;

        case "security.ca-path":
            File path = new File(value);
            checkArgument(path.isAbsolute(), "Absolute path required");
            checkArgument(path.isDirectory(), "Path is not a directory");
            try {
                _caPath = path.getCanonicalPath();
            } catch (IOException e) {
                throw new IllegalArgumentException("Unable to set path: " + e.getMessage());
            }
            updateCanlContextFactory();
            break;

        case "security.crl-checking":
            _crlChecking = CrlCheckingMode.valueOf(value);
            updateCanlContextFactory();
            break;

        case "security.OCSP":
            _ocsp = OCSPCheckingMode.valueOf(value);
            updateCanlContextFactory();
            break;

        case "security.ca-namespace":
            _namespace = NamespaceCheckingMode.valueOf(value);
            updateCanlContextFactory();
            break;

        default:
            throw new IllegalArgumentException("No such option \"" + key + "\"");
        }
    }

    @Override
    public void start()
    {
        updateCanlContextFactory();
    }

    @Override
    public void close()
    {
        MoreExecutors.shutdownAndAwaitTermination(_executor, 500, TimeUnit.MILLISECONDS);
    }

    @Override
    public Map<String,Integer> getSupportedProtocols()
    {
        return Collections.singletonMap("gsiftp", 100);
    }

    @Override
    public FileTransfer download(URI source, File destination)
    {
        if (source.getScheme().equals("gsiftp")) {
            GridFTPDownload transfer = new GridFTPDownload(source, destination);
            transfer.start();
            return transfer;
        }

        return null;
    }

    @Override
    public FileTransfer upload(File source, URI destination)
    {
        if (destination.getScheme().equals("gsiftp")) {
            GridFTPUpload transfer = new GridFTPUpload(source, destination);
            transfer.start();
            return transfer;
        }

        return null;
    }

    private abstract class GridFTPTransfer extends AbstractFileTransfer
    {
        private final Entity dataInitiator = GridFTPTransferAgent.this._dataInitiator;
        private final TransferMode transferMode = GridFTPTransferAgent.this._transferMode;
        private final ChecksumMode checksumHandling = GridFTPTransferAgent.this._checksumHandling;

        private final URI _remote;
        private final File _localFile;

        private long _bytesTransferred;
        private long _size;
        protected GridFTPClient _client;
        protected volatile String _status;

        GridFTPTransfer(URI remote, File localFile)
        {
            _remote = remote;
            _localFile = localFile;

            Futures.addCallback(this, new FutureCallback(){
                @Override
                public void onSuccess(Object result)
                {
                }

                @Override
                public void onFailure(Throwable t)
                {
                    if (t instanceof CancellationException) {
                        onCancel();
                    }
                }
            });

        }

        protected void onCancel()
        {
            try {
                if (_client != null) {
                    _client.abort();
                }
                _status = "cancelled";
            } catch (IOException | ServerException e) {
                _status = "cancel failed [" + e.toString() + "]";
            }
        }

        @Override
        public String getStatus()
        {
            return _status;
        }

        protected void incrementBytesTransferred(int increment)
        {
            _bytesTransferred += increment;
        }

        protected long getBytesTransferred()
        {
            return _bytesTransferred;
        }

        protected URI getRemote()
        {
            return _remote;
        }

        protected ChecksumMode getChecksumHandling()
        {
            return checksumHandling;
        }

        private int getPort()
        {
            int port = _remote.getPort();
            return port == -1 ? 2811 : port;
        }

        protected String getRemotePath()
        {
            return _remote.getPath();
        }

        protected File getLocalFile()
        {
            return _localFile;
        }

        protected void setTargetSize(long size)
        {
            _size = size;
        }

        protected long getTargetSize()
        {
            return _size;
        }

        protected String percent()
        {
            return NumberFormat.getPercentInstance().format(((double)_bytesTransferred)/_size);
        }

        protected boolean isPassive()
        {
            return transferMode == TransferMode.STREAM && dataInitiator == Entity.CLIENT;
        }

        protected GridFTPClient buildClient() throws IOException, ServerException, ClientException
        {
            GridFTPClient client = new GridFTPClient(_remote.getHost(), getPort());
            client.setUsageInformation("srmfs", "0.0.1");
            client.setType(GridFTPSession.TYPE_IMAGE);
            client.authenticate(_dssContextFactory);

            if (client.isFeatureSupported("DCAU")) {
                client.setDataChannelAuthentication(DataChannelAuthentication.NONE);
            }

            client.setLocalNoDataChannelAuthentication();

            if (transferMode == TransferMode.EMODE) {
                client.setMode(GridFTPSession.MODE_EBLOCK);
                client.setOptions(new RetrieveOptions(1));
            } else {
                client.setMode(GridFTPSession.MODE_STREAM);

                if (!client.isFeatureSupported("GETPUT")) {
                    switch (dataInitiator) {
                    case CLIENT:
                        client.setPassive();
                        client.setLocalActive();
                        break;
                    case SERVER:
                        client.setLocalPassive();
                        client.setActive();
                        break;
                    }
                }
            }

            return client;
        }

        protected void start()
        {
            _executor.submit(new Runnable(){
                @Override
                public void run()
                {
                    try {
                        _status = "Connecting to FTP server.";
                        _client = buildClient();

                        doTransfer();

                        _status = "Succeeded.";
                        succeeded();
                    } catch (IOException e) {
                        Throwable cause = Throwables.getRootCause(e);
                        _status = "Transfer failed: " + cause.getMessage();
                        failed(cause);
                    } catch (ServerException e) {
                        Throwable cause = Throwables.getRootCause(e);
                        _status = "Transfer failed (server exception): " + cause.getMessage();
                        failed(cause);
                    } catch (ClientException e) {
                        Throwable cause = Throwables.getRootCause(e);
                        _status = "Transfer failed (client exception): " + cause.getMessage();
                        failed(cause);
                    } finally {
                        try {
                            _client.close();
                        } catch (IOException|ServerException e) {
                            //FIXME: we ignore errors sent back when saying BYE.
                        }
                    }
                }
            });
        }

        protected String getRemoteChecksum() throws IOException
        {
            switch (getChecksumHandling()) {
            case REQUIRE:
                try {
                    return _client.checksum(ADLER32, 0, -1, getRemotePath());
                } catch (IOException | ServerException e) {
                    throw new IOException("Unable to fetch remote checksum: " + e.getMessage());
                }

            case IGNORE:
                return null;

            case IF_AVAILABLE:
                try {
                    return _client.checksum(ADLER32, 0, -1, getRemotePath());
                } catch (IOException | ServerException e) {
                    return null;
                }

            default:
                throw new RuntimeException("No further options");
            }
        }


        protected abstract void doTransfer() throws IOException, ClientException, ServerException;
    }

    /**
     * Represents a file being uploaded via GridFTP.
     */
    private class GridFTPUpload extends GridFTPTransfer
    {
        public GridFTPUpload(File source, URI destination)
        {
            super(destination, source);
            setTargetSize(getLocalFile().length());
        }

        @Override
        protected void doTransfer() throws IOException, ClientException, ServerException
        {
            _status = "Preparing for upload.";
            MonitoringFileDataSource source = new MonitoringFileDataSource();

            if (_client.isFeatureSupported("GETPUT")) {
                _client.put2(getRemotePath(), isPassive(), source, null);
            } else {
                _client.put(getRemotePath(), source, null);
            }

            String localChecksum = source.getHash();
            String remoteChecksum = getRemoteChecksum();

            if (remoteChecksum != null && !localChecksum.equals(remoteChecksum)) {
                throw new IOException("checksum mismatch: " + remoteChecksum + " != " + localChecksum);
            }
        }

        @Override
        protected void incrementBytesTransferred(int count)
        {
            super.incrementBytesTransferred(count);
            _status = "Sent " + percent() + " of " + getTargetSize() + " bytes.";
        }

        private class MonitoringFileDataSource extends DataSourceStream
        {
            private final Hasher hasher;
            private HashCode hashcode;

            MonitoringFileDataSource() throws FileNotFoundException
            {
                super(new FileInputStream(getLocalFile()));
                switch (GridFTPUpload.this.getChecksumHandling()) {
                case IF_AVAILABLE:
                case REQUIRE:
                    hasher = Hashing.adler32().newHasher();
                    break;
                case IGNORE:
                    hasher = null;
                    break;
                default:
                    throw new RuntimeException("Unknown ChecksumHandling");
                }
            }

            @Override
            public Buffer read() throws IOException
            {
                Buffer buffer = super.read();
                if (buffer != null) {
                    incrementBytesTransferred(buffer.getLength());

                    if (hasher != null) {
                        hasher.putBytes(buffer.getBuffer(), 0, buffer.getLength());
                    }
                }
                return buffer;
            }

            @Override
            public void close() throws IOException
            {
                if (hashcode != null) {
                    throw new IllegalStateException("Attempt to close already closed DataSource");
                }
                if (hasher != null) {
                    hashcode = hasher.hash();
                }
                super.close();
            }

            public String getHash()
            {
                if (hashcode == null) {
                    return null;
                }

                // We cannot use HashCode#toString as that returns a little-endian hex string.
                int nibbles = hashcode.bits() >> 3;
                return Strings.padStart(Integer.toHexString(hashcode.asInt()), nibbles, '0');
            }
        }
    }

    /**
     * Represents downloading a file via GridFTP.
     */
    private class GridFTPDownload extends GridFTPTransfer
    {
        public GridFTPDownload(URI source, File destination)
        {
            super(source, destination);
        }

        @Override
        protected void doTransfer() throws IOException, ServerException, ClientException
        {
            _status = "Querying file size.";
            setTargetSize(_client.getSize(getRemotePath()));

            _status = "Preparing for download.";
            MonitoringFileDataSink sink = new MonitoringFileDataSink();
            if (_client.isFeatureSupported("GETPUT")) {
                _client.get2(getRemotePath(), isPassive(), sink, null);
            } else {
                _client.get(getRemotePath(), sink, null);
            }

            String localChecksum = sink.getHash();
            String remoteChecksum = getRemoteChecksum();

            if (remoteChecksum != null && !remoteChecksum.equals(localChecksum)) {
                throw new IOException("checksum mismatch: " + remoteChecksum + " != " + localChecksum);
            }
        }

        @Override
        protected void incrementBytesTransferred(int count)
        {
            super.incrementBytesTransferred(count);
            _status = "Recieved " + percent() + " of " + getTargetSize() + " bytes.";
        }

        private class MonitoringFileDataSink extends DataSinkStream
        {
            private final Hasher hasher;
            private HashCode hashcode;

            MonitoringFileDataSink() throws FileNotFoundException
            {
                super(new FileOutputStream(getLocalFile()));

                switch (GridFTPDownload.this.getChecksumHandling()) {
                case IF_AVAILABLE:
                case REQUIRE:
                    hasher = Hashing.adler32().newHasher();
                    break;
                case IGNORE:
                    hasher = null;
                    break;
                default:
                    throw new RuntimeException("Unknown ChecksumHandling");
                }
            }

            @Override
            public void write(Buffer out) throws IOException
            {
                if (hasher != null) {
                    hasher.putBytes(out.getBuffer(), 0, out.getLength());
                }
                incrementBytesTransferred(out.getLength());
                super.write(out);
            }

            @Override
            public void close() throws IOException
            {
                if (hashcode != null) {
                    throw new IllegalStateException("Attempt to close already closed DataSink");
                }
                if (hasher != null) {
                    hashcode = hasher.hash();
                }
                super.close();
            }

            public String getHash()
            {
                if (hashcode == null) {
                    return null;
                }

                // We cannot use HashCode#toString as that returns a little-endian hex string.
                int nibbles = hashcode.bits() >> 3;
                return Strings.padStart(Integer.toHexString(hashcode.asInt()), nibbles, '0');
            }
        }
    }
}

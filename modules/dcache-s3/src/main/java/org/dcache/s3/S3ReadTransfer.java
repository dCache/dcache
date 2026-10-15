/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.s3;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.ProtocolInfo;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.util.RedirectedTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A download that the door relays from a pool's HTTP mover to the S3 client.
 * <p>
 * The pool is told that the door is the client, so it accepts the door's connection from the
 * door's internal address; pool selection uses the real client address.  The pool reports the
 * mover URL through {@link #redirect}, and its completion through {@link #finished}.
 */
class S3ReadTransfer extends RedirectedTransfer<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3ReadTransfer.class);

    private static final String PROTOCOL_INFO_NAME = "Http";
    private static final int PROTOCOL_INFO_MAJOR_VERSION = 1;
    private static final int PROTOCOL_INFO_MINOR_VERSION = 1;

    private final InetSocketAddress internalAddress;

    S3ReadTransfer(PnfsHandler pnfs, Subject subject, Restriction restriction, FsPath path,
          InetSocketAddress internalAddress) {
        super(pnfs, subject, restriction, path);
        this.internalAddress = internalAddress;
    }

    private ProtocolInfo createProtocolInfo(InetSocketAddress clientAddress) {
        var protocolInfo = new HttpProtocolInfo(PROTOCOL_INFO_NAME,
              PROTOCOL_INFO_MAJOR_VERSION, PROTOCOL_INFO_MINOR_VERSION, clientAddress,
              getCellName(), getDomainName(), getTransferPath(), null, null, List.of());
        protocolInfo.setSessionId((int) getId());
        return protocolInfo;
    }

    @Override
    protected ProtocolInfo getProtocolInfoForPoolManager() {
        return createProtocolInfo(getClientAddress());
    }

    @Override
    protected ProtocolInfo getProtocolInfoForPool() {
        return createProtocolInfo(internalAddress);
    }

    @Override
    public synchronized void finished(CacheException error) {
        super.finished(error);
        if (error == null) {
            notifyBilling(0, "");
        } else {
            notifyBilling(error.getRc(), error.getMessage());
        }
    }

    /**
     * Connects to the mover and returns the requested part of the file.  On failure, the caller
     * remains responsible for ending the transfer.
     *
     * @param offset            the first byte to read
     * @param length            the number of bytes to read
     * @param timeoutMillis     timeout for connecting to and reading from the pool
     * @param confirmationMillis how long closing the stream waits for the pool to confirm the end
     *                          of the transfer
     * @param killMillis        how long closing the stream waits for the mover to be killed if
     *                          the data was not read to the end
     * @param onClose           called once the stream is closed and the transfer has ended
     */
    InputStream open(long offset, long length, int timeoutMillis, long confirmationMillis,
          long killMillis, Runnable onClose) throws CacheException {
        long size = getFileAttributes().getSize();
        boolean isPartial = offset != 0 || length != size;
        setStatus("Mover " + getPool() + "/" + getMoverId() + ": Opening data connection");
        try {
            URL url = new URL(getRedirect());
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            try {
                connection.setRequestProperty("Connection", "close");
                connection.setConnectTimeout(timeoutMillis);
                connection.setReadTimeout(timeoutMillis);
                if (isPartial) {
                    connection.setRequestProperty("Range",
                          "bytes=" + offset + "-" + (offset + length - 1));
                }
                int status = connection.getResponseCode();
                int expected = isPartial
                      ? HttpURLConnection.HTTP_PARTIAL : HttpURLConnection.HTTP_OK;
                if (status != expected) {
                    throw new CacheException(CacheException.SERVICE_UNAVAILABLE,
                          "Pool " + getPool() + " answered " + status + " "
                                + connection.getResponseMessage());
                }
                setStatus("Mover " + getPool() + "/" + getMoverId() + ": Sending data");
                return new TransferStream(connection, length, confirmationMillis, killMillis,
                      onClose);
            } catch (CacheException | IOException | RuntimeException e) {
                connection.disconnect();
                throw e;
            }
        } catch (SocketTimeoutException e) {
            throw new TimeoutCacheException("Pool " + getPool() + " did not answer in time");
        } catch (IOException e) {
            throw new CacheException(CacheException.SERVICE_UNAVAILABLE,
                  "Failed to connect to pool " + getPool() + ": " + e.getMessage(), e);
        }
    }

    /**
     * The data from the mover.  Closing it ends the transfer: after a complete read it waits for
     * the pool to confirm the transfer; otherwise it kills the mover.
     */
    private class TransferStream extends FilterInputStream {

        private final HttpURLConnection connection;
        private final long length;
        private final long confirmationMillis;
        private final long killMillis;
        private final Runnable onClose;

        @GuardedBy("this")
        private long remaining;
        @GuardedBy("this")
        private String failure;
        @GuardedBy("this")
        private boolean isClosed;

        TransferStream(HttpURLConnection connection, long length, long confirmationMillis,
              long killMillis, Runnable onClose) throws IOException {
            super(connection.getInputStream());
            this.connection = connection;
            this.length = length;
            this.remaining = length;
            this.confirmationMillis = confirmationMillis;
            this.killMillis = killMillis;
            this.onClose = onClose;
        }

        @Override
        public synchronized int read() throws IOException {
            try {
                int b = super.read();
                if (b != -1) {
                    remaining--;
                }
                return b;
            } catch (IOException e) {
                failure = "Error reading from pool: " + e.getMessage();
                throw e;
            }
        }

        @Override
        public synchronized int read(byte[] b, int off, int len) throws IOException {
            try {
                int n = super.read(b, off, len);
                if (n > 0) {
                    remaining -= n;
                }
                return n;
            } catch (IOException e) {
                failure = "Error reading from pool: " + e.getMessage();
                throw e;
            }
        }

        @Override
        public synchronized long skip(long n) throws IOException {
            long skipped = super.skip(n);
            remaining -= skipped;
            return skipped;
        }

        @Override
        public synchronized void close() throws IOException {
            if (isClosed) {
                return;
            }
            isClosed = true;
            try {
                super.close();
            } catch (IOException e) {
                LOGGER.debug("Failed to close connection to pool: {}", e.toString());
            } finally {
                connection.disconnect();
            }

            try {
                if (remaining > 0) {
                    String explanation = failure != null ? failure
                          : "Client disconnected after " + (length - remaining) + " of "
                                + length + " bytes";
                    notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, explanation);
                    killMover(killMillis, explanation);
                } else if (!waitForMover(confirmationMillis, TimeUnit.MILLISECONDS)) {
                    String explanation = "Missing transfer confirmation from pool";
                    LOGGER.warn("Download of {} from {}: {}", getPnfsId(), getPool(),
                          explanation);
                    notifyBilling(CacheException.TIMEOUT, explanation);
                    killMover(killMillis, explanation);
                }
            } catch (CacheException e) {
                // finished(..) has already sent the billing record.
                LOGGER.warn("Download of {} from {} failed: {}", getPnfsId(), getPool(),
                      e.getMessage());
            } catch (InterruptedException e) {
                notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, "Shutting down");
                killMover(0, "door shutting down");
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("Interrupted while ending transfer");
            } finally {
                setStatus(null);
                onClose.run();
            }
        }
    }
}

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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.util.RedirectedTransfer;
import org.dcache.vehicles.FileAttributes;

/**
 * An upload that the door relays from the S3 client to a pool's HTTP mover.
 * <p>
 * As for {@link S3ReadTransfer}, the pool is told that the door is the client and pool selection
 * uses the real client address.  The pool reports the mover URL through {@link #redirect}, and
 * its completion through {@link #finished}.
 */
class S3WriteTransfer extends RedirectedTransfer<String> {

    private static final String PROTOCOL_INFO_NAME = "S3";
    private static final int PROTOCOL_INFO_MAJOR_VERSION = 1;
    private static final int PROTOCOL_INFO_MINOR_VERSION = 1;

    private static final int BUFFER_SIZE = 64 * 1024;

    /**
     * The status with which a pool refuses an upload it has no space for.
     */
    private static final int INSUFFICIENT_STORAGE = 507;

    private final FsPath billingPath;
    private final InetSocketAddress internalAddress;
    private final Map<String, String> xattrs;

    /**
     * @param path        the file to write
     * @param billingPath the path billing records instead of {@code path}
     */
    S3WriteTransfer(PnfsHandler pnfs, Subject subject, Restriction restriction, FsPath path,
          FsPath billingPath, InetSocketAddress internalAddress, Map<String, String> xattrs) {
        super(pnfs, subject, restriction, path);
        this.billingPath = billingPath;
        this.internalAddress = internalAddress;
        this.xattrs = Map.copyOf(xattrs);
    }

    @Override
    public String getBillingPath() {
        return billingPath.toString();
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
    protected FileAttributes fileAttributesForNameSpace() {
        FileAttributes attributes = super.fileAttributesForNameSpace();
        if (!xattrs.isEmpty()) {
            attributes.setXattrs(xattrs);
        }
        return attributes;
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
     * Sends the data to the mover and waits for the pool to confirm the transfer.  On failure,
     * the caller remains responsible for ending the transfer.
     *
     * @param data               the content of the file
     * @param length             the number of bytes to send
     * @param timeoutMillis      timeout for connecting to the pool and for its answer
     * @param confirmationMillis how long to wait for the pool to confirm the transfer
     * @throws IOException if reading {@code data} failed or it ended early
     */
    void relay(InputStream data, long length, int timeoutMillis, long confirmationMillis)
          throws CacheException, InterruptedException, IOException {
        setStatus("Mover " + getPool() + "/" + getMoverId() + ": Opening data connection");
        try {
            HttpURLConnection connection;
            OutputStream out;
            try {
                connection = (HttpURLConnection) new URL(getRedirect()).openConnection();
                connection.setRequestMethod("PUT");
                connection.setRequestProperty("Connection", "close");
                connection.setConnectTimeout(timeoutMillis);
                connection.setReadTimeout(timeoutMillis);
                connection.setDoOutput(true);
                connection.setFixedLengthStreamingMode(length);
                out = connection.getOutputStream();
            } catch (SocketTimeoutException e) {
                throw new TimeoutCacheException("Pool " + getPool() + " did not answer in time");
            } catch (IOException e) {
                throw new CacheException(CacheException.SERVICE_UNAVAILABLE,
                      "Failed to connect to pool " + getPool() + ": " + e.getMessage(), e);
            }

            try {
                setStatus("Mover " + getPool() + "/" + getMoverId() + ": Receiving data");
                send(data, length, connection, out);
                checkResponse(connection);
            } finally {
                connection.disconnect();
            }

            if (!waitForMover(confirmationMillis)) {
                throw new TimeoutCacheException("Missing transfer confirmation from pool");
            }
        } finally {
            setStatus(null);
        }
    }

    /**
     * Copies the data to the mover.  Failures to read the data are thrown as they are; failures to
     * write to the pool become a CacheException.
     */
    private void send(InputStream data, long length, HttpURLConnection connection,
          OutputStream out) throws CacheException, IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        long remaining = length;
        while (remaining > 0) {
            int n = data.read(buffer, 0, (int) Math.min(buffer.length, remaining));
            if (n == -1) {
                throw new EOFException("Client sent " + (length - remaining) + " of " + length
                      + " bytes");
            }
            try {
                out.write(buffer, 0, n);
            } catch (IOException e) {
                throw poolFailure(connection, e);
            }
            remaining -= n;
        }
        try {
            out.close();
        } catch (IOException e) {
            throw poolFailure(connection, e);
        }
    }

    /**
     * Explains a failure to send data to the pool, which may have answered before it stopped
     * reading.
     */
    private CacheException poolFailure(HttpURLConnection connection, IOException e) {
        try {
            checkResponse(connection);
        } catch (CacheException answer) {
            return answer;
        }
        return new CacheException(CacheException.SERVICE_UNAVAILABLE,
              "Failed to send data to pool " + getPool() + ": " + e.getMessage(), e);
    }

    private void checkResponse(HttpURLConnection connection) throws CacheException {
        int status;
        try {
            status = connection.getResponseCode();
        } catch (SocketTimeoutException e) {
            throw new TimeoutCacheException("Pool " + getPool() + " did not answer in time");
        } catch (IOException e) {
            throw new CacheException(CacheException.SERVICE_UNAVAILABLE,
                  "Pool " + getPool() + " did not answer: " + e.getMessage(), e);
        }
        String message;
        try {
            message = connection.getResponseMessage();
        } catch (IOException e) {
            message = "";
        }
        switch (status) {
            case HttpURLConnection.HTTP_CREATED:
                return;
            case INSUFFICIENT_STORAGE:
                throw new CacheException(CacheException.RESOURCE,
                      "Pool " + getPool() + " has insufficient storage: " + message);
            default:
                throw new CacheException(
                      "Pool " + getPool() + " answered " + status + " " + message);
        }
    }
}

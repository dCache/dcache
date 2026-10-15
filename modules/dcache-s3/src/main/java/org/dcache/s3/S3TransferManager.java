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

import static java.util.Objects.requireNonNull;
import static org.dcache.util.TransferRetryPolicy.tryOnce;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolManagerStub;
import org.dcache.util.NetworkUtils;
import org.dcache.util.PingMoversTask;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.vehicles.FileAttributes;

/**
 * Reads files from pools for the S3 door, relaying the data through the door.
 * <p>
 * Each read is a {@link S3ReadTransfer} with its own HTTP mover.  This component receives the
 * pools' messages about those movers, so it must be registered as a message receiver of the
 * door's cell.
 */
public class S3TransferManager implements S3ObjectReader, CellMessageReceiver,
      CellIdentityAware, CellInfoProvider {

    private static final long PING_PERIOD = TimeUnit.MINUTES.toMillis(5);

    private final Map<Integer, S3ReadTransfer> transfers = new ConcurrentHashMap<>();

    private PnfsHandler pnfs;
    private PoolManagerStub poolManagerStub;
    private CellStub poolStub;
    private CellStub billingStub;
    private CellAddressCore cellAddress;
    private InetSocketAddress internalAddress;
    private String ioQueue;
    private long moverTimeout = 180;
    private TimeUnit moverTimeoutUnit = TimeUnit.SECONDS;
    private long poolTimeout = 300;
    private TimeUnit poolTimeoutUnit = TimeUnit.SECONDS;
    private long transferConfirmationTimeout = 60;
    private TimeUnit transferConfirmationTimeoutUnit = TimeUnit.SECONDS;
    private long killTimeout = 1500;
    private TimeUnit killTimeoutUnit = TimeUnit.MILLISECONDS;

    public void setPnfsHandler(PnfsHandler pnfs) {
        this.pnfs = requireNonNull(pnfs);
    }

    public void setPoolManagerStub(PoolManagerStub stub) {
        poolManagerStub = requireNonNull(stub);
    }

    public void setPoolStub(CellStub stub) {
        poolStub = requireNonNull(stub);
    }

    public void setBillingStub(CellStub stub) {
        billingStub = requireNonNull(stub);
    }

    /**
     * The address pools see the door's data connections come from; empty for the address of the
     * local host.
     */
    public void setInternalAddress(String address) throws UnknownHostException {
        internalAddress = new InetSocketAddress(NetworkUtils.getInternalAddress(address), 0);
    }

    /**
     * Periodically checks that the movers of active transfers still exist, so that transfers
     * whose end the door missed are failed.
     */
    public void setExecutor(ScheduledExecutorService executor) {
        executor.scheduleAtFixedRate(new PingMoversTask<>(transfers.values()), PING_PERIOD,
              PING_PERIOD, TimeUnit.MILLISECONDS);
    }

    /**
     * The mover queue on the pools; null for the default queue.
     */
    public void setIoQueue(String queue) {
        ioQueue = (queue == null || queue.isEmpty()) ? null : queue;
    }

    /**
     * How long to wait for a pool to be selected and its mover to start.
     */
    public void setMoverTimeout(long timeout) {
        moverTimeout = timeout;
    }

    public void setMoverTimeoutUnit(TimeUnit unit) {
        moverTimeoutUnit = requireNonNull(unit);
    }

    /**
     * Timeout for connecting to the mover and for each read from it.
     */
    public void setPoolTimeout(long timeout) {
        poolTimeout = timeout;
    }

    public void setPoolTimeoutUnit(TimeUnit unit) {
        poolTimeoutUnit = requireNonNull(unit);
    }

    /**
     * How long to wait for the pool to confirm a transfer once all data was read.
     */
    public void setTransferConfirmationTimeout(long timeout) {
        transferConfirmationTimeout = timeout;
    }

    public void setTransferConfirmationTimeoutUnit(TimeUnit unit) {
        transferConfirmationTimeoutUnit = requireNonNull(unit);
    }

    /**
     * How long to wait for a killed mover to end.
     */
    public void setKillTimeout(long timeout) {
        killTimeout = timeout;
    }

    public void setKillTimeoutUnit(TimeUnit unit) {
        killTimeoutUnit = requireNonNull(unit);
    }

    @Override
    public void setCellAddress(CellAddressCore address) {
        cellAddress = address;
    }

    @Override
    public InputStream read(S3User user, FsPath path, FileAttributes attributes, long offset,
          long length) throws CacheException, InterruptedException {
        var transfer = new S3ReadTransfer(pnfs, user.subject(), user.restriction(), path,
              internalAddress);
        transfer.setPnfsId(attributes.getPnfsId());
        transfer.setCellAddress(cellAddress);
        transfer.setPoolManagerStub(poolManagerStub);
        transfer.setPoolStub(poolStub);
        transfer.setBillingStub(billingStub);
        transfer.setIoQueue(ioQueue);
        transfer.setClientAddresses(List.of(new InetSocketAddress(
              RequestOrigin.current().orElse(InetAddress.getLoopbackAddress()), 0)));

        long moverTimeoutMillis = moverTimeoutUnit.toMillis(moverTimeout);
        long killTimeoutMillis = killTimeoutUnit.toMillis(killTimeout);
        int id = (int) transfer.getId();
        transfers.put(id, transfer);
        boolean isStarted = false;
        try {
            transfer.readNameSpaceEntry(false);
            TransferRetryPolicy policy = tryOnce().timeoutAfter(moverTimeoutMillis,
                  TimeUnit.MILLISECONDS);
            try {
                transfer.selectPoolAndStartMover(policy);
                if (transfer.waitForRedirect(moverTimeoutMillis, TimeUnit.MILLISECONDS) == null) {
                    throw new TimeoutCacheException("Server is busy (internal timeout)");
                }
            } finally {
                transfer.setStatus(null);
            }
            int poolTimeoutMillis = (int) Math.min(Integer.MAX_VALUE,
                  poolTimeoutUnit.toMillis(poolTimeout));
            InputStream in = transfer.open(offset, length, poolTimeoutMillis,
                  transferConfirmationTimeoutUnit.toMillis(transferConfirmationTimeout),
                  killTimeoutMillis, () -> transfers.remove(id));
            isStarted = true;
            return in;
        } catch (CacheException e) {
            transfer.notifyBilling(e.getRc(), e.getMessage());
            transfer.killMover(killTimeoutMillis, "door experienced internal error: " + e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, "Shutting down");
            transfer.killMover(0, "door shutting down");
            throw e;
        } catch (RuntimeException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.toString());
            transfer.killMover(killTimeoutMillis, "door encountered a bug");
            throw e;
        } finally {
            if (!isStarted) {
                transfers.remove(id);
            }
        }
    }

    /**
     * The mover URL of a transfer.
     */
    public void messageArrived(HttpDoorUrlInfoMessage message) {
        S3ReadTransfer transfer = transfers.get((int) message.getId());
        if (transfer != null) {
            transfer.redirect(message.getUrl());
        }
    }

    /**
     * The end of a transfer.
     */
    public void messageArrived(DoorTransferFinishedMessage message) {
        S3ReadTransfer transfer = transfers.get((int) message.getId());
        if (transfer != null) {
            transfer.finished(message);
        }
    }

    /**
     * A mover created after its transfer gave up waiting for it; it has no use, so kill it.
     */
    public void messageArrived(PoolIoFileMessage message) {
        if (message.getReturnCode() == 0) {
            String pool = message.getPoolName();
            poolStub.notify(new CellPath(pool), new PoolMoverKillMessage(pool,
                  message.getMoverId(), "door timed out before pool"));
        }
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.println("Active transfers : " + transfers.size());
    }
}

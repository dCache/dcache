package diskCacheV111.services.space;

import com.google.common.base.Throwables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.remoting.RemoteAccessException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionException;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.VOInfo;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.util.command.Command;
import dmg.util.command.DelayedCommand;

import org.dcache.cells.CuratorFrameworkAware;
import org.dcache.poolmanager.PoolLinkGroupInfo;
import org.dcache.poolmanager.RemotePoolMonitor;
import org.dcache.poolmanager.Utils;

public class LinkGroupLoader
        implements CellCommandListener, CuratorFrameworkAware, CellLifeCycleAware, CellInfoProvider, CellIdentityAware
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LinkGroupLoader.class);
    private static final long EAGER_LINKGROUP_UPDATE_PERIOD = 1000;

    private long updateLinkGroupsPeriod;

    private File authorizationFileName;
    private long latestUpdateTime = System.currentTimeMillis();
    private LinkGroupAuthorizationFile linkGroupAuthorizationFile;
    private long authorizationFileLastUpdateTimestamp;

    private RemotePoolMonitor poolMonitor;
    private SpaceManagerDatabase db;

    private ScheduledExecutorService executor;
    private String zkPath;
    private LeaderLatch leaderLatch;
    private CuratorFramework client;

    private CellAddressCore cellAddress;

    @Override
    public void setCellAddress(CellAddressCore address)
    {
        cellAddress = address;
    }

    @Required
    public void setUpdateLinkGroupsPeriod(long updateLinkGroupsPeriod)
    {
        this.updateLinkGroupsPeriod = updateLinkGroupsPeriod;
    }

    @Required
    public void setDatabase(SpaceManagerDatabase db)
    {
        this.db = db;
    }

    @Required
    public void setPoolMonitor(RemotePoolMonitor poolMonitor)
    {
        this.poolMonitor = poolMonitor;
    }

    @Required
    public void setAuthorizationFileName(File authorizationFileName)
    {
        this.authorizationFileName = authorizationFileName;
    }

    @Required
    public void setServiceName(String serviceName)
    {
        zkPath = getZooKeeperLeaderPath(serviceName);
    }

    public long getLatestUpdateTime()
    {
        return latestUpdateTime;
    }

    @Override
    public void setCuratorFramework(CuratorFramework client)
    {
        this.client = client;
    }

    public void start()
    {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void afterStart()
    {
        try {
            leaderLatch = new LeaderLatch(client, zkPath, cellAddress.toString());
            leaderLatch.addListener(new LeaderListener());
            leaderLatch.start();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void beforeStop()
    {
        CloseableUtils.closeQuietly(leaderLatch);
    }

    public void stop()
    {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public void getInfo(PrintWriter printWriter) {
        printWriter.append("updateLinkGroupsPeriod = ").println(updateLinkGroupsPeriod);
        printWriter.append("authorizationFileName = ").println(authorizationFileName);
    }

    private void loadLinkGroupAuthorizationFile() {
        File file = authorizationFileName;
        if(file == null) {
            return;
        }
        if(!file.exists()) {
            linkGroupAuthorizationFile = null;
        }
        long lastModified = file.lastModified();
        if (linkGroupAuthorizationFile == null|| lastModified >= authorizationFileLastUpdateTimestamp) {
            authorizationFileLastUpdateTimestamp = lastModified;
            try {
                linkGroupAuthorizationFile =
                        new LinkGroupAuthorizationFile(file);
            } catch (IOException | ParseException e) {
                LOGGER.error("Failed to read {}: {}", file, e.toString());
            }
        }
    }

    private int updateLinkGroups() throws InterruptedException, RemoteAccessException, DataAccessException, TransactionException
    {
        long currentTime = System.currentTimeMillis();
        Collection<PoolLinkGroupInfo> linkGroupInfos =
                Utils.linkGroupInfos(poolMonitor.getPoolSelectionUnit(), poolMonitor.getCostModule()).values();
        if (!linkGroupInfos.isEmpty()) {
            loadLinkGroupAuthorizationFile();
            for (PoolLinkGroupInfo info : linkGroupInfos) {
                saveLinkGroup(currentTime, info);
            }
        }
        latestUpdateTime = currentTime;
        return linkGroupInfos.size();
    }

    private void saveLinkGroup(long currentTime, PoolLinkGroupInfo info) throws InterruptedException
    {
        String linkGroupName = info.getName();
        long avalSpaceInBytes = info.getAvailableSpaceInBytes();
        VOInfo[] vos = null;
        boolean onlineAllowed = info.isOnlineAllowed();
        boolean nearlineAllowed = info.isNearlineAllowed();
        boolean replicaAllowed = info.isReplicaAllowed();
        boolean outputAllowed = info.isOutputAllowed();
        boolean custodialAllowed = info.isCustodialAllowed();
        if (linkGroupAuthorizationFile != null) {
            LinkGroupAuthorizationRecord record =
                    linkGroupAuthorizationFile
                            .getLinkGroupAuthorizationRecord(linkGroupName);
            if (record != null) {
                vos = record.getVOInfoArray();
            }
        }
        while (true) {
            try {
                db.updateLinkGroup(linkGroupName,
                                   avalSpaceInBytes,
                                   currentTime,
                                   onlineAllowed,
                                   nearlineAllowed,
                                   replicaAllowed,
                                   outputAllowed,
                                   custodialAllowed,
                                   vos);
                break;
            } catch (DeadlockLoserDataAccessException e) {
                LOGGER.info("Update of link group {} lost deadlock race and will be retried: {}",
                            linkGroupName, e.toString());
            } catch (TransientDataAccessException | RecoverableDataAccessException | CannotCreateTransactionException e) {
                LOGGER.warn("Update of link group {} failed and will be retried: {}",
                            linkGroupName, e.getMessage());
            }
            Thread.sleep(500);
        }
    }

    @Command(name = "update link groups", hint = "update link group information",
             description = "Link groups are periodically imported from pool manager and stored in " +
                     "the space manager database. This command performs an immediate " +
                     "update of the link group information.")
    public class UpdateLinkGroupsCommand extends DelayedCommand<String>
    {
        public UpdateLinkGroupsCommand()
        {
            super(executor);
        }

        @Override
        protected String execute()
                throws InterruptedException, DataAccessException, TransactionException, CacheException
        {
            poolMonitor.refresh();
            int updated = updateLinkGroups();
            return updated + (updated == 1 ? " link group " : " link groups ") + "updated.";
        }
    }

    private class LeaderListener implements LeaderLatchListener, Runnable
    {
        private ScheduledFuture<?> future;

        @Override
        public synchronized void isLeader()
        {
            future = executor.schedule(this, 1, TimeUnit.SECONDS);
        }

        @Override
        public synchronized void notLeader()
        {
            future.cancel(false);
            future = null;
        }

        private synchronized void reschedule(long period)
        {
            if (future != null) {
                future = executor.schedule(this, period, TimeUnit.MILLISECONDS);
            }
        }

        @Override
        public void run() {
            long period = EAGER_LINKGROUP_UPDATE_PERIOD;
            try {
                if (updateLinkGroups() > 0) {
                    period = updateLinkGroupsPeriod;
                }
            } catch (RemoteAccessException | DataAccessException | TransactionException e) {
                LOGGER.error("Link group update failed: {}", e.getMessage());
            } catch (RuntimeException e) {
                LOGGER.error("Link group update failed: " + e.toString(), e);
            } catch (InterruptedException e) {
                LOGGER.trace("update LinkGroup thread has been interrupted");
            } finally {
                reschedule(period);
            }
        }
    }

    private static String getZooKeeperLeaderPath(String serviceName)
    {
        return ZKPaths.makePath("/dcache/spacemanager", serviceName, "leader");
    }
}

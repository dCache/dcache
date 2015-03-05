package diskCacheV111.services.space;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.target.dynamic.Refreshable;
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
import java.util.concurrent.Callable;

import diskCacheV111.util.VOInfo;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.poolmanager.PoolLinkGroupInfo;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.poolmanager.RemovableRefreshable;
import org.dcache.poolmanager.Utils;

public class LinkGroupLoader
        extends AbstractCellComponent implements Runnable, CellCommandListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LinkGroupLoader.class);
    private static final long EAGER_LINKGROUP_UPDATE_PERIOD = 1000;

    private final Object updateLinkGroupsSyncObject = new Object();

    private long updateLinkGroupsPeriod;
    private long currentUpdateLinkGroupsPeriod = EAGER_LINKGROUP_UPDATE_PERIOD;

    private File authorizationFileName;
    private long latestUpdateTime = System.currentTimeMillis();
    private LinkGroupAuthorizationFile linkGroupAuthorizationFile;
    private long authorizationFileLastUpdateTimestamp;

    private PoolMonitor poolMonitor;
    private SpaceManagerDatabase db;

    private Thread updateLinkGroups;

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
    public void setPoolMonitor(PoolMonitor poolMonitor)
    {
        this.poolMonitor = poolMonitor;
    }

    @Required
    public void setAuthorizationFileName(File authorizationFileName)
    {
        this.authorizationFileName = authorizationFileName;
    }

    public long getLatestUpdateTime()
    {
        return latestUpdateTime;
    }

    public void start()
    {
        (updateLinkGroups = new Thread(this,"UpdateLinkGroups")).start();
    }

    public void stop()
    {
        if (updateLinkGroups != null) {
            updateLinkGroups.interrupt();
        }
    }

    @Override
    public void getInfo(PrintWriter printWriter) {
        printWriter.append("updateLinkGroupsPeriod=").println(updateLinkGroupsPeriod);
        printWriter.append("authorizationFileName=").println(authorizationFileName);
    }

    @Override
    public void run(){
        try {
            while (true) {
                updateLinkGroups();
                synchronized (updateLinkGroupsSyncObject) {
                    updateLinkGroupsSyncObject.notifyAll();
                    updateLinkGroupsSyncObject.wait(currentUpdateLinkGroupsPeriod);
                }
            }
        } catch (InterruptedException ie) {
            LOGGER.trace("update LinkGroup thread has been interrupted");
        }
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

    private void updateLinkGroups() throws InterruptedException
    {
        try {
            currentUpdateLinkGroupsPeriod = EAGER_LINKGROUP_UPDATE_PERIOD;
            long currentTime = System.currentTimeMillis();
            Collection<PoolLinkGroupInfo> linkGroupInfos = Utils.linkGroupInfos(poolMonitor.getPoolSelectionUnit(),
                                                                                poolMonitor.getCostModule()).values();
            if (linkGroupInfos.isEmpty()) {
                latestUpdateTime = currentTime;
                return;
            }

            currentUpdateLinkGroupsPeriod = updateLinkGroupsPeriod;

            loadLinkGroupAuthorizationFile();
            for (PoolLinkGroupInfo info : linkGroupInfos) {
                saveLinkGroup(currentTime, info);
            }
            latestUpdateTime = currentTime;
        } catch (RemoteAccessException | DataAccessException | TransactionException e) {
            LOGGER.error("Link group update failed: {}", e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Link group update failed: " + e.toString(), e);
        }
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

    @Command(name = "update link groups", hint = "trigger link group update",
             description = "Link groups are periodically imported from pool manager and stored in " +
                     "the space manager database. This command triggers an immediate " +
                     "asynchronous update of the link group information.")
    public class UpdateLinkGroupsCommand implements Callable<String>
    {
        @Option(name = "blocking",
                usage = "Wait for update to complete.")
        boolean blocking;

        @Override
        public String call()
        {
            String response = "Update started";
            String extra = "";

            if (blocking && poolMonitor instanceof RemovableRefreshable) {
                extra = ", cached pool information was removed";
                ((RemovableRefreshable) poolMonitor).remove();
            } else if (poolMonitor instanceof Refreshable) {
                extra = ", refreshing cached pool information";
                ((Refreshable) poolMonitor).refresh();
            }

            synchronized (updateLinkGroupsSyncObject) {
                updateLinkGroupsSyncObject.notify();

                if (blocking) {
                    response = "Update completed";
                    try {
                        updateLinkGroupsSyncObject.wait();
                    } catch (InterruptedException e) {
                        response = "Interrupted while updating";
                    }
                }
            }

            return response + extra + ".";
        }
    }
}

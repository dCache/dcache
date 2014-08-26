package diskCacheV111.services.space;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.target.dynamic.Refreshable;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;
import org.springframework.remoting.RemoteAccessException;

import java.io.File;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.concurrent.Callable;

import diskCacheV111.util.VOInfo;
import diskCacheV111.vehicles.PoolLinkGroupInfo;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Command;

import org.dcache.poolmanager.PoolMonitor;
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
            while(true) {
                try {
                    updateLinkGroups();
                } catch (RemoteAccessException e) {
                    LOGGER.error("Link group update failed: {}", e.getMessage());
                } catch (RuntimeException e) {
                    LOGGER.error("Link group update failed: " +  e.toString(), e);
                }
                synchronized(updateLinkGroupsSyncObject) {
                    try {
                        updateLinkGroupsSyncObject.wait(currentUpdateLinkGroupsPeriod);
                    }
                    catch (InterruptedException ie) {
                        LOGGER.trace("update LinkGroup thread has been interrupted");
                        return;
                    }
                }
            }
    }

    private void updateLinkGroupAuthorizationFile() {
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
            }
            catch(Exception e) {
                LOGGER.error("failed to parse LinkGroupAuthorizationFile: {}",
                             e.getMessage());
            }
        }
    }

    private void updateLinkGroups() {
        currentUpdateLinkGroupsPeriod = EAGER_LINKGROUP_UPDATE_PERIOD;
        long currentTime = System.currentTimeMillis();
        Collection<PoolLinkGroupInfo> linkGroupInfos = Utils.linkGroupInfos(poolMonitor.getPoolSelectionUnit(),
                                                                            poolMonitor.getCostModule()).values();
        if (linkGroupInfos.isEmpty()) {
            latestUpdateTime = currentTime;
            return;
        }

        currentUpdateLinkGroupsPeriod = updateLinkGroupsPeriod;

        updateLinkGroupAuthorizationFile();
        for (PoolLinkGroupInfo info : linkGroupInfos) {
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
            } catch (DataAccessException sqle) {
                LOGGER.error("Update of link group {} failed: {}",
                             linkGroupName, sqle.getMessage());
            }
        }
        latestUpdateTime = currentTime;
    }

    @Command(name = "update link groups", hint = "trigger link group update",
             description = "Link groups are periodically imported from pool manager and stored in " +
                     "the space manager database. This command triggers an immediate " +
                     "asynchronous update of the link group information.")
    public class UpdateLinkGroupsCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            if (poolMonitor instanceof Refreshable) {
                ((Refreshable) poolMonitor).refresh();
            }
            synchronized (updateLinkGroupsSyncObject) {
                updateLinkGroupsSyncObject.notify();
            }
            return "Update started.";
        }
    }
}

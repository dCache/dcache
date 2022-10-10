package diskCacheV111.services.space;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.VOInfo;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.command.Command;
import dmg.util.command.DelayedCommand;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.dcache.poolmanager.PoolLinkGroupInfo;
import org.dcache.poolmanager.RemotePoolMonitor;
import org.dcache.poolmanager.Utils;
import org.dcache.util.files.LineByLineParser;
import org.dcache.util.files.ParsableFile;
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

public class LinkGroupLoader
      implements CellCommandListener, CellLifeCycleAware, CellInfoProvider, Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LinkGroupLoader.class);
    private static final long EAGER_LINKGROUP_UPDATE_PERIOD = 1000;

    private long updateLinkGroupsPeriod;

    private Optional<ParsableFile<Map<String, LinkGroupAuthorizationRecord>>> authorizationFile;
    private long latestUpdateTime = System.currentTimeMillis();

    private RemotePoolMonitor poolMonitor;
    private SpaceManagerDatabase db;

    private ScheduledExecutorService executor;

    @Required
    public void setUpdateLinkGroupsPeriod(long updateLinkGroupsPeriod) {
        this.updateLinkGroupsPeriod = updateLinkGroupsPeriod;
    }

    @Required
    public void setDatabase(SpaceManagerDatabase db) {
        this.db = db;
    }

    @Required
    public void setPoolMonitor(RemotePoolMonitor poolMonitor) {
        this.poolMonitor = poolMonitor;
    }

    @Required
    public void setAuthorizationFileName(Path authorizationFileName) {
        if (authorizationFileName == null) {
            authorizationFile = Optional.empty();
            return;
        }
        authorizationFile = Optional.of(new ParsableFile(
              new LineByLineParser(LinkGroupAuthorizationFileParser::new),
              authorizationFileName));
    }

    public long getLatestUpdateTime() {
        return latestUpdateTime;
    }

    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void afterStart() {
        executor.schedule(this, 0, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public void getInfo(PrintWriter printWriter) {
        printWriter.append("updateLinkGroupsPeriod = ").println(updateLinkGroupsPeriod);
        printWriter.append("authorizationFileName = ")
              .println(authorizationFile.isPresent() ? authorizationFile.get().getPath() : "-");
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
            executor.schedule(this, period, TimeUnit.MILLISECONDS);
        }
    }

    private int updateLinkGroups()
          throws InterruptedException, RemoteAccessException, DataAccessException, TransactionException {
        long currentTime = System.currentTimeMillis();
        Collection<PoolLinkGroupInfo> linkGroupInfos =
              Utils.linkGroupInfos(poolMonitor.getPoolSelectionUnit(), poolMonitor.getCostModule())
                    .values();
        if (!linkGroupInfos.isEmpty()) {
            for (PoolLinkGroupInfo info : linkGroupInfos) {
                saveLinkGroup(currentTime, info);
            }
        } else {
            LOGGER.debug("In updateLinkGroups, poolMonitor updated no linkgroups");
        }
        latestUpdateTime = currentTime;
        return linkGroupInfos.size();
    }

    private void saveLinkGroup(long currentTime, PoolLinkGroupInfo info)
          throws InterruptedException {
        String linkGroupName = info.getName();
        long avalSpaceInBytes = info.getAvailableSpaceInBytes();
        boolean onlineAllowed = info.isOnlineAllowed();
        boolean nearlineAllowed = info.isNearlineAllowed();
        boolean replicaAllowed = info.isReplicaAllowed();
        boolean outputAllowed = info.isOutputAllowed();
        boolean custodialAllowed = info.isCustodialAllowed();
        VOInfo[] vos = authorizationFile.isEmpty() ? null :
              authorizationFile.get().get().getSuccess()
                    .flatMap(f -> Optional.ofNullable(f.get(linkGroupName)))
                    .map(LinkGroupAuthorizationRecord::getVOInfoArray)
                    .orElse(null);

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
    public class UpdateLinkGroupsCommand extends DelayedCommand<String> {

        public UpdateLinkGroupsCommand() {
            super(executor);
        }

        @Override
        protected String execute()
              throws InterruptedException, DataAccessException, TransactionException, CacheException, NoRouteToCellException {
            poolMonitor.refresh();
            int updated = updateLinkGroups();
            return updated + (updated == 1 ? " link group " : " link groups ") + "updated.";
        }
    }
}

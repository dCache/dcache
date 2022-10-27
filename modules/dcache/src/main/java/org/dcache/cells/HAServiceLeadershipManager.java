/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.cells;

import static dmg.util.CommandException.checkCommand;

import com.google.common.base.Throwables;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.zookeeper.CDCLeaderLatchListener;
import dmg.util.CommandException;
import dmg.util.command.Command;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.concurrent.Callable;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.springframework.beans.factory.annotation.Required;

/**
 * Manages the leader election for HA services and propagates leadership change events to the
 * registered listener.
 */
public class HAServiceLeadershipManager implements CellIdentityAware, CellCommandListener,
      CellInfoProvider, CuratorFrameworkAware {

    public static final String HA_NOT_LEADER_MSG = "This cell does not have leadership. Doing nothing.";

    private CellAddressCore cellAddress;

    private String zkLeaderPath;
    private CuratorFramework zkClient;
    private LeaderLatch zkLeaderLatch;
    private LeaderLatchListener leadershipListener;

    public HAServiceLeadershipManager(String serviceName) {
        createZkLeadershipPath(serviceName);
    }

    @Override
    public void setCuratorFramework(CuratorFramework client) {
        zkClient = client;
    }

    @Override
    public void setCellAddress(CellAddressCore address) {
        cellAddress = address;
    }

    @Required
    public void setLeadershipListener(LeaderLatchListener leadershipListener) {
        this.leadershipListener = leadershipListener;
    }

    public void shutdown() {
        if (zkLeaderLatch != null) {
            CloseableUtils.closeQuietly(zkLeaderLatch);
        }
    }

    private void createZkLeadershipPath(String serviceName) {
        zkLeaderPath = ZKPaths.makePath("/dcache", serviceName, "leader");
    }

    /**
     * Creates a ZooKeeper leader latch, attaches and starts a listener.
     */
    private void initZkLeaderListener() {
        zkLeaderLatch = new LeaderLatch(zkClient, zkLeaderPath, cellAddress.toString());
        zkLeaderLatch.addListener(new CDCLeaderLatchListener(leadershipListener));
        try {
            zkLeaderLatch.start();
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public boolean hasLeadership() {
        return zkLeaderLatch.hasLeadership();
    }

    private synchronized void releaseLeadership() {
        try {
            zkLeaderLatch.close(LeaderLatch.CloseMode.NOTIFY_LEADER);
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        initZkLeaderListener();
    }

    private String getHighAvailabilityRole() {
        return zkLeaderLatch.hasLeadership() ? "LEADER" : "FOLLOWER";
    }

// ---  HA cluster related admin commands

    @Command(name = "ha release leadership",
          description = "Starts a leader election.")
    public class ZkStartLeaderElectionCommand implements Callable<String> {

        @Override
        public String call() throws Exception {
            checkCommand(hasLeadership(), HA_NOT_LEADER_MSG);
            int haParticipantCount = zkLeaderLatch.getParticipants().size();
            if (haParticipantCount == 1) {
                return "Single instance HA service. Not dropping leadership.";
            }
            releaseLeadership();
            return "Releasing leadership, starting election. New leader: "
                  + zkLeaderLatch.getLeader().getId();
        }
    }

    @Command(name = "ha show participants",
          description = "Shows which cells are involved in the leader election.")
    public class ZkShowParticipantsCommand implements Callable<String> {

        @Override
        public String call() throws InterruptedException, CommandException {
            Collection<Participant> participants;
            try {
                participants = zkLeaderLatch.getParticipants();
            } catch (Exception e) {
                Throwables.throwIfUnchecked(e);
                throw new CommandException("Failed to list HA participants: " + e.getMessage(), e);
            }
            StringBuilder sb = new StringBuilder();
            for (Participant p : participants) {
                sb.append(p.toString()).append("\n");
            }
            return sb.toString();
        }
    }

    @Command(name = "ha get role",
          description = "Shows which leadership role the cell has.")
    public class ZkIsLeaderCommand implements Callable<String> {

        @Override
        public String call() throws InterruptedException {
            return getHighAvailabilityRole();
        }
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.printf("HA role: %s\n", getHighAvailabilityRole());
    }

}

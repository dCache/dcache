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
package org.dcache.chimera.namespace;

import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.util.HashSet;
import java.util.Set;

/**
 * Group of components that listen to common leadership events.
 */
public class LeadershipListenerGroup implements LeaderLatchListener {

    private static final Logger log = LoggerFactory.getLogger(LeadershipListenerGroup.class);

    private final Set<LeaderLatchListener> leaderElectionAwareComponents = new HashSet<>();

    @Required
    public void setLeaderElectionAwareComponents(Set<LeaderLatchListener> components) {
        leaderElectionAwareComponents.addAll(components);
    }

    @Override
    public void isLeader() {
        log.info("HA: Assuming leader role.");
        leaderElectionAwareComponents.forEach(LeaderLatchListener::isLeader);
    }

    @Override
    public void notLeader() {
        log.info("HA: Dropping leader role.");
        leaderElectionAwareComponents.forEach(LeaderLatchListener::notLeader);
    }
}

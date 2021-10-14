/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package dmg.cells.zookeeper;

import dmg.cells.nucleus.CDC;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

/**
 * A work-around for injecting the correct CDC into some wrapped LeaderLatchListener.  We need this
 * as Curator creates an internal ExecutorService that looses the CDC (see
 * AfterConnectionEstablished#execute).
 */
public class CDCLeaderLatchListener implements LeaderLatchListener {

    private final LeaderLatchListener inner;
    private final CDC cdc = new CDC();

    public CDCLeaderLatchListener(LeaderLatchListener inner) {
        this.inner = inner;
    }

    @Override
    public void isLeader() {
        try (CDC ignored = cdc.restore()) {
            inner.isLeader();
        }
    }

    @Override
    public void notLeader() {
        try (CDC ignored = cdc.restore()) {
            inner.notLeader();
        }
    }
}

/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.apache.zookeeper.server;

import org.apache.zookeeper.server.ZooKeeperServer.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * This class wraps ZooKeeperServerShutdownHandler primarily to provide a
 * public class as a work-around for issue ZOOKEEPER-2991.  See:
 *
 *     https://issues.apache.org/jira/browse/ZOOKEEPER-2991
 */
public class LoggingShutdownHandler extends ZooKeeperServerShutdownHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingShutdownHandler.class);

    public LoggingShutdownHandler(CountDownLatch shutdownLatch)
    {
        super(shutdownLatch);
    }

    @Override
    void handle(State state)
    {
        LOGGER.warn("Zookeeper server now {}", state);
        super.handle(state);
    }
}

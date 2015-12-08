/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.util.aspects;

import org.slf4j.Logger;
import org.apache.zookeeper.ClientCnxn;

import java.net.ConnectException;
import java.net.InetSocketAddress;

/**
 * Advices ZooKeeper client not to log stack trace on ConnectionException.
 */
aspect ZooKeeperConnectionExceptionAspect perthis(logStartConnectCall(InetSocketAddress))
{
    private InetSocketAddress addr;

    pointcut logStartConnectCall(InetSocketAddress addr) :
            execution(void ClientCnxn.SendThread.logStartConnect(InetSocketAddress)) && args(addr);

    pointcut warningOnSenderException(Logger log, String message, Throwable exception) :
            withincode(void ClientCnxn.SendThread.run()) && call(void Logger.warn(java.lang.String,java.lang.Throwable)) && args(message, exception) && target(log);

    before(InetSocketAddress addr) : logStartConnectCall(addr) {
        this.addr = addr;
    }

    void around(Logger log, String message, Throwable e) : warningOnSenderException(log, message, e) {
        if (e instanceof ConnectException) {
            log.warn("ZooKeeper connection to {} failed ({}), attempting reconnect.", addr, e.getMessage());
        } else {
            proceed(log, message, e);
        }
    }
}

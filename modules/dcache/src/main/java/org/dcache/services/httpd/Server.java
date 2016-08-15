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
package org.dcache.services.httpd;

import org.eclipse.jetty.util.thread.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;

import dmg.cells.nucleus.CellLifeCycleAware;

public class Server extends org.eclipse.jetty.server.Server
    implements CellLifeCycleAware
{
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    public Server(ThreadPool pool)
    {
        super(pool);
    }

    @PostConstruct
    public void startServer() throws Exception
    {
        /* Since the handlers used by httpd call back to the bean factory for initialization,
         * we have to call this during the post construct phase rather than the afterStart
         * phase.
         */
        start();
    }

    @Override
    public void beforeStop()
    {
        try {
            stop();
        } catch (Exception e) {
            LOG.error("Web server shutdown failed: " + e);
        }
    }
}

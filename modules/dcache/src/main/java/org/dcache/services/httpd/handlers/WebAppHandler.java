/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package org.dcache.services.httpd.handlers;

import org.eclipse.jetty.plus.jndi.EnvEntry;
import org.eclipse.jetty.webapp.WebAppContext;

import java.util.Map;
import java.util.Properties;
import javax.naming.NamingException;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.EnvironmentAware;
import org.dcache.poolmanager.RemotePoolMonitor;

public class WebAppHandler extends WebAppContext
    implements EnvironmentAware, CellMessageSender
{
    public static final String CELL_ENDPOINT = "serviceCellEndpoint";
    public static final String JNDI_ARGS = "jndiArgs";
    public static final String POOL_MONITOR = "poolMonitor";

    private static final String[] CONFIGURATION_CLASSES = {
            "org.eclipse.jetty.webapp.WebInfConfiguration",
            "org.eclipse.jetty.webapp.WebXmlConfiguration",
            "org.eclipse.jetty.webapp.MetaInfConfiguration",
            "org.eclipse.jetty.webapp.FragmentConfiguration",
            "org.eclipse.jetty.plus.webapp.EnvConfiguration",
            "org.eclipse.jetty.plus.webapp.PlusConfiguration",
            "org.eclipse.jetty.webapp.JettyWebXmlConfiguration"
    };

    private Map<String, Object> environment;
    private CellEndpoint endpoint;
    private RemotePoolMonitor remotePoolMonitor;

    public WebAppHandler()
    {
        setConfigurationClasses(CONFIGURATION_CLASSES);
    }

    @Override
    public void setEnvironment(Map<String, Object> environment)
    {
        this.environment = environment;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        this.endpoint = endpoint;
    }

    public void setPoolMonitor(RemotePoolMonitor remotePoolMonitor) {
        this.remotePoolMonitor = remotePoolMonitor;
    }

    @Override
    protected void doStart() throws Exception
    {
        populateJndi();
        super.doStart();
    }

    private void populateJndi() throws NamingException
    {
        /* export to JNDI (constructor binds the entry into JNDI); all resources
         * and env entries are scoped to the webapp context
         */
        new EnvEntry(this, CELL_ENDPOINT, endpoint, true);
        new EnvEntry(this, POOL_MONITOR, remotePoolMonitor, true);

        Properties properties = new Properties();
        for (String key : environment.keySet()) {
            properties.setProperty(key, String.valueOf(environment.get(key)));
        }

        new EnvEntry(this, JNDI_ARGS, properties, true);
    }
}

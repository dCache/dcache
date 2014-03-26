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
package diskCacheV111.srm.dcache;

import org.springframework.beans.factory.annotation.Required;

import java.net.InetAddress;
import java.net.UnknownHostException;

import dmg.cells.services.login.LoginBrokerHandler;

import org.dcache.srm.SRM;

import static java.util.Arrays.asList;

public class SrmLoginBrokerHandler extends LoginBrokerHandler
{
    public SrmLoginBrokerHandler() throws UnknownHostException
    {
        setAddresses(asList(InetAddress.getAllByName(InetAddress.getLocalHost().getHostName())));
    }

    @Required
    public void setSrm(final SRM srm)
    {
        setLoad(new LoginBrokerHandler.LoadProvider() {
            @Override
            public double getLoad() {
                return srm.getLoad();
            }
        });
    }
}

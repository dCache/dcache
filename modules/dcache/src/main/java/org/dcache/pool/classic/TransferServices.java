/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.classic;

import org.springframework.beans.factory.annotation.Required;

import java.util.Map;

import diskCacheV111.vehicles.ProtocolInfo;

import org.dcache.pool.movers.MoverFactory;

public class TransferServices
{
    private MoverFactory _defaultMoverFactory;
    private Map<String, MoverFactory> _transferServices;

    @Required
    public void setDefaultFactory(MoverFactory defaultMoverFactory) {
        _defaultMoverFactory = defaultMoverFactory;
    }

    @Required
    public void setFactories(Map<String, MoverFactory> transferServices) {
        _transferServices = transferServices;
    }

    public MoverFactory getMoverFactory(ProtocolInfo info) {
        MoverFactory factory = _transferServices.get(info.getProtocol() + "-" + info.getMajorVersion());
        if (factory != null) {
            return factory;
        }
        return _defaultMoverFactory;
    }
}

/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 - 2025 Deutsches Elektronen-Synchrotron
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

import diskCacheV111.vehicles.ProtocolInfo;
import java.util.HashMap;
import java.util.Map;
import org.dcache.pool.PoolDataBeanProvider;
import org.dcache.pool.classic.json.TransferServicesData;
import org.springframework.beans.factory.annotation.Required;

public class TransferServices
      implements PoolDataBeanProvider<TransferServicesData> {

    private TransferService<?> _defaultTransferService;
    private Map<String, TransferService<?>> _transferServices;

    @Override
    public TransferServicesData getDataObject() {
        TransferServicesData info = new TransferServicesData();
        info.setLabel("Transfer Services");
        Map<String, String> map = new HashMap<>();
        _transferServices.entrySet().stream()
              .forEach((e) -> map.put(e.getKey(),
                    e.getValue().getClass().getSimpleName()));
        info.setTransferServices(map);
        return info;
    }

    public TransferService<?> getTransferService(ProtocolInfo info) {
        return _transferServices.getOrDefault(
        info.getProtocol() + "-" + info.getMajorVersion(), _defaultTransferService);
    }

    @Required
    public void setDefaultTransferService(TransferService<?> defaultTransferService) {
        _defaultTransferService = defaultTransferService;
    }

    @Required
    public void setTransferServices(Map<String, TransferService<?>> transferServices) {
        _transferServices = transferServices;
    }
}

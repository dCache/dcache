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

public class TransferServices
{
    private TransferService<?> _defaultTransferService;
    private Map<String, TransferService<?>> _transferServices;
    private PostTransferService _postTransferService;

    @Required
    public void setDefaultTransferService(TransferService<?> defaultTransferService) {
        _defaultTransferService = defaultTransferService;
    }

    @Required
    public void setTransferServices(Map<String, TransferService<?>> transferServices) {
        _transferServices = transferServices;
    }

    @Required
    public void setPostTransferService(PostTransferService postTransferService) {
        _postTransferService = postTransferService;
    }

    public PostTransferService getPostTransferService(ProtocolInfo info) {
        return _postTransferService;
    }

    public TransferService getTransferService(ProtocolInfo info) {
        TransferService service = _transferServices.get(info.getProtocol() + "-" + info.getMajorVersion());
        if (service != null) {
            return service;
        }
        return _defaultTransferService;
    }
}

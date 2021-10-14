/*
 * dCache - http://www.dcache.org/
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
package org.dcache.pinmanager;

import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellMessageReceiver;
import java.util.List;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.dcache.pinmanager.PinManagerListPinsMessage.Info;

/**
 * Process request to get a list of pins.  The subject field is used to decide which pins the user
 * is allowed to remove.
 */
public class ListRequestProcessor implements CellMessageReceiver {

    private final PinDao _dao;
    private final AuthorizationPolicy _pdp;

    public ListRequestProcessor(PinDao dao, AuthorizationPolicy pdp) {
        _dao = dao;
        _pdp = pdp;
    }

    public PinManagerListPinsMessage messageArrived(PinManagerListPinsMessage message) {
        Subject subject = message.getSubject();
        PnfsId id = message.getPnfsId();

        List<Info> info = _dao.get(_dao.where().pnfsId(id)).stream()
              .map(p -> new Info(p, _pdp.canUnpin(subject, p)))
              .collect(Collectors.toList());

        message.setInfo(info);
        return message;
    }
}

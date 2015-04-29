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
package org.dcache.cells;

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import static java.util.stream.Collectors.toList;

public class LogNoRouteToCellExceptionReceiver
    implements CellMessageReceiver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LogNoRouteToCellExceptionReceiver.class);
    private List<Class> excludedMessages = Collections.emptyList();
    private List<CellAddressCore> excludedDestinations = Collections.emptyList();

    public void messageArrived(NoRouteToCellException e)
    {
        if (isExcluded(e.getDestinationPath()) || isExcluded(e.getMessageObject())) {
            LOGGER.debug(e.getMessage());
        } else {
            LOGGER.warn(e.getMessage());
        }
    }

    private boolean isExcluded(CellPath path)
    {
        return excludedDestinations.stream().anyMatch(address -> address.equals(path.getCurrent()));
    }

    private boolean isExcluded(Serializable messageObject)
    {
        return excludedMessages.stream().anyMatch(clazz -> clazz.isInstance(messageObject));
    }

    public void setExcludedMessages(List<Class> excluded)
    {
        excludedMessages = excluded;
    }

    public void setExcludedDestinations(String excluded)
    {
        excludedDestinations =
                Splitter.on(",").omitEmptyStrings()
                        .splitToList(excluded)
                        .stream()
                        .map(CellAddressCore::new)
                        .collect(toList());
    }
}

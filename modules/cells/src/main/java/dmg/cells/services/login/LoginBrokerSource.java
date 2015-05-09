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
package dmg.cells.services.login;

import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Source of login broker information. Login broker information describes the features
 * of a door.
 */
public interface LoginBrokerSource
{
    Collection<LoginBrokerInfo> doors();

    Map<String, Collection<LoginBrokerInfo>> readDoorsByProtocol();

    Map<String, Collection<LoginBrokerInfo>> writeDoorsByProtocol();

    boolean anyMatch(Predicate<? super LoginBrokerInfo> predicate);
}

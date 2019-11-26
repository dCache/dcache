/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 - 2019 Deutsches Elektronen-Synchrotron
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
package dmg.cells.services;

import javax.annotation.concurrent.Immutable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

@Immutable
public class CoreRouteUpdate implements Serializable
{
    private static final long serialVersionUID = 5105253793542041494L;
    private final String[] exports;
    private final String[] topics;
    // as Optional class is not serializable use raw String
    private final String zone;

    public CoreRouteUpdate(Collection<String> exports, Optional<String> zone)
    {
        this(exports, Collections.emptyList(), zone);
    }

    public CoreRouteUpdate(Collection<String> exports, Collection<String> topics, Optional<String> zone)
    {
        this.exports = exports.stream().distinct().toArray(String[]::new);
        this.topics = topics.stream().distinct().toArray(String[]::new);
        this.zone = zone.orElse(null);
    }

    public Collection<String> getExports()
    {
        return unmodifiableCollection(asList(exports));
    }

    public Collection<String> getTopics()
    {
        return unmodifiableCollection(asList(topics));
    }

    public Optional<String> getZone()
    {
        return Optional.ofNullable(zone);
    }
}

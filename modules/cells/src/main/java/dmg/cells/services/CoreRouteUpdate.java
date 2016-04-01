/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

@Immutable
public class CoreRouteUpdate implements Serializable
{
    private static final long serialVersionUID = 5105253793542041494L;
    private final String[] exports;
    private final String[] topics;

    public CoreRouteUpdate(Collection<String> exports)
    {
        this(exports, Collections.emptyList());
    }

    public CoreRouteUpdate(Collection<String> exports, Collection<String> topics)
    {
        this.exports = exports.stream().distinct().toArray(String[]::new);
        this.topics = topics.stream().distinct().toArray(String[]::new);
    }

    public Collection<String> getExports()
    {
        return unmodifiableCollection(asList(exports));
    }

    public Collection<String> getTopics()
    {
        return unmodifiableCollection(asList(topics));
    }
}

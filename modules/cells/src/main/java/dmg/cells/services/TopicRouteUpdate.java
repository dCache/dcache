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
package dmg.cells.services;

import java.io.Serializable;
import java.util.Collection;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

public class TopicRouteUpdate implements Serializable
{
    private static final long serialVersionUID = 5264637613740128918L;

    private final String[] topics;

    public TopicRouteUpdate(String[] topics)
    {
        this.topics = topics;
    }

    public Collection<String> getTopics()
    {
        return unmodifiableCollection(asList(topics));
    }
}

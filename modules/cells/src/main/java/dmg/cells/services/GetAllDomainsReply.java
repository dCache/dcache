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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class GetAllDomainsReply implements Serializable {

    private static final long serialVersionUID = -1927002132633862165L;
    private final Map<String, Collection<String>> domains;

    public GetAllDomainsReply(Map<String, Collection<String>> domains) {
        this.domains = domains;
    }

    /**
     * Returns a multimap from domains to the well known cells of that domain.
     */
    public Map<String, Collection<String>> getDomains() {
        Map<String, Collection<String>> map = new HashMap<>();
        domains.forEach((k, v) -> map.put(k, new ArrayList<>(v)));
        return map;
    }
}

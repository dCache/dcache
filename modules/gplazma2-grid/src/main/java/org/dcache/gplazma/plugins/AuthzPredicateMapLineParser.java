/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.plugins;

import org.dcache.gplazma.plugins.AuthzMapLineParser.UserAuthzInformation;

/**
 * Build a PredicateMap by parsing a file line-by-line, with each line parsed
 * using a AuthzMapLineParser.
 */
public class AuthzPredicateMapLineParser
        extends PredicateMapParser<String,UserAuthzInformation> {

    private final AuthzMapLineParser parser = new AuthzMapLineParser();

    @Override
    public void accept(String line) {
        parser.parseLine(line)
                .ifPresent(i -> accept(i.getUsername()::equals, i));
    }
}

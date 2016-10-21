/* dCache - http://www.dcache.org/
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
package dmg.util.command;

import java.util.List;

import org.dcache.util.Glob;

/**
 * A class that implements this interface can expand a supplied glob
 * into a list of zero or more items.  Typically, a class implementing this
 * interface represents some namespace.
 */
public interface GlobExpander<T>
{
    /**
     * Provide a list of items that match the supplied pattern.
     * @param glob the pattern to select matching items.
     * @return the result of expanding {@literal argument}, or an empty list
     * if no matches were found.
     */
    List<T> expand(Glob glob);
}

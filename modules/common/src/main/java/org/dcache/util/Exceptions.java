/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.util;

import java.util.function.Function;

/**
 * Utility class for handling exceptions.
 */
public class Exceptions
{
    private Exceptions()
    {
        // prevent instantiation.
    }

    public static <E extends Exception> void genericCheck(boolean isOK, Function <String,E> asException,  String format, Object... arguments) throws E
    {
        if (!isOK) {
            throw asException.apply(String.format(format, arguments));
        }
    }
}

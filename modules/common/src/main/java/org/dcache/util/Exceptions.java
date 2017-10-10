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

/**
 * Utility class for handling exceptions.
 */
public class Exceptions
{
    private Exceptions()
    {
        // prevent instantiation.
    }

    /**
     * Return an Exception's message, if it was constructed with one, otherwise
     * return the Exception's class name.  This method is intended to handle
     * describing problems (e.g., for logging) identified by an Exception that
     * was created outside dCache's control.
     */
    public static String messageOrClassName(Exception e)
    {
        String message = e.getMessage();
        return message == null ? e.getClass().getName() : message;
    }
}

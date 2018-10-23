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
package org.dcache.macaroons;

import static org.dcache.util.Exceptions.genericCheck;

/**
 * An exception that indicates that the supplied macaroon is not acceptable.
 */
public class InvalidMacaroonException extends Exception
{
    /**
     * A utility method similar to Guava check* methods.
     */
    public static void checkMacaroon(boolean isOK, String message, Object... arguments)
            throws InvalidMacaroonException
    {
        genericCheck(isOK, InvalidMacaroonException::new, message, arguments);
    }

    public InvalidMacaroonException(String message) {
        super(message);
    }
}

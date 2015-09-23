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
 * Indicates that one of the supplied caveats is somehow invalid.
 */
public class InvalidCaveatException extends Exception
{
    /**
     * A utility method similar to Guava check* methods.
     */
    public static void checkCaveat(boolean isOK, String format, Object...args)
            throws InvalidCaveatException
    {
        genericCheck(isOK, InvalidCaveatException::new, format, args);
    }

    /**
     * Wrap some existing exception with an InvalidCaveatException.  The
     * supplied message is appended with information taken from the
     * cause
     */
    public static InvalidCaveatException wrap(String message, Exception cause) {
        if (cause.getMessage() == null) {
            return new InvalidCaveatException(message + ": " + cause.getClass().getSimpleName(), cause);
        } else {
            return new InvalidCaveatException(message + ": " + cause.getMessage(), cause);
        }
    }

    public InvalidCaveatException(String message) {
        super(message);
    }

    public InvalidCaveatException(String message, Throwable cause) {
        super(message, cause);
    }
}

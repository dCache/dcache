/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package org.dcache.gridsite;

import org.dcache.delegation.gridsite2.DelegationException;

/**
 * Common utility methods.
 */
public class Utilities
{
    private Utilities()
    {
        // prevent instantiation
    }

    /**
     * Useful method for checking a condition is valid and throwing a
     * DelegationException if not.
     */
    public static void assertThat(boolean isValid, String message)
            throws DelegationException
    {
        if(!isValid) {
            throw new DelegationException(message);
        }
    }

    /**
     * Useful method for checking a condition is held and throwing a
     * DelegationException if the condition isn't held.  The message is appended
     * with a phrase to identity the DN and the delegation-ID.
     */
    public static void assertThat(boolean isValid, String message,
            DelegationIdentity id) throws DelegationException
    {
        if(!isValid) {
            throw new DelegationException(message + " for " + id.getDn() +
                    " with id " + id.getDelegationId());
        }
    }
}

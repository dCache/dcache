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

import com.google.common.base.CharMatcher;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import org.dcache.delegation.gridsite2.DelegationException;

import static com.google.common.base.CharMatcher.JAVA_LETTER_OR_DIGIT;
import static com.google.common.base.CharMatcher.anyOf;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.dcache.gridsite.Utilities.assertThat;

/**
 * The identity of a delegated credential.  A delegated credential, whether
 * completed or on-going, has a unique identity.  This is based on the
 * requesting user's DN plus a delegation-ID.
 */
public class DelegationIdentity
{
    private static final HashFunction HASH_FUNCTION = Hashing.goodFastHash(32);
    private final String _dn;
    private final String _delegationId;

    private static final CharMatcher VALID_DELEGATION_ID_CHARACTERS =
            JAVA_LETTER_OR_DIGIT.or(anyOf("-_(){}[]?!%$^&*'#@~="));

    public DelegationIdentity(String dn, String delegationId) throws DelegationException
    {
        _dn = checkNotNull(dn);
        _delegationId = checkNotNull(delegationId);

        assertThat(VALID_DELEGATION_ID_CHARACTERS.matchesAllOf(delegationId),
                "delegationID \"" + delegationId + "\" is not valid");
    }

    public String getDn()
    {
        return _dn;
    }

    public String getDelegationId()
    {
        return _delegationId;
    }

    @Override
    public int hashCode()
    {
        return HASH_FUNCTION.hashString(_dn + _delegationId, UTF_8).asInt();
    }

    @Override
    public boolean equals(Object otherRaw)
    {
        if (otherRaw == this) {
            return true;
        }

        if(!(otherRaw instanceof DelegationIdentity)) {
            return false;
        }

        DelegationIdentity other = (DelegationIdentity) otherRaw;
        return _dn.equals(other._dn) && _delegationId.equals(other._delegationId);
    }

    @Override
    public String toString()
    {
        return "[" + _dn + ";" + _delegationId + "]";
    }
}

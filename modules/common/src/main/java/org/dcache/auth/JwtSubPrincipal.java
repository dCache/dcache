/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package org.dcache.auth;

/**
 * A principal that represents the JWT 'sub' claim.  The claim value is unique
 * only within the OAuth Provider, so this principal's value is a combination of
 * a unique nickname for the OP and the subject.
 * @since 5.1
 */
public class JwtSubPrincipal extends OpScopedPrincipal
{
    private static final long serialVersionUID = 1L;

    public JwtSubPrincipal(String op, String sub)
    {
        super(op, sub);
    }
}

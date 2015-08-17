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
package javatunnel.dss;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;

public class KerberosDssContext extends GssDssContext
{
    public KerberosDssContext(GSSContext context) throws GSSException
    {
        super(context);
    }

    protected Subject createSubject() throws GSSException
    {
        Set<Principal> principals = Collections.singleton(new KerberosPrincipal(context.getSrcName().toString()));
        return new Subject(false, principals, Collections.emptySet(), Collections.emptySet());
    }
}

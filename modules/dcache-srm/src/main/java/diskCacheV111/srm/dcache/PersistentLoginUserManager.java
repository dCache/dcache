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
package diskCacheV111.srm.dcache;

import com.google.common.base.Throwables;

import javax.security.auth.Subject;
import javax.sql.DataSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.Principal;
import java.security.cert.CertPath;
import java.util.HashSet;
import java.util.Set;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.srm.SRMUser;

import static diskCacheV111.srm.dcache.CanonicalizingByteArrayStore.Token;
import static java.util.Collections.emptySet;
import static java.util.Comparator.comparing;

/**
 * An SRM user manager that delegates authorization and user mapping to a {@code LoginStrategy}
 * and persists the serialized login to a database.
 *
 * Since not all principals can be accurately represented as a string, this user manager
 * relies on Java object serialization to obtain a reversible encoding of both principals
 * and login attributes. This is the primary downside of this user manager, as Java object
 * serialization is not a good candidate for long term persistence.
 *
 * Also, the Origin principal is written to the database too which means the same user
 * submitting requests from two different client nodes will get two different request IDs.
 */
public class PersistentLoginUserManager extends DcacheUserManager
{
    private static final String TYPE = "Login";

    public PersistentLoginUserManager(LoginStrategy loginStrategy, DataSource dataSource)
    {
        super(loginStrategy, dataSource, TYPE);
    }

    @Override
    protected byte[] encode(CertPath path, LoginReply reply)
    {
        return encode(reply);
    }

    @Override
    protected SRMUser decode(String clientHost, Token token, byte[] encoded)
    {
        return new DcacheUser(token, decode(encoded));
    }

    private byte[] encode(LoginReply reply)
    {
        /* Sort principals and login attributes to generate a more consistent representation.
         */
        Principal[] principals =
                reply.getSubject().getPrincipals().stream()
                        .sorted(comparing(Principal::toString))
                        .toArray(Principal[]::new);
        LoginAttribute[] attributes =
                reply.getLoginAttributes().stream()
                        .sorted(comparing(LoginAttribute::toString))
                        .toArray(LoginAttribute[]::new);

        ByteArrayOutputStream encoded = new ByteArrayOutputStream(512);
        try (ObjectOutputStream out = new ObjectOutputStream(encoded)) {
            out.writeInt(principals.length);
            for (Principal principal : principals) {
                out.writeObject(principal);
            }
            out.writeInt(attributes.length);
            for (LoginAttribute attribute : attributes) {
                out.writeObject(attribute);
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return encoded.toByteArray();
    }

    private LoginReply decode(byte[] encoded)
    {
        Set<Principal> principals = new HashSet<>();
        Set<LoginAttribute> attributes = new HashSet<>();
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(encoded))) {
            int len = in.readInt();
            if (len < 0) {
                throw new RuntimeException("Corrupt user record in database.");
            }
            for (int i = 0; i < len; i++) {
                principals.add((Principal) in.readObject());
            }
            len = in.readInt();
            if (len < 0) {
                throw new RuntimeException("Corrupt user record in database.");
            }
            for (int i = 0; i < len; i++) {
                attributes.add((LoginAttribute) in.readObject());
            }
        } catch (ClassNotFoundException | IOException e) {
            throw Throwables.propagate(e);
        }
        return new LoginReply(new Subject(false, principals, emptySet(), emptySet()), attributes);
    }
}

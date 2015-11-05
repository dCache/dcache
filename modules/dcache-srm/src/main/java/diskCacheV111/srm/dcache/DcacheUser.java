/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 Deutsches Elektronen-Synchrotron
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

import org.globus.gsi.gssapi.jaas.GlobusPrincipal;

import javax.annotation.Nonnull;
import javax.security.auth.Subject;

import java.net.InetAddress;
import java.net.UnknownHostException;

import diskCacheV111.util.FsPath;

import org.dcache.auth.LoginReply;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.Request;
import org.dcache.util.NetLoggerBuilder;

import static com.google.common.base.Preconditions.checkNotNull;
import static diskCacheV111.srm.dcache.CanonicalizingByteArrayStore.Token;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

/**
 * SRMUser adaptor for Subjects.
 *
 * Some authorization strategies will establish a new login session when loading
 * the user identity back from the SRM database. Since this login may fail, a
 * DcacheUser may represent a user that isn't logged into dCache. This will only
 * ever be the case for request owners loaded back from the database.
 *
 * The wrapper maintains a token referencing the user in the SRM database. As long
 * as this Token is referenced, the user is not eligible for garbage collection.
 */
public class DcacheUser implements SRMUser
{
    private static final ReadOnly READ_ONLY = new ReadOnly(true);

    private final Token token;
    private final Subject subject;
    private final boolean isReadOnly;
    private final FsPath root;
    private final boolean isLoggedIn;

    public DcacheUser(Token token, LoginReply login)
    {
        this.isLoggedIn = true;
        this.token = token;
        this.subject = checkNotNull(login.getSubject());
        this.isReadOnly = login.getLoginAttributes().contains(READ_ONLY);
        this.root =
                login.getLoginAttributes().stream()
                        .filter(RootDirectory.class::isInstance)
                        .findFirst()
                        .map(RootDirectory.class::cast)
                        .map(RootDirectory::getRoot)
                        .map(FsPath::new)
                        .orElseGet(FsPath::new);
    }

    public DcacheUser(Token token, GlobusPrincipal dn)
    {
        this.token = token;
        this.subject = new Subject(true, singleton(dn), emptySet(), emptySet());
        this.root = new FsPath();
        this.isReadOnly = true;
        this.isLoggedIn = false;
    }

    public DcacheUser()
    {
        this.token = null;
        this.subject = Subjects.NOBODY;
        this.root = new FsPath();
        this.isReadOnly = true;
        this.isLoggedIn = false;
    }

    boolean isLoggedIn()
    {
        return isLoggedIn;
    }

    @Override
    public int getPriority()
    {
        return 0;
    }

    @Override
    public Long getId()
    {
        return (token == null) ? null : token.getId();
    }

    @Override
    public boolean isReadOnly()
    {
        return this.isReadOnly;
    }

    @Nonnull
    public Subject getSubject()
    {
        return subject;
    }

    @Nonnull
    public FsPath getRoot()
    {
        return root;
    }

    @Override
    public String toString()
    {
        return subject.getPrincipals() + " " +
                (isReadOnly() ? "read-only" : "read-write") + " " + root;
    }

    @Override
    public String getDisplayName()
    {
        return Subjects.getDisplayName(subject);
    }

    @Override
    public boolean hasAccessTo(Request request)
    {
        DcacheUser user = (DcacheUser) request.getUser();
        if (!isLoggedIn() || !user.isLoggedIn()) {
            return false;
        }
        Subject owner = user.getSubject();
        return Subjects.hasUid(subject, Subjects.getUid(owner)) ||
               Subjects.hasGid(subject, Subjects.getPrimaryGid(owner));
    }

    @Override
    public CharSequence getDescriptiveName()
    {
        return NetLoggerBuilder.describeSubject(subject);
    }
}

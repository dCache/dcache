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

import javax.annotation.Nonnull;
import javax.security.auth.Subject;

import diskCacheV111.util.FsPath;

import org.dcache.auth.Subjects;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.Request;
import org.dcache.util.NetLoggerBuilder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * SRMUser adaptor for Subjects.
 */
public class DcacheUser implements SRMUser
{
    private final long id;
    private final Subject subject;
    private final boolean isReadOnly;
    private final FsPath root;

    public DcacheUser(long id, Subject subject, boolean isReadOnly, FsPath root)
    {
        this.id = id;
        this.subject = checkNotNull(subject);
        this.isReadOnly = isReadOnly;
        this.root = checkNotNull(root);
    }

    @Override
    public int getPriority()
    {
        return 0;
    }

    @Override
    public long getId()
    {
        return id;
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
        Subject owner = ((DcacheUser) request.getUser()).getSubject();
        return Subjects.hasUid(subject, Subjects.getUid(owner)) ||
               Subjects.hasGid(subject, Subjects.getPrimaryGid(owner));
    }

    @Override
    public CharSequence getDescriptiveName()
    {
        return NetLoggerBuilder.describeSubject(subject);
    }
}

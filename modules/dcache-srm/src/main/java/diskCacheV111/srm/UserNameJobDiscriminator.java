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
package diskCacheV111.srm;

import com.google.common.base.Strings;

import javax.annotation.Nonnull;

import diskCacheV111.srm.dcache.DcacheUser;

import org.dcache.auth.Subjects;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.strategy.UserDiscriminator;

public class UserNameJobDiscriminator extends UserDiscriminator
{
    @Nonnull
    @Override
    protected String getDiscriminatingValue(SRMUser user)
    {
        return Strings.nullToEmpty(Subjects.getUserName(((DcacheUser) user).getSubject()));
    }

    @Nonnull
    @Override
    public String getKey()
    {
        return "user";
    }
}

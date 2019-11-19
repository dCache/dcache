/*
 * dCache - http://www.dcache.org/
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
package org.dcache.srm;

import com.google.common.hash.Hashing;
import org.springframework.beans.factory.FactoryBean;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;

import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * Generate the ID that will be used to identify this srm-manager instance.  The
 * value is returned to the client as the part of the SRM request identifier
 * before the colon.
 */
public class SrmIdFactoryBean implements FactoryBean<String>, CellIdentityAware
{
    String id;

    @Override
    public void setCellAddress(CellAddressCore address)
    {
        this.id = Hashing.murmur3_32().hashString(address.toString(), US_ASCII).toString();
    }

    @Override
    public String getObject() throws Exception
    {
        return id;
    }

    @Override
    public Class<?> getObjectType()
    {
        return String.class;
    }
}

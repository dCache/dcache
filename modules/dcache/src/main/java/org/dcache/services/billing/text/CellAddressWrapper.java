/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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
package org.dcache.services.billing.text;

import dmg.cells.nucleus.CellAddressCore;

public class CellAddressWrapper
{
    private final CellAddressCore address;

    public CellAddressWrapper(String address)
    {
        this.address = new CellAddressCore(address);
    }

    public String getCell()
    {
        return address.getCellName();
    }

    public String getDomain()
    {
        return address.getCellDomainName();
    }

    public boolean isDomainAddress()
    {
        return address.isDomainAddress();
    }

    public boolean isQualified()
    {
        return !address.isLocalAddress();
    }

    @Override
    public String toString()
    {
        return address.isLocalAddress() ? address.getCellName() : address.toString();
    }
}

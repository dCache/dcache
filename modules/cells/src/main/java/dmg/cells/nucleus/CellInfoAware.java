/* dCache - http://www.dcache.org/
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
package dmg.cells.nucleus;

import java.util.function.Supplier;

/**
 * A class that implements this interface needs to know the CellInfo
 * associated with the current cell.
 */
public interface CellInfoAware
{
    /**
     * Provides a source of information about the host cell.
     *
     * Depending on the cell, a subclass of CellInfo with additional
     * information may be returned instead.
     * @param supplier An object from which a CellInfo may be requested.
     */
    void setCellInfoSupplier(Supplier<CellInfo> supplier);
}

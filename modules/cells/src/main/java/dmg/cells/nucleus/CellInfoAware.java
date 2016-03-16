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

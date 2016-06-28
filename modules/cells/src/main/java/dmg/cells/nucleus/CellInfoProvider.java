package dmg.cells.nucleus;

import java.io.PrintWriter;

/**
 * Classes implementing this interface can provide information about
 * the cell.
 *
 * The information can be provided in two ways: As clear text via the
 * cell 'info' command and in binary form via the getCellInfo()
 * method. The binary form is exposed via the 'xgetcellinfo' command.
 */
public interface CellInfoProvider
{
    /**
     * Provides information in clear text by appending it to the
     * PrintWriter.
     */
    default void getInfo(PrintWriter pw) {}

    /**
     * Provides information in binary form by updating or replacing
     * the CellInfo object. The method may return the same or a new
     * CellInfo object. It may choose to return a subclass of
     * CellInfo. Care must be taken that existing information is not
     * discarded in the process.
     */
    default CellInfo getCellInfo(CellInfo info)
    {
        return info;
    }
}

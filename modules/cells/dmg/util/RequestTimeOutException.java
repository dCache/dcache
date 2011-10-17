package dmg.util ;

import dmg.cells.nucleus.CellPath;

/**
 * @author bernardt
 * @version 0.1, 10 Oct 2011 This is an exception that is thrown when a command
 *          issued to the admin interface times out.
 */
public class RequestTimeOutException extends Exception {
    private static final long serialVersionUID = -5388852798664109692L;
    private CellPath _cellPath;

    public RequestTimeOutException()
    {
        super("Request timed out.");
    }

    public RequestTimeOutException(long timeoutMillis) {
        super("Request timed out after " + timeoutMillis / 1000 + " seconds.");
    }

    public RequestTimeOutException(long timeoutMillis, CellPath cellPath) {
        super("Request timed out after " + timeoutMillis / 1000
                + " seconds while sending message to " + cellPath);
        _cellPath = cellPath;
    }

    public CellPath getCellPath() {
        return _cellPath;
    }
}

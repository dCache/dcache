package diskCacheV111.doors;

import dmg.cells.nucleus.CellMessage;
import java.io.PrintWriter;

/**
 * DCAP protocol Interpreter. Used By doors to parse ascii commands sent over
 * the control connection.
 *
 * @since 1.9.5-3
 */
public interface DcapProtocolInterpreter {

    /**
     * Execute dcap command.
     *
     * @param command to execute
     * @return {@link String} reply
     * @throws Exception
     */
    String execute(String command) throws Exception;

    /**
     * Free allocated resources.
     */
    void close();
    void messageArrived(CellMessage msg);
    void getInfo( PrintWriter pw );
}

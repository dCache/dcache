package diskCacheV111.doors;

import java.io.PrintWriter;

import diskCacheV111.util.VspArgs;

import dmg.cells.nucleus.CellMessage;

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
     * @param {@link VspArgs} command to execute
     * @return {@link String} reply
     * @throws Exception
     */
    String execute(VspArgs command) throws Exception;

    /**
     * Free allocated resources.
     */
    void close();
    void messageArrived(CellMessage msg);
    void getInfo( PrintWriter pw );
}

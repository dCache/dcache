package diskCacheV111.doors;

import dmg.cells.nucleus.CellMessage;
import java.io.PrintWriter;

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
    public void messageArrived(CellMessage msg);
    void getInfo( PrintWriter pw );
}

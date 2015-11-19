package dmg.cells.services.login;

import java.io.IOException;

public interface InputHandler {

    /**
     * Close the stream.
     *
     * @exception  IOException  If an I/O error occurs
     */
    void close() throws IOException;

    String readLine() throws IOException;

}

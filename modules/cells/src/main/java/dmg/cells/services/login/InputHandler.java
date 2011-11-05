package dmg.cells.services.login;

import java.io.IOException;

public interface InputHandler {

    /**
     * Close the stream.
     *
     * @exception  IOException  If an I/O error occurs
     */
    public abstract void close() throws IOException;

    public abstract String readLine() throws IOException;

}
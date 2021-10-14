package dmg.protocols.telnet;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class TelnetInputStreamReader extends InputStreamReader {

    TelnetOutputStreamWriter _output;

    public TelnetInputStreamReader(InputStream input) {
        super(input);
        _output = null;
    }

    public TelnetInputStreamReader(InputStream input,
          TelnetOutputStreamWriter output) {
        super(input);
        _output = output;
    }

    @Override
    public int read(char[] cbuf, int off, int len)
          throws IOException {

        return super.read(cbuf, off, 1);

    }

}

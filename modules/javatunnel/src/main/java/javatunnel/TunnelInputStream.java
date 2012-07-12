/*
 * $Id: TunnelInputStream.java,v 1.4 2006-10-11 07:54:50 tigran Exp $
 */

package javatunnel;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

class TunnelInputStream extends InputStream {

    private InputStream _in = null;

    private Convertable _converter = null;

    private byte[] _buffer = null;

    int _pos = 0;

    public TunnelInputStream(InputStream in, Convertable converter) {
        _in = in;
        _converter = converter;
    }

    @Override
    public int read() throws java.io.IOException {

        byte b;

        if ((_buffer == null) || (_pos >= _buffer.length)) {
            try {
                _buffer = _converter.decode(_in);
            }catch(EOFException e) {
                return -1;
            }
            _pos = 0;
        }

        b = _buffer[_pos];
        ++_pos;

        return (int) b;
    }

	@Override
        public void close() throws IOException {
		_in.close();
	}

}

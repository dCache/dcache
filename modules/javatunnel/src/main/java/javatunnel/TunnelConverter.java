/*
 * $Id: TunnelConverter.java,v 1.6 2007-06-19 13:24:50 tigran Exp $
 */

package javatunnel;

import java.io.*;
import javax.security.auth.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TunnelConverter implements Convertable,UserBindible  {

    private final static Logger _log = LoggerFactory.getLogger(TunnelConverter.class);

    private boolean _isAuthentificated;
    private final static int IO_BUFFER_SIZE = 1048576; // 1 MB

    @Override
    public void encode(byte[] buf, int len, OutputStream out) throws java.io.IOException  {

        byte[] realBytes = new byte[len];

        System.arraycopy(buf, 0, realBytes, 0, len);

        String outData = "enc " + Base64.byteArrayToBase64(realBytes) + "\n";

        out.write(outData.getBytes());
    }

    @Override
    public byte[] decode(InputStream in) throws java.io.IOException {

        byte[] buf = new byte[IO_BUFFER_SIZE];
        int c;
        int total = 0;

        do {
            c = in.read();
            if (c < 0) {
                throw new EOFException("Remote end point has closed connection");
            }
            buf[total] = (byte) c;
            total++;
        } while ((c != '\n') && (c != '\r'));

        if (total < 5) {
            throw new IOException("short read: " + total + new String(buf, 0, total));
        }

        return Base64.base64ToByteArray(new String(buf, 4, total - 5));

    }

    @Override
    public boolean auth(InputStream in, OutputStream out, Object addon) {

        if( _isAuthentificated ) {
            return true;
        }

        try{

            PrintStream os = new PrintStream(out);

            String secret = "xxx >> SECRET << xxxx";
            os.println(secret);
        }catch ( Exception e ) {
            _log.error("failed auth", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean verify(InputStream in, OutputStream out, Object addon) {
        try{


            DataInputStream is;
            is = new DataInputStream(in);
            System.out.println(  is.readLine());

        }catch ( IOException e ) {
            _log.error("verify failed", e);
            return false;
        }
        return true;
    }

    @Override
    public Convertable makeCopy() throws IOException {
        return this;
    }

    @Override
    public Subject getSubject() {
        return new Subject();
    }
}

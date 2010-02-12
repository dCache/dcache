/*
 * $Id: TunnelConverter.java,v 1.6 2007-06-19 13:24:50 tigran Exp $
 */

package javatunnel;

import java.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TunnelConverter implements Convertable,UserBindible  {

    private final static Logger _log = LoggerFactory.getLogger(TunnelConverter.class);

    private boolean _isAuthentificated = false;
    private final static int IO_BUFFER_SIZE = 1048576; // 1 MB

    public void encode(byte[] buf, int len, OutputStream out) throws java.io.IOException  {
        
        byte[] realBytes = new byte[len];
                
        System.arraycopy(buf, 0, realBytes, 0, len);
        
        String outData = "enc " + Base64.byteArrayToBase64(realBytes) ;        
		
		out.write(outData.getBytes());
		out.write('\n');
        
    }
    
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
    
    public boolean auth(InputStream in, OutputStream out, Object addon) {
        
        if( _isAuthentificated ) {
            return true;
        }
        
        try{
            
            PrintStream os = null;
            DataInputStream is = null;
            os = new PrintStream(out);
            
            String secret = "xxx >> SECRET << xxxx";
            os.println(secret);
        }catch ( Exception e ) {
            _log.error("failed auth", e);
            return false;
        }
        return true;
    }
    
    public boolean verify(InputStream in, OutputStream out, Object addon) {
        try{
            
            
            DataInputStream is = null;
            is = new DataInputStream(in);                        
            System.out.println(  is.readLine());
            
        }catch ( IOException e ) {
            _log.error("verify failed", e);
            return false;
        }
        return true;
    }
    
    public String getUserPrincipal() {
        return "nobody@NOWHERE";
    }
    
    public Convertable makeCopy( ) {
        return this;
    }

	public String getGroup() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getRole() {
		// TODO Auto-generated method stub
		return null;
	}
}

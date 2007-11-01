/*
 * $Id: TunnelConverter.java,v 1.4.2.2 2007-06-26 07:36:19 tigran Exp $
 */

package javatunnel;

import java.io.*;

class TunnelConverter implements Convertable,UserBindible  {
    
    private boolean _isAuthentificated = false;
    
    public TunnelConverter() {
    }
    
    public void encode(byte[] buf, int len, OutputStream out) throws java.io.IOException  {
        
        byte[] realBytes = new byte[len];
                
        System.arraycopy(buf, 0, realBytes, 0, len);
        
        String outData = "enc " + Base64.byteArrayToBase64(realBytes) ;        
		
		out.write(outData.getBytes());
		out.write('\n');
        
    }
    
    public byte[] decode(InputStream in) throws java.io.IOException {

		byte[] buf = new byte[16384];
		int c;
		int total = 0;
		
		do {
			c = in.read();
			if ( c < 0 ) break;
			buf[total] = (byte)c;
			total++;
		}while( (c != '\n') && (c !='\r'));

	if( total < 5 ) return null;
                 		
        return Base64.base64ToByteArray( new String (buf, 4, total-5) );
        
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
            e.printStackTrace();
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

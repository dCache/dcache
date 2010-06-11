/*
 * $Id: Convertable.java,v 1.1 2002-10-14 11:53:07 cvs Exp $
 */

package javatunnel;

import java.io.InputStream;
import java.io.OutputStream;
import javax.security.auth.Subject;

interface Convertable {

    public boolean auth( InputStream in, OutputStream out, Object addon);
    public boolean verify( InputStream in, OutputStream out, Object addon);
    public void encode( byte[] buf , int len, OutputStream out) throws java.io.IOException ;
    public byte[] decode( InputStream in ) throws java.io.IOException ;
    public Subject getSubject();
    public Convertable makeCopy();
}

/*
 * $Id: Convertable.java,v 1.1 2002-10-14 11:53:07 cvs Exp $
 */

package javatunnel;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

interface Convertable {

    public boolean auth( InputStream in, OutputStream out, Object addon);
    public boolean verify( InputStream in, OutputStream out, Object addon);
    public void encode( byte[] buf , int len, OutputStream out) throws IOException ;
    public byte[] decode( InputStream in ) throws IOException ;
    public Subject getSubject();
    public Convertable makeCopy() throws IOException ;
}

/*
 * SpaceException.java
 *
 * Created on February 16, 2007, 6:09 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.services.space;

/**
 *
 * @author timur
 */
public class SpaceException extends Exception{
    
    /** Creates a new instance of SpaceException */
    public SpaceException() {
        super();
    }
    
    public SpaceException(String message) {
        super(message);
    }
    
    public SpaceException(String message,Throwable cause) {
        super(message,cause);
    }
    
    public SpaceException(Throwable cause) {
        super(cause);
    }
    
}

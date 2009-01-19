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
public class SpaceReleasedException extends SpaceException{
    
    /** Creates a new instance of SpaceException */
    public SpaceReleasedException() {
        super();
    }
    
    public SpaceReleasedException(String message) {
        super(message);
    }
    
    public SpaceReleasedException(String message,Throwable cause) {
        super(message,cause);
    }
    
    public SpaceReleasedException(Throwable cause) {
        super(cause);
    }
    
}

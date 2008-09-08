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
public class NoFreeSpaceException extends SpaceException{
    
    /** Creates a new instance of SpaceException */
    public NoFreeSpaceException() {
        super();
    }
    
    public NoFreeSpaceException(String message) {
        super(message);
    }
    
    public NoFreeSpaceException(String message,Throwable cause) {
        super(message,cause);
    }
    
    public NoFreeSpaceException(Throwable cause) {
        super(cause);
    }
    
}

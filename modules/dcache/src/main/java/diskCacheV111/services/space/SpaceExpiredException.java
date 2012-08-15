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
public class SpaceExpiredException extends SpaceException{

    private static final long serialVersionUID = 4785214084431499268L;

    /** Creates a new instance of SpaceException */
    public SpaceExpiredException() {
        super();
    }

    public SpaceExpiredException(String message) {
        super(message);
    }

    public SpaceExpiredException(String message,Throwable cause) {
        super(message,cause);
    }

    public SpaceExpiredException(Throwable cause) {
        super(cause);
    }

}

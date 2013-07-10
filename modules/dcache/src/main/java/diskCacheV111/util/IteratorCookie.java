// $Id: IteratorCookie.java,v 1.3 2004-11-09 08:04:48 tigran Exp $

package diskCacheV111.util ;


import java.io.Serializable;

public class IteratorCookie implements Serializable {

    private static final long serialVersionUID = -8975020858113782371L;

    public boolean done(){ return false ; }
    public boolean invalidated(){ return false ; }
}

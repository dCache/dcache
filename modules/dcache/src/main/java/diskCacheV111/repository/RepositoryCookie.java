
// $Id: RepositoryCookie.java,v 1.3 2004-11-09 08:04:46 tigran Exp $

package diskCacheV111.repository ;

import java.io.Serializable;

import diskCacheV111.util.IteratorCookie;

public class RepositoryCookie
       extends    IteratorCookie
       implements Serializable {

   private boolean _valid = true ;

   private static final long serialVersionUID = -4614545184199940148L;

   @Override
   public boolean done(){ return true ; }
   public void setInvalidated(){ _valid = false ; }
   @Override
   public boolean invalidated(){ return ! _valid ; }



}

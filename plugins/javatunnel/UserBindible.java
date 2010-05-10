/*
 * $Id: UserBindible.java,v 1.1 2006-09-05 13:19:53 tigran Exp $
 */

package javatunnel;

import java.util.List;


public interface UserBindible {

    public String getUserPrincipal();
    public List<String> getRoles();

}
/*
 * $Log: not supported by cvs2svn $
 */

/*
 * $Id: UserBindible.java,v 1.1 2006-09-05 13:19:53 tigran Exp $
 */

package javatunnel;

import javax.security.auth.Subject;


public interface UserBindible {

    Subject getSubject();
}

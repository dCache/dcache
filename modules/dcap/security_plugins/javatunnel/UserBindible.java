/*
 * $Id: UserBindible.java,v 1.1.2.1 2006-09-07 07:36:59 tigran Exp $
 */

package javatunnel;


public interface UserBindible {

    public String getUserPrincipal();
	public String getGroup();
	public String getRole();
	
}
/*
 * $Log: not supported by cvs2svn $
 * Revision 1.1  2006/09/05 13:19:53  tigran
 * added concept of user,role and group
 *
 */

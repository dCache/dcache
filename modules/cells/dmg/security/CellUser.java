package dmg.security;


import java.security.Principal;


public class CellUser implements  Principal, java.io.Serializable {

	private static final long serialVersionUID = 7437573242083470794L;	
	
	private String _name = null;
	private String _group = null;
	private String _role = null;
		
	public CellUser(String name, String group, String role) {
		_name = name;
		_group = group;
		_role = role;		
	}	
	
	
	public String getName() {
		return _name;
	}

	public void setName(String newName) {
		_name = newName;
	}
	
	public String getGroup() {
		return _group;
	}
	
	public void setGroup(String newGroup) {
		_group = newGroup;
	}
	
	public String getRole() {
		return _role;
	}
	
	public void setRole( String newRole) {
		_role = newRole;
	}	
	
	
	public String toString() {
		return  _name + ":" + _role + ":" + _group;
	}
	
}

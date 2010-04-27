/*
 * VOInfo.java
 *
 * Created on October 24, 2006, 4:18 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.util;

/**
 *
 * @author timur
 */
public class VOInfo implements java.io.Serializable{
    static final long serialVersionUID = -8014669884189610627L;

    private String voGroup;
    private String voRole;

    /** Creates a new instance of VOInfo */
    public VOInfo(String voGroup,String voRole) {
        if(voGroup == null) {
            throw new IllegalArgumentException("null group");
        }
        this.voGroup = voGroup;
        this.voRole = voRole;
    }

    public String toString(){
        return voGroup+":"+voRole;
    }

    public String getVoGroup() {
        return voGroup;
    }

    public String getVoRole() {
        return voRole;
    }

    public int hashCode(){
        return voGroup.hashCode() ^ voRole.hashCode();
    }

    public boolean equals(Object o) {
        if(o == null || !(o instanceof VOInfo )){
            return false;
        }
        VOInfo voinfo = (VOInfo) o;
        return voGroup.equals(voinfo.voGroup) && voRole.equals(voinfo.voRole) ;
    }

}

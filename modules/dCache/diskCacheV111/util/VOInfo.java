/*
 * VOInfo.java
 *
 * Created on October 24, 2006, 4:18 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.util;
import org.dcache.util.Glob;


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

    public boolean match(final String group,
                         final String role) {
        Glob groupPattern = new Glob(voGroup);
        if (voRole!=null) {
            Glob rolePattern  = new Glob(voRole);
            return (groupPattern.matches((group!=null ? group : "null"))&&
                    rolePattern.matches((role!=null  ? role  : "null")));
        }
        else {
            return groupPattern.matches((group!=null ? group : "null"));
        }
    }

    public static boolean  match(final VOInfo info,
                                 final String group,
                                 final String role)  {
        return info.match(group,role);
    }

    public static void main(String argv[]) {

        String g="*";
        String r="*";
        VOInfo i1 = new VOInfo(g,r);
        String ug="group";
        String ur="role";
        System.out.println("matching ("+ug+","+ur+") to ("+g+","+r+") :" + i1.match(ug,ur));

        g="gr*";
        r="r*le";
        i1 = new VOInfo(g,r);
        ug="group";
        ur="role";
        System.out.println("matching ("+ug+","+ur+") to ("+g+","+r+") :" + i1.match(ug,ur));

        g="*";
        r="r*ule";
        i1 = new VOInfo(g,r);
        ug=null;
        ur=null;
        System.out.println("matching ("+ug+","+ur+") to ("+g+","+r+") :" + i1.match(ug,ur));


        g="cmsatlas";
        r=null;
        i1 = new VOInfo(g,r);
        ug="cmsatlas";
        ur=null;
        System.out.println("matching ("+ug+","+ur+") to ("+g+","+r+") :" + i1.match(ug,ur));


    }

}

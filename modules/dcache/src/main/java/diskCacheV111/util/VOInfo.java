package diskCacheV111.util;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.dcache.util.Glob;

public class VOInfo implements Serializable
{
    private static final long serialVersionUID = -8014669884189610627L;

    private static Pattern p1 = Pattern.compile( "(.*)/Role=(.*)");
    private static Pattern p2 = Pattern.compile( "(.*)()");

    private String voGroup;
    private String voRole;

    public VOInfo(String pattern) {
        Matcher m = getMatcher(pattern);
        voGroup = m.group(1);
        voRole = mapRoleToGlob( m.group(2));
    }


    /**
     * Map a role-part of a user-supplied pattern to a Glob.
     * <p>
     * If the user supplied a pattern without a role part (e.g., <tt>/ops</tt>)
     * then we treat this as if the user supplied a pattern with a wildcard
     * role (e.g., <tt>/ops/Role=*</tt>)
     * <p>
     * This should not be needed as <tt>voms-proxy-init</tt> will always
     * returns all subgroups a user is a member of.
     * <p>
     * In an ideal world, this method would be an identity transform, as it is
     * with the group-part of the user-supplied pattern.
     * <p>
     * In fact, this method is needed to hide a bug in gPlazma's
     * getFQANSfromVOMSAttributes method in X509Utils.  This method is to
     * be reviewed (and removed) once this bug is fixed.
     */
    private String mapRoleToGlob( String roleInFqan) {
        return roleInFqan.isEmpty() ? "*" : roleInFqan;
    }

    private Matcher getMatcher( String pattern) {
        Matcher m = p1.matcher( pattern);
        if( m.matches()) {
            return m;
        }

        m = p2.matcher( pattern);
        if( m.matches()) {
            return m;
        }

        throw new RuntimeException("Failed to find a matcher for FQAN pattern: " + pattern);
    }

    public VOInfo(String voGroup,String voRole) {
        this.voGroup = voGroup;
        this.voRole = voRole;
    }

    @Override
    public String toString(){
        return voGroup+":"+voRole;
    }

    @Nullable
    public String getVoGroup() {
        return voGroup;
    }

    @Nullable
    public String getVoRole() {
        return voRole;
    }

    @Override
    public int hashCode(){
        return Objects.hashCode(voGroup) ^ Objects.hashCode(voRole);
    }

    @Override
    public boolean equals(Object o) {
        if(o == null || !(o instanceof VOInfo )){
            return false;
        }
        VOInfo voinfo = (VOInfo) o;
        return Objects.equals(voGroup, voinfo.voGroup) && Objects.equals(voRole, voinfo.voRole);
    }

    public boolean match(final String group, final String role)
    {
        if (voGroup == null) {
            return false;
        }

        boolean roleMatches;

        if( voRole != null) {
            Glob rolePattern  = new Glob(voRole);
            roleMatches = rolePattern.matches(role==null ? "null" : role);
        } else {
            roleMatches = true;
        }

        Glob groupPattern = new Glob(voGroup);
        return groupPattern.matches(group==null ? "null" : group) && roleMatches;
    }
}

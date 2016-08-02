/*
 * FQAN.java
 *
 * Created on October 4, 2007, 2:59 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.auth;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author timur
 */
public class FQAN implements Serializable {

    private static final long serialVersionUID = -2212735007788920585L;

    private static final String NULL_CAPABILITY = "/Capability=NULL";
    private static final String NULL_ROLE = "/Role=NULL";

    /* In general, the regular expressions (REs) are taken from:
     *
     *    http://edg-wp2.web.cern.ch/edg-wp2/security/voms/edg-voms-credential.pdf
     *
     * Changes and clarification from that document:
     *
     * 1. The document states that the RE for RFC 1035 is:
     *
     *    ^([a-z]([a-z0-9-]*[a-z0-9])*\.)*[a-z]{2,4}$
     *
     * However, this is at odds with RFC 1035 section 2.3.1, which describes
     * a simpler format.  Here we use:
     *
     *    <label>(\.<label>)*
     *
     * where <label> is:
     *
     *     [a-z]([a-z0-9-]*[a-z0-9])?
     *
     * 2. In the document, the RE for a valid capability value is written as
     *
     *     [\w_-]
     *
     * where \w is defined as:
     *
     *     [a-zA-Z0-9_]
     *
     * which is in keeping with Java Pattern definition for \w.  Note that
     * [\w_-] is equivalent to [\w-].
     */
   private static final String RE_VALID_LABEL = "[a-z]([a-z0-9-]*[a-z0-9])?";
   private static final String RE_RFC_1035_VALUE = RE_VALID_LABEL + "(\\." + RE_VALID_LABEL + ")*";
   private static final String RE_VALID_VO_VALUE = '/' + RE_RFC_1035_VALUE;
   private static final String RE_VALID_GROUP_VALUE = "(/[\\w-]+)*";
   private static final String RE_VALID_ROLE_VALUE = "[\\w-]+";
   private static final String RE_VALID_CAPABILITY_VALUE = "[\\w-]+";

   private static final String RE_VALID_FQAN = '^' +
                                               RE_VALID_VO_VALUE + '(' + RE_VALID_GROUP_VALUE + ")?" +
                                               "(/Role=" + RE_VALID_ROLE_VALUE + ")?" +
                                               "(/Capability=" + RE_VALID_CAPABILITY_VALUE + ")?$";

   private static final Pattern VALID_FQAN = Pattern.compile( RE_VALID_FQAN);

    private static final Pattern p1 = Pattern.compile("(.*)/Role=(.*)/Capability=(.*)");
    private static final Pattern p2 = Pattern.compile("(.*)/Role=(.*)(.*)");
    private static final Pattern p3 = Pattern.compile("(.*)()/Capability=(.*)");
    private static final Pattern p4 = Pattern.compile("(.*)(.*)(.*)");
    private transient Matcher m;

    //immutable
    private final String fqan;

    /** Identify if given String is a valid FQAN */
    public static boolean isValid( String fqan) {
        if( fqan == null) {
            return false;
        }

        Matcher fqanMatcher = VALID_FQAN.matcher( fqan);
        return fqanMatcher.matches();
    }

    /** Creates a new instance of FQAN */
    public FQAN(String fqan) {
        if(fqan ==null ){
            throw new IllegalArgumentException("fqan is null");
        }

        fqan = filterOutNullCapability(fqan);
        fqan = filterOutNullRole( fqan);

        this.fqan = fqan;
    }

    private static String filterOutNullCapability( String originalFqan) {
        String filteredFqan;

        if( originalFqan.endsWith( NULL_CAPABILITY)) {
            int newLength = originalFqan.length() - NULL_CAPABILITY.length();
            filteredFqan = originalFqan.substring( 0, newLength);
        } else {
            filteredFqan = originalFqan;
        }

        return filteredFqan;
    }

    private static String filterOutNullRole( String originalFqan) {
        int index = originalFqan.indexOf( NULL_ROLE);

        if( index == -1) {
            return originalFqan;
        }

        String filteredFqan = originalFqan.substring(0, index) +
                              originalFqan.substring(index + NULL_ROLE.length());

        return filteredFqan;
    }

    @Override
    public String toString() {
        return fqan;
    }

    private Matcher getMatcher() {
        if (m!=null) {
            return m;
        }

        m = p1.matcher(fqan);
        if(!m.matches()) {
            m = p2.matcher(fqan);
            if(!m.matches()) {
                m = p3.matcher(fqan);
                if(!m.matches()) {
                    m = p4.matcher(fqan);
                    m.matches();
                }
            }
        }
        return m;
    }

    public String getGroup() {
        return getMatcher().group(1);
    }

    public String getRole() {
        return getMatcher().group(2);
    }

    public boolean hasRole() {
        String role = getRole();
        return !role.isEmpty();
    }

    public String getCapability() {
        return getMatcher().group(3);
    }

    public boolean hasCapability() {
        String capability = getCapability();
        return !capability.isEmpty();
    }

    public FQAN getParent()
    {
        int i = fqan.lastIndexOf('/');
        return (i > 1) ? new FQAN(fqan.substring(0, i)) : null;
    }

    @Override
    public int hashCode() {
        return fqan.hashCode();
    }

    @Override
    public boolean equals(Object testFQAN) {
        if (testFQAN == this) {
            return true;
        }
        if (!(testFQAN instanceof FQAN)){
            return false;
        }
        FQAN otherFQAN = (FQAN) testFQAN;
        return this.fqan.equals(otherFQAN.fqan);
    }



    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        FQAN fqan = new FQAN(args[0]);
        System.out.println("FQAN     = "+fqan);
        System.out.println("Group    = "+fqan.getGroup());
        System.out.println("Role     = "+fqan.getRole());
        System.out.println("Capacity = "+fqan.getGroup());

    }

}

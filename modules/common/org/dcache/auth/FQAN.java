/*
 * FQAN.java
 *
 * Created on October 4, 2007, 2:59 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.auth;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 *
 * @author timur
 */
public class FQAN implements java.io.Serializable {

    static final long serialVersionUID = -2212735007788920585L;

    private static final String NULL_CAPABILITY = "/Capability=NULL";
    private static final String NULL_ROLE = "/Role=NULL";

    private static Pattern p1 = Pattern.compile("(.*)/Role=(.*)/Capability=(.*)");
    private static Pattern p2 = Pattern.compile("(.*)/Role=(.*)(.*)");
    private static Pattern p3 = Pattern.compile("(.*)()/Capability=(.*)");
    private static Pattern p4 = Pattern.compile("(.*)(.*)(.*)");
    private transient Matcher m;

    //immutable
    private final String fqan;

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

        StringBuilder filteredFqan = new StringBuilder();

        filteredFqan.append( originalFqan.substring( 0, index));
        filteredFqan.append( originalFqan.substring( index + NULL_ROLE.length()));

        return filteredFqan.toString();
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

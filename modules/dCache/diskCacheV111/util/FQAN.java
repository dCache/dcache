/*
 * FQAN.java
 *
 * Created on October 4, 2007, 2:59 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.util;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 *
 * @author timur
 */
public class FQAN implements java.io.Serializable {

    static final long serialVersionUID = -2212735007788920585L;
    private static Pattern p1 = Pattern.compile("(.*)/Role=(.*)/Capability=(.*)");
    private static Pattern p2 = Pattern.compile("(.*)/Role=(.*)(.*)");
    private static Pattern p3 = Pattern.compile("(.*)(.*)(.*)");
    private transient Matcher m;

    //immutable
    private final String fqan;
    /** Creates a new instance of FQAN */
    public FQAN(String fqan) {
        if(fqan ==null ){
            throw new IllegalArgumentException("fqan is null");
        }
        this.fqan = fqan;
    }

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
                m.matches();
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

    public String getCapability() {
        return getMatcher().group(3);
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

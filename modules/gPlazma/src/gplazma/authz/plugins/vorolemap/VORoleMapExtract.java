/*
 * VORoleMapExtract.java
 *
 * Created on March 29, 2005
 */

package gplazma.authz.plugins.vorolemap;

import org.glite.voms.FQAN;

//import org.opensciencegrid.authz.ac.FQAN;


/**
 *
 *  @author Abhishek Singh Rana
 */

public class VORoleMapExtract {

        public String subjectDN = null;
        public FQAN[] fqanArray = null;

        //constructor
        public VORoleMapExtract(String subjectDnReturned, FQAN[] fqanReturned) {
        	subjectDN = subjectDnReturned;
			fqanArray = fqanReturned;
        }
}

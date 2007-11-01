/*
 * gPLAZMAliteFineGrainAuthzExtract.java
 *
 * Created on March 29, 2005
 */
                                                                                                                                                                                                     
package gplazma.gplazmalite.gridVORolemapService;

import org.glite.security.voms.*;
import org.glite.security.voms.ac.*;
import java.util.*;
//import org.opensciencegrid.authz.ac.FQAN;
                                                                                                                                                                                                     
                                                                                                                                                                                                     
/**
 *
 *  @author Abhishek Singh Rana
 */
                                                                                                                                                                                                     
public class gPLAZMAliteFineGrainAuthzExtract {
                                                                                                                                                                                                     
        public String subjectDN = null;
        public FQAN[] fqanArray = null;
                                                                                                                                                                                                     
        //constructor
        public gPLAZMAliteFineGrainAuthzExtract (String subjectDnReturned, FQAN[] fqanReturned) {
        	subjectDN = subjectDnReturned;
			fqanArray = fqanReturned;
        }
}

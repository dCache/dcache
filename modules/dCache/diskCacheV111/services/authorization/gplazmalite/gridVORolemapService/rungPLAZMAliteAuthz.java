package gplazma.gplazmalite.gridVORolemapService;

import java.util.*;
import java.io.*;
import java.lang.*;

public class rungPLAZMAliteAuthz {

 private static String AuthzConfPath;
 private static String dn;
 private static String fqan;
 private static gPLAZMAliteGridVORoleAuthz gplRoleAuthz;


 public static void main(String[] args) {
      /**                                                                                                                                                                                               
      if(args.length != 3) {
         System.out.println("Usage: thisfilename <path to map> <dn> <fqan>");
         return;
      }**/

      AuthzConfPath = (args[0]); //System.out.println("AuthzConfPath :" +AuthzConfPath);
      dn = (args[1]);             //System.out.println("dn :" +dn);
      if (args.length == 3) {fqan = (args[2]);}	//System.out.println("fqan :" +fqan);

      try {
            gplRoleAuthz = new gPLAZMAliteGridVORoleAuthz(AuthzConfPath);
      }
      catch(java.io.IOException ioe) {
            System.err.println("Exception in gPLAZMAliteGridVORoleAuthz instantiation:" + ioe);
      }
	  	dn = dn.trim();
		if (fqan == null) fqan="";
		fqan = fqan.trim();
		String gridFineGrainIdentity = dn.concat(fqan);
		//System.out.println("gridFineGrainIdentity being passed to service:" +gridFineGrainIdentity);

      try {
	  	String gotName = gplRoleAuthz.getMappedUsername(gridFineGrainIdentity);
		
           System.out.println(" Username received as:        " +gotName);
      } catch(Exception e) {
         System.err.println("Exception:" + e);
      }

 }
}

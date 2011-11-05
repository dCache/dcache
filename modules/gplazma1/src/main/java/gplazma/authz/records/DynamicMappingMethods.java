package gplazma.authz.records;

import gplazma.authz.plugins.dynamic.UIDMapFileHandler;
import gplazma.authz.plugins.dynamic.GIDMapFileHandler;

public class DynamicMappingMethods {

  public static Integer hashcode_dn(String subjectDN, String role) {
    return getUnixIDfromHashCode(subjectDN.hashCode());
  }

  public static Integer hashcode_role(String subjectDN, String role) {
    return getUnixIDfromHashCode(role.hashCode());
  }

  public static Integer hashcode_dnrole(String subjectDN, String role) {
    return getUnixIDfromHashCode((subjectDN+role).hashCode());
  }

  public static Integer getUnixIDfromString(String name) {
    return getUnixIDfromHashCode(name.hashCode());
  }

  public static Integer getUnixIDfromHashCode(int i) {
    return Integer.valueOf(Math.abs(i));
  }

  public static Long getLongUnixIDfromHashCode(int i) {
    return Long.valueOf(Integer.toBinaryString(i), 2);
  }

  public static String sequential_dn(String subjectDN, String role) {
    return null;
  }

  public static String sequential_role(String subjectDN, String role) {
    return null;
  }

  public static String sequential_dnrole(String subjectDN, String role) {
    return null;
  }

  public static String gums_dn(String subjectDN, String role) {
    return null;
  }

  public static String gums(String subjectDN, String role) {
    return gums_dnrole(subjectDN, role);
  }

  public static String gums_dnrole(String subjectDN, String role) {
    return null;
  }

  public static String dn_uidmap(String subjectDN, String role) throws Exception {

    UIDMapFileHandler gridmapServ = null;
    String UID;

    try {
      gridmapServ = new UIDMapFileHandler("/etc/grid-security/grid-uidmap");
    } catch(Exception ase) {
	  System.out.println("Exception in reading grid-uidmap configuration file");
      System.out.println("/etc/grid-security/grid-uidmap" + " " + ase);
      throw ase;
    }

	try {
	  UID = gridmapServ.getMappedUID(subjectDN);
	} catch(Exception e) {
      throw e;
    }

    System.out.println("Subject DN " + subjectDN + " is mapped to UID: " + UID);

    if (UID == null) {
	  String denied = /*DENIED_MESSAGE + */": Cannot determine UID from grid-uidmap file for DN " + subjectDN;
      System.out.println(denied);
      throw new Exception("Cannot determine UID from grid-uidmap file for DN " + subjectDN);
    }

    return UID;
  }

  public static String role_gidmap(String subjectDN, String role) throws Exception {

    GIDMapFileHandler gridmapServ = null;
    String GID;

    try {
      gridmapServ = new GIDMapFileHandler("/etc/grid-security/grid-gidmap");
    } catch(Exception ase) {
	  System.out.println("Exception in reading grid-gidmap configuration file");
      System.out.println("/etc/grid-security/grid-gidmap" + " " + ase);
      throw ase;
    }

	try {
	  GID = gridmapServ.getMappedGID(role);
	} catch(Exception e) {
      throw e;
    }

    System.out.println("Role " + role + " is mapped to GID: " + GID);

    if (GID == null) {
	  String denied = /*DENIED_MESSAGE + */": Cannot determine GID from grid-gidmap file for role " + role;
      System.out.println(denied);
      throw new Exception("Cannot determine GID from grid-gidmap file for role " + role);
    }

    return GID;
  }

  public static String regular_expression(String re, String input) {


      //Pattern.compile(id_method).matcher(gid_str).matches()

      /*
        if(record==null) {
        Iterator keys = auth_records.keySet().iterator();
        while(keys.hasNext()) {
          String key = (String) keys.next();
          Pattern p = Pattern.compile(key);
          Matcher m = p.matcher(username);
          if(m.matches()) {
            record = (gPlazmaAuthorizationRecord) auth_records.get(key);
            String UID_str = (String) auth_uid_hash.get(key);
            String GID_str = (String) auth_gid_hash.get(key);
            int gct = m.groupCount() + 1;
            for(int i=1; i<gct; i++) {
              String brf = "\\$" + String.valueOf(i);
              if(UID_str!=null) UID_str = UID_str.replaceAll(brf, m.group(i));
              if(GID_str!=null) GID_str = GID_str.replaceAll(brf, m.group(i));
              record.Home = record.Home.replaceAll(brf, m.group(i));
              record.Root = record.Root.replaceAll(brf, m.group(i));
              record.FsRoot = record.FsRoot.replaceAll(brf, m.group(i));
            }
            if(!Pattern.compile("^dynamic.*").matcher(username.toLowerCase()).matches()) {
            if(UID_str!=null) record.UID = 	Integer.valueOf(UID_str).intValue();
            if(GID_str!=null) record.GID = 	Integer.valueOf(GID_str).intValue();
            }
            break;
          }
        }
  */

    return null;
  }
}

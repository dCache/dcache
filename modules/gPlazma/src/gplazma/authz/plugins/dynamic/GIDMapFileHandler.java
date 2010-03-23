package gplazma.authz.plugins.dynamic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.io.*;
import java.lang.*;
import java.text.MessageFormat;

public class GIDMapFileHandler {

  private static final Logger log = LoggerFactory.getLogger(GIDMapFileHandler.class);
  private static final String GIDMAP_FILENAME ="grid-gidmap";
  private HashMap gidMap = new HashMap();
  private static HashMap gidMap_static;
  private static long prev_refresh_time=0;
  public static final String capnull = "/Capability=NULL";
  public static final int capnulllen = capnull.length();
  public static final String rolenull ="/Role=NULL";
  public static final int rolenulllen = rolenull.length();

  public GIDMapFileHandler(String filename)
  	throws IOException {
     	try {
        	String fileSeparator = System.getProperty("file.separator");
        	String[] testFilenamePath = filename.split(fileSeparator);
        	String testFilename = testFilenamePath[testFilenamePath.length-1];
         	if (!testFilename.equals(GIDMAP_FILENAME)) {
           		log.warn("The grid-gidmap filename " + testFilename + " is not as expected.");
				log.warn("WARNING: Possible security violation.");
        	}
		} catch(Exception se) {
        	log.error("Exception in testing file: " +se);
     	}
        log.debug("GIDMapFileHandler reading " + filename);
        read(filename);

  }

  private synchronized void read(String filename) throws IOException {
      long current_time = System.currentTimeMillis();
      File config = new File(filename);
      boolean readable = config.canRead() || prev_refresh_time==0;
      if(!readable) log.warn(MessageFormat.format("WARNING: Could not read grid-gidmap. Will use cached copy.", filename));
      if(readable && config.lastModified() >= prev_refresh_time) {
        FileReader fr = new FileReader(config);
        BufferedReader reader = new BufferedReader(fr);
        try {
          read(reader);
        } finally {
          reader.close();
        }
        prev_refresh_time = current_time;
        gidMap_static = gidMap;
      } else {
        gidMap = gidMap_static;
      }
  }

  private void read(BufferedReader reader)
	throws IOException {
        boolean eof = false;
        String line;
        while((line = reader.readLine()) != null) {
            String thisLine = line.trim();
			if(thisLine.length()==0 || thisLine.charAt(0) != '\"') {
				continue;
			}
			thisLine=thisLine.substring(1);
			int last_quote = thisLine.lastIndexOf('\"');
			if(last_quote == -1) {
				continue;
			}
			String gridRole = thisLine.substring(0,last_quote);
			String mappedGID = thisLine.substring(last_quote+1).trim();
			if(mappedGID != null && mappedGID.length() >0) {
				fillMap(gridRole, mappedGID);
			}
		}
	}

	private void fillMap(String role, String username) {
		String mapValue = username.trim();
		String mapKey = role.trim();
			gidMap.put(mapKey,mapValue);
	}

	private String removeQuotes(String quotedString)
	throws Exception {
		if (quotedString != null) {
			quotedString = quotedString.replace('\"',' ').trim();
		}
		return quotedString;
	}

	//public String getMappedGID(String gridRole)
	//throws Exception {
	//	return removeQuotes((String) gidMap.get(gridRole));
	//}

  public String getMappedGID(String gridRole) throws Exception {
    String GID;
    try {
        GID = (String) gidMap.get(gridRole);
        if(GID==null) {
          // Remove "/Capability=NULL" and "/Role=NULL"
          if(gridRole.endsWith(capnull))
            gridRole = gridRole.substring(0, gridRole.length() - capnulllen);
          if(gridRole.endsWith(rolenull))
            gridRole = gridRole.substring(0, gridRole.length() - rolenulllen);
          GID = (String) gidMap.get(gridRole);
        }
    } catch (Exception e) {
      throw e;
    }

    return removeQuotes(GID);
  }

} //end of GIDMapFileHandler
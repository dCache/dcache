/*
 * VORoleMapHandler.java
 *
 * Created on March 29, 2005
 */

package gplazma.authz.plugins.vorolemap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.io.*;
import java.lang.*;

import gplazma.authz.AuthorizationController;

/**
 *
 *  @author Abhishek Singh Rana
 */

public class VORoleMapHandler {
  private static final Logger log = LoggerFactory.getLogger(VORoleMapHandler.class);
  private static String logpattern = "%d{MM/dd HH:mm:ss,SSS} %C{1} authRequestID ";

  private static final String GRID_VOROLE_AUTHZ_DB="grid-vorolemap";
  private HashMap gridFineGrainMap = new HashMap();
  private HashMap<String, LinkedList<String>> gridFineGrainMapMultiple = new HashMap();
  private static HashMap gridFineGrainMap_static;
  private static HashMap<String, LinkedList<String>> gridFineGrainMapMultiple_static;
  private static long prev_refresh_time=0;

  public VORoleMapHandler(String filename, long authRequestID)
  	throws IOException {
         try {
        	String fileSeparator = System.getProperty("file.separator");
        	String[] testFilenamePath = filename.split(fileSeparator);
        	String testFilename = testFilenamePath[testFilenamePath.length-1];
         	if (!testFilename.equals(GRID_VOROLE_AUTHZ_DB)) {
           	log.warn("The Grid-VO-Role database name " + testFilename + " is not as expected.");
				    log.warn("WARNING: Possible security violation.");
        	}
		} catch(Exception se) {
        	log.error("Exception in testing file: " +se);
     	}
    log.debug("VORoleMapHandler reading " + filename);
    read(filename);
  }

  private synchronized void read(String filename) throws IOException {
      long current_time = System.currentTimeMillis();
      File config = new File(filename);
      boolean readable = config.canRead() || prev_refresh_time==0;
      if(!readable) log.warn("WARNING: Could not read grid-vorolemap file " + filename + ". Will use cached copy.");
      if(readable && config.lastModified() >= prev_refresh_time) {
        FileReader fr = new FileReader(config);
        BufferedReader reader = new BufferedReader(fr);
        try {
          read(reader);
        } finally {
          reader.close();
        }
        prev_refresh_time = current_time;
        gridFineGrainMap_static = gridFineGrainMap;
        gridFineGrainMapMultiple_static = gridFineGrainMapMultiple;
      } else {
        gridFineGrainMap = gridFineGrainMap_static;
        gridFineGrainMapMultiple = gridFineGrainMapMultiple_static;
      }
  }

  private void read(BufferedReader reader)
	throws IOException {
        boolean eof = false;
        String line;

      while((line = reader.readLine()) != null) {
      String thisLine = line.trim();
      if(thisLine.length()==0) continue;
      if(thisLine.charAt(0)=='*') thisLine = "\"*\"" + thisLine.substring(1);
      else if(thisLine.charAt(0) != '\"') continue;

			thisLine = thisLine.substring(1);
			int secondQuote = thisLine.indexOf('\"',0);
			if(secondQuote == -1) {
				continue;
			}
			String gridSubjectDN = thisLine.substring(0, secondQuote).trim();
			int thirdQuote = thisLine.indexOf('\"', secondQuote+1);

			String mappedUsername = "" ;
			String gridVORole = "" ;

			if(thirdQuote == -1) {
					mappedUsername = thisLine.substring(secondQuote+1).trim();
			}
			else {
				int fourthQuote = thisLine.indexOf('\"', thirdQuote+1);
				if(fourthQuote == -1) {
					continue;
				}
				gridVORole = thisLine.substring(thirdQuote+1, fourthQuote).trim();
				mappedUsername = thisLine.substring(fourthQuote+1).trim();
			}

      String gridFineGrainIdentity = gridSubjectDN.concat(gridVORole);
			if(mappedUsername != null && !mappedUsername.equals("")) {
				fillMap(gridFineGrainIdentity, mappedUsername);
			}
		}
	}

	private void fillMap(String gridFineGrainIdentity, String username) {
		String mapValue = username;
		String mapKey = gridFineGrainIdentity;
		gridFineGrainMap.put(mapKey,mapValue);
    LinkedList mapValues = gridFineGrainMapMultiple.get(mapKey);
    if(mapValues==null) {
      mapValues = new LinkedList<String> ();
      gridFineGrainMapMultiple.put(mapKey,mapValues);
    }
    mapValues.add(mapValue);
  }

	private String removeQuotes(String quotedString)
	throws Exception {
		if (quotedString != null) {
			quotedString = quotedString.replace('\"',' ').trim();
		}
		return quotedString;
	}

	public String getMappedUsername(String gridFineGrainSecureId)
	throws Exception {
		gridFineGrainSecureId = gridFineGrainSecureId.trim();
		return removeQuotes((String)gridFineGrainMap.get(gridFineGrainSecureId));
	}

	public LinkedList<String> getMappedUsernames(String gridFineGrainSecureId)
	throws Exception {
		gridFineGrainSecureId = gridFineGrainSecureId.trim();
		return gridFineGrainMapMultiple.get(gridFineGrainSecureId);
	}

	public String getMappedUsername(String gridFineGrainSecureId, String username)
	throws Exception {
		gridFineGrainSecureId = gridFineGrainSecureId.trim();
    LinkedList<String> mapValues = getMappedUsernames(gridFineGrainSecureId);
    if(mapValues!=null && mapValues.contains(username)) {
      return username;
    } else if(mapValues!=null && mapValues.contains("\""+username+"\"")) {
      return username;
    } else {
      return null;
    }

  }

} //end of VORoleMapHandler

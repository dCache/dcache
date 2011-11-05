package gplazma.authz.plugins.dynamic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.io.*;
import java.lang.*;
import java.text.MessageFormat;

public class UIDMapFileHandler {

  private static final Logger log = LoggerFactory.getLogger(UIDMapFileHandler.class);
  private static final String UIDMAP_FILENAME ="grid-uidmap";
  private HashMap uidMap = new HashMap();
  private static HashMap uidMap_static;
  private static long prev_refresh_time=0;

  public UIDMapFileHandler(String filename)
  	throws IOException {
     	try {
        	String fileSeparator = System.getProperty("file.separator");
        	String[] testFilenamePath = filename.split(fileSeparator);
        	String testFilename = testFilenamePath[testFilenamePath.length-1];
         	if (!testFilename.equals(UIDMAP_FILENAME)) {
           		log.warn("The grid-uidmap filename " + testFilename + " is not as expected.");
				log.warn("WARNING: Possible security violation.");
        	}
		} catch(Exception se) {
        	log.error("Exception in testing file: " +se);
     	}
        log.debug("UIDMapFileHandler reading " + filename);
        read(filename);

  }

  private synchronized void read(String filename) throws IOException {
      long current_time = System.currentTimeMillis();
      File config = new File(filename);
      boolean readable = config.canRead() || prev_refresh_time==0;
      if(!readable) log.warn(MessageFormat.format("WARNING: Could not read grid-uidmap. Will use cached copy.", filename));
      if(readable && config.lastModified() >= prev_refresh_time) {
        FileReader fr = new FileReader(config);
        BufferedReader reader = new BufferedReader(fr);
        try {
          read(reader);
        } finally {
          reader.close();
        }
        prev_refresh_time = current_time;
        uidMap_static = uidMap;
      } else {
        uidMap = uidMap_static;
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
			String gridSubjectDN = thisLine.substring(0,last_quote);
			String mappedUID = thisLine.substring(last_quote+1).trim();
			if(mappedUID != null && mappedUID.length() >0) {
				fillMap(gridSubjectDN, mappedUID);
			}
		}
	}

	private void fillMap(String subjectDN, String username) {
		String mapValue = username.trim();
		String mapKey = subjectDN.trim();
			uidMap.put(mapKey,mapValue);
	}

	private String removeQuotes(String quotedString)
	throws Exception {
		if (quotedString != null) {
			quotedString = quotedString.replace('\"',' ').trim();
		}
		return quotedString;
	}

	public String getMappedUID(String gridDN)
	throws Exception {
		return removeQuotes((String) uidMap.get(gridDN));
	}

} //end of UIDMapFileHandler

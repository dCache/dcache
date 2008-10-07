/*
 * GridMapFileHandler.java
 *
 * Created on March 29, 2005
 */

package gplazma.authz.plugins.gridmapfile;

import org.apache.log4j.*;

import java.util.*;
import java.io.*;
import java.lang.*;
import java.text.MessageFormat;

import gplazma.authz.AuthorizationController;

/**
 *
 *  @author Abhishek Singh Rana
 */

public class GridMapFileHandler {
  static Logger log = Logger.getLogger(GridMapFileHandler.class.getSimpleName());
  private static String logpattern = "%d{MM/dd HH:mm:ss,SSS} %C{1} authRequestID ";

    private static final String GRIDMAP_FILENAME="grid-mapfile";
	private HashMap gridMap = new HashMap();
	private static HashMap gridMap_static;
  private static long prev_refresh_time=0;

  public GridMapFileHandler(String filename, long authRequestID)
  	throws IOException {
      String authRequestID_str = AuthorizationController.getFormattedAuthRequestID(authRequestID);
          if(log.getAppender("GridMapFileHandler")==null) {
            Enumeration appenders = log.getParent().getAllAppenders();
            while(appenders.hasMoreElements()) {
                Appender apnd = (Appender) appenders.nextElement();
                if(apnd instanceof ConsoleAppender)
                    apnd.setLayout(new PatternLayout(logpattern + authRequestID_str + "  %m%n"));
          }
        }
      try {
        	String fileSeparator = System.getProperty("file.separator");
        	String[] testFilenamePath = filename.split(fileSeparator);
        	String testFilename = testFilenamePath[testFilenamePath.length-1];
         	if (!testFilename.equals(GRIDMAP_FILENAME)) {
           		System.out.println("The grid-mapfile filename " + testFilename + " is not as expected.");
				System.out.println("WARNING: Possible security violation.");
        	}
		} catch(Exception se) {
        	System.err.println("Exception in testing file: " +se);
     	}
		read(filename);

  }

  public void setLogLevel(Level level) {
        log.setLevel(level);
  }

  private synchronized void read(String filename) throws IOException {
      long current_time = System.currentTimeMillis();
      File config = new File(filename);
      boolean readable = config.canRead() || prev_refresh_time==0;
      if(!readable) System.out.println(MessageFormat.format("WARNING: Could not read grid-mapfile. Will use cached copy.", filename));
      if(readable && config.lastModified() >= prev_refresh_time) {
        FileReader fr = new FileReader(config);
        BufferedReader reader = new BufferedReader(fr);
        try {
          read(reader);
        } finally {
          reader.close();
        }
        prev_refresh_time = current_time;
        gridMap_static = gridMap;
      } else {
        gridMap = gridMap_static;
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
			String mappedUsername = thisLine.substring(last_quote+1).trim();
			if(mappedUsername != null && mappedUsername.length() >0) {
				fillMap(gridSubjectDN, mappedUsername);
			}
		}
	}	
	
	private void fillMap(String subjectDN, String username) {
		String mapValue = username.trim();
		String mapKey = subjectDN.trim();
			gridMap.put(mapKey,mapValue);
	}

	private String removeQuotes(String quotedString)
	throws Exception {
		if (quotedString != null) {
			quotedString = quotedString.replace('\"',' ').trim();
		}
		return quotedString;
	}	
	
	public String getMappedUsername(String gridDN) 
	throws Exception {
		return removeQuotes((String)gridMap.get(gridDN));
	}	

} //end of GridMapFileHandler

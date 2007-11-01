/*
 * GridMapFileAuthzService.java
 *
 * Created on March 29, 2005
 */
                                                                                                                                                                                                     
package gplazma.gplazmalite.gridmapfileService;

import java.util.*;
import java.io.*;
import java.lang.*;

/**
 *
 *  @author Abhishek Singh Rana
 */

public class GridMapFileAuthzService {

	private static final String GRIDMAP_FILENAME="grid-mapfile";
	private HashMap gridMap = new HashMap();
	private static HashMap gridMap_static;
  private static long prev_refresh_time=0;

  public GridMapFileAuthzService (String filename)
  	throws IOException {
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
		//System.out.println("- - -                 Activating     g P L A Z M A - l i t e    Suite                 - - - ");
		//System.out.println("- - -                   Built-in Legacy Grid Authorization Service                    - - - ");

    read(filename);

  }

  private synchronized void read(String filename) throws IOException {
      long current_time = System.currentTimeMillis();
      File config = new File(filename);
      boolean readable = config.canRead();
      if(!readable) System.out.println("WARNING: Could not read grid-mapfile " + filename + ". Will try to use cached copy.");
      if(readable && config.lastModified() > prev_refresh_time) {
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
			if(mappedUsername != null && mappedUsername != "") {
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
		//System.out.println("- - -                Deactivating    g P L A Z M A - l i t e    Suite                 - - - ");
		return removeQuotes((String)gridMap.get(gridDN));
	}	

} //end of GridMapFileAuthzService

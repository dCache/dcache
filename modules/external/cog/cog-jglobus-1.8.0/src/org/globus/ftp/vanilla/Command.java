/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.ftp.vanilla;

/**
 * Represents an FTP Control Channel Command
 */
public class Command {

    public static final Command FEAT = new Command("FEAT");
    public static final Command ABOR = new Command("ABOR");
    public static final Command CDUP = new Command("CDUP");
    public static final Command PWD  = new Command("PWD");
    public static final Command QUIT = new Command("QUIT");
    public static final Command PASV = new Command("PASV");
    public static final Command SPAS  = new Command("SPAS");
    public static final Command EPSV = new Command("EPSV");


    ///////////////////////////////////////
    // attributes

    private String name;
    private String parameters;

    ///////////////////////////////////////
    // operations


    /**
     * @param name the command name, eg. "PUT"
     * @param parameters the command parameters; in other words everything that
     * is contained between the space after the command name and the trailing 
     * Telnet EOL, excluding both the mentioned space and EOL. For instance,
     * in command "STOR /tmp/file.txt\r\n", the parameters would be:
     * "/tmp/file.txt"
     * and trailing EOL.
     */
    public  Command(String name, String parameters) 
	throws IllegalArgumentException{
	initialize(name, parameters);
    } // end Command        
    
    public  Command(String name) 
	throws IllegalArgumentException{
	initialize(name, null);
    }
    
    private void initialize(String name, String parameters) 
	throws IllegalArgumentException {
        if (name == null) {
	    throw new IllegalArgumentException("null name");
	}
	if (parameters != null && parameters.endsWith(FTPControlChannel.CRLF)) {
	    throw new IllegalArgumentException("parameters end with EOL");
	}
        this.name = name;
	this.parameters = parameters;
    } // end initialize

    /**
     * @return a String representation of this object, that is 
     * <name> <sp> <parameters> <CRLF>
     * </p>
     */
    public static String toString(Command command) {       
	return command.toString();
    }

    public String toString() {
	if (parameters == null) {
	    return name + FTPControlChannel.CRLF;
	} else {
	    return name + " " + parameters + FTPControlChannel.CRLF;
	}
    }

} // end Command




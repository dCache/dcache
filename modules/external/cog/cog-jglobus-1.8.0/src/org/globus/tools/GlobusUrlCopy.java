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
package org.globus.tools;

import org.globus.io.urlcopy.UrlCopy;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.IdentityAuthorization;
import org.globus.util.GlobusURL;

/** globus-url-copy command line tool.
*/
public class GlobusUrlCopy {

    private static final String message =
        "\n" +
        "Syntax: java GlobusUrlCopy [options] fromURL toURL\n" +
        "        java GlobusUrlCopy -help\n\n" +
        "\tOptions\n" +
        "\t-s  <subject> | -subject <subject>\n" +
        "\t      Use this subject to match with both the source \n" + 
        "\t      and destination servers\n" +
        "\t-ss <subject> | -source-subject <subject>\n" +
        "\t      Use this subject to match with the source server\n" +
        "\t-ds <subject> | -dest-subject <subject>\n" +
        "\t      Use this subject to match with the destination server\n" +
        "\t-notpt | -no-third-party-transfers\n" +
        "\t      Turn third-party transfers off (on by default)\n" +
        "\t-nodcau | -no-data-channel-authentication\n" +
        "\t      Turn off data channel authentication for ftp transfers\n" +
        "\t      Applies to FTP protocols only.\n" + 
        "\t-tcp-bs <size> | -tcp-buffer-size <size>\n" +
        "\t      Specifies the size (in bytes) of the TCP buffer to be\n" +
        "\t      used by the underlying FTP data channels.\n" +
        "\t-no-allo\n" +
        "\t      Turn off the use of size pre-allocation if uploading\n" +
        "\t      files to a GridFTP server\n" +
        "\n" +
        "\tProtocols supported:\n" +
        "\t- gass (http and https)\n" +
        "\t- ftp (ftp and gsiftp)\n" +
        "\t- file\n";
    
    public static void main(String[] args) {
        
        Authorization srcAuth = null;
        Authorization dstAuth = null;
        boolean append = false;
        boolean error  = false;
        boolean debug  = false;

        boolean thirdPartyTransfer = true;
        boolean dcau = true;
        boolean disableAllo = false;
        int tcpBufferSize = 0;
        
        int argv = args.length-2;
        if (args.length == 1) argv = 1;
        
        for (int i = 0; i < argv; i++) {
            if (args[i].equalsIgnoreCase("-notpt") ||
                args[i].equalsIgnoreCase("-no-thid-party-transfer")) {
                thirdPartyTransfer = false;
            } else if (args[i].equalsIgnoreCase("-nodcau") ||
                       args[i].equalsIgnoreCase("-no-data-channel-authentication")) {
                dcau = false;
            } else if (args[i].equalsIgnoreCase("-debug")) {
                debug = true;
            } else if (args[i].equalsIgnoreCase("-ss")) {
                if (i+1 == argv) {
                    System.out.println("Error: -ss requires an argument");
                    error = true;
                    break;
                } else {
                    srcAuth = new IdentityAuthorization(args[++i]);
                }
            } else if (args[i].equalsIgnoreCase("-ds")) {
                if (i+1 == argv) {
                    System.out.println("Error: -ds requires an argument");
                    error = true;
                    break;
                } else {
                    dstAuth = new IdentityAuthorization(args[++i]);
                }
            } else if (args[i].equalsIgnoreCase("-s")) {
                if (i+1 == argv) {
                    System.out.println("Error: -s requires an argument");
                    error = true;
                    break;
                } else {
                    String ident = args[++i];
                    srcAuth = new IdentityAuthorization(ident);
                    dstAuth  = new IdentityAuthorization(ident);
                }
            } else if (args[i].equalsIgnoreCase("-tcp-bs") ||
                       args[i].equalsIgnoreCase("-tcp-buffer-size")) {
                if (i+1 == argv) {
                    System.out.println("Error: " + args[i] + " requires an argument");
                    error = true;
                    break;
                } else {
                    try {
                        tcpBufferSize = Integer.parseInt(args[++i]);
                        if (tcpBufferSize < 0) {
                            throw new NumberFormatException();
                        }
                    }
                    catch (NumberFormatException e) {
                        System.out.println("Error: the TCP buffer size must be a positive integer");
                        error = true;
                        break;
                    }
                }
            } else if (args[i].equalsIgnoreCase("-no-allo")) {
                disableAllo = true;
            } else if (args[i].equalsIgnoreCase("-help") ||
                       args[i].equalsIgnoreCase("-usage")) {
                System.out.println(message);
                System.exit(1);
            } else {
                System.err.println("Error: option not supported: " + args[i]);
                error = true;
                break;
            } 
        }
        
        if (args.length == 0 || error) {
            System.err.println("\nSyntax: java GlobusUrlCopy [options] fromURL toURL\n");
            System.err.println("Use -help to display full usage.");
            System.exit(1);
        }
        
        String fromURL = args[ args.length - 2];
        String toURL   = args[ args.length - 1];
        
        try {
            GlobusURL from = new GlobusURL(fromURL);
            GlobusURL to   = new GlobusURL(toURL);
            
            if (debug) {
                System.out.println("From: " + fromURL);
                System.out.println(from);
                
                System.out.println("To: " + toURL);
                System.out.println(to);
                
                System.out.print("Third party transfer: ");
                if (thirdPartyTransfer) {
                    System.out.println("yes");
                } else {
                    System.out.println("no");
                }
                
            }
            
            UrlCopy uc = new UrlCopy();
            uc.setSourceUrl(from);
            uc.setDestinationUrl(to);
            uc.setUseThirdPartyCopy(thirdPartyTransfer);
            uc.setDCAU(dcau);
            uc.setSourceAuthorization(srcAuth);
            uc.setDestinationAuthorization(dstAuth);
            uc.setDisableAllo(disableAllo);
            if (tcpBufferSize != 0) {
                uc.setTCPBufferSize(tcpBufferSize);
            }
            
            uc.copy();
            
        } catch(Exception e) {
            System.err.println("GlobusUrlCopy error: " + e.getMessage());
            if (debug) {
                e.printStackTrace();
            }
        }
        
    }
    
}

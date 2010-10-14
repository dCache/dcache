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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.security.PrivateKey;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;

import org.globus.gsi.CertUtil;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.OpenSSLKey;
import org.globus.gsi.bc.BouncyCastleOpenSSLKey;
import org.globus.gsi.bc.BouncyCastleCertProcessingFactory;
import org.globus.gsi.gssapi.auth.IdentityAuthorization;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.util.Util;
import org.globus.common.CoGProperties;
import org.globus.common.Version;
import org.globus.myproxy.CredentialInfo;
import org.globus.myproxy.ChangePasswordParams;
import org.globus.myproxy.DestroyParams;
import org.globus.myproxy.InitParams;
import org.globus.myproxy.GetParams;
import org.globus.myproxy.GetTrustrootsParams;
import org.globus.myproxy.InfoParams;
import org.globus.myproxy.StoreParams;

import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSCredential;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSException;

/** MyProxy Command Line Client */
public class MyProxy {

    public static final int MYPROXY_SERVER_PORT   = 7512;
    public static final int PORTAL_LIFETIME_HOURS = 12;
    public static final int CRED_LIFETIME_HOURS   = 168;
    
    public static final int MATCH_CN_ONLY = 0;
    public static final int REGULAR_EXP = 1;
    
    private static final String commonOptions = 
        "\tCommon Options:\n" +
        "\t-help\n" +
        "\t\tDisplays usage\n" +
        "\t-v | -version\n" +
        "\t\tDisplays version\n" +
        "\n" +
        "\t-h <host> | -host <host>\n" +
        "\t\tHostname of the myproxy-server\n" +
        "\t-p <port> | -port <port>\n" +
        "\t\tPort of the myproxy-server\n" +
        "\t\t(default " + MYPROXY_SERVER_PORT + ")\n" + 
        "\t-s <subject> | -subject <subject>\n" +
        "\t\tPerforms subject authorization\n" +
        "\t-l <username> | -username <username>\n" +
        "\t\tUsername for the delegated proxy\n" +
        "\t-d | -dn_as_username\n" +
        "\t\tUse the proxy certificate subject (DN) as the default\n" +
        "\t\tusername instead of the \"user.name\" system property.\n" +
        "\t-S | -stdin_pass\n" +
        "\t\tAllows passphrase from stdin.\n";

    private static final String message =
        "\n" +
        "Syntax: java MyProxy [common options] command [command options]\n" +
        "        java MyProxy -version\n" +
        "        java MyProxy -help\n\n" +
        commonOptions +
        "\n" +
        "\tCommands:\n" +
        "\t put            - put proxy\n" +
        "\t store          - store credentials\n" +
        "\t get            - get proxy\n" +
        "\t anonget        - get proxy without local credentials\n" + 
        "\t get-trustroots - get trustroots information\n" +
        "\t destroy        - remove proxy\n" +
        "\t info           - credential information\n" +
        "\t pwd            - change credential password\n\n" +
        "\tSpecify -help after a command name for command-specific help.\n";

    private static final String destroyMessage =
        "\n" +
        "Syntax: java MyProxy [common options] destroy [command options]\n\n" +
        commonOptions +
        "\n" + 
        "\tCommand Options:\n" +
        "\t-help\n" +
        "\t\tDisplays usage\n" +
        "\t-k | -credname <name>\n" +
        "\t\tSpecifies credential name\n";

    private static final String pwdMessage =
        "\n" +
        "Syntax: java MyProxy [common options] pwd [command options]\n\n" +
        commonOptions +
        "\n" +
        "\tCommand Options:\n" +
        "\t-help\n" +
        "\t\tDisplays usage\n" +
        "\t-k | -credname <name>\n" +
        "\t\tSpecifies credential name\n";

    private static final String infoMessage =
        "\n" +
        "Syntax: java MyProxy [common options] info [command options]\n\n" +
        commonOptions +
        "\n" +
        "\tCommand Options:\n" +
        "\t-help\n" +
        "\t\tDisplays usage\n";

    private static final String getMessage =
        "\n" +
        "Syntax: java MyProxy [common options] [get|anonget] [command options]\n\n" +
        commonOptions +
        "\n" +
        "\tCommand Options:\n" +
        "\t-help\n" +
        "\t\tDisplays usage\n" +
        "\t-t <hours> | -portal_lifetime <hours>\n" +
        "\t\tLifetime of delegated proxy on\n" +
        "\t\tthe portal (default " + PORTAL_LIFETIME_HOURS + " hours)\n" +
        "\t-o | -out <path>\n" +
        "\t\tLocation of delegated proxy\n" +
        "\t-T | -trustroots\n" +
        "\t\tManage trust roots\n" +
        "\t-k | -credname <name>\n" +
        "\t\tSpecifies credential name\n" +
        "\t-a | -authorization <path>\n" +
        "\t\tSpecifies path to credentials to renew\n";

    private static final String putMessage =
        "\n" +
        "Syntax: java MyProxy [common options] put [command options]\n\n" +
        commonOptions +
        "\n" +
        "\tCommand Options:\n" +
        "\t-help\n" +
        "\t\tDisplays usage\n" +
        "\t-cert <certfile>\n" +
        "\t\tNon-standard location of user certificate\n" +
        "\t-key <keyfile>\n" +
        "\t\tNon-standard location of user key\n" +
        "\t-t <hours> | -portal_lifetime <hours>\n" +
        "\t\tLifetime of delegated proxy on\n" +
        "\t\tthe portal (default " + PORTAL_LIFETIME_HOURS + " hours)\n" +
        "\t-c <hours> | -cred_lifetime <hours> \n" +
        "\t\tLifetime of delegated proxy\n" +
        "\t\t(default 1 week - 168 hours)\n" +
        "\t-a | -allow_anonymous_retrievers\n" +
        "\t\tAllow credentials to be retrieved with just username/passphrase\n" +
        "\t-A | -allow_anonymous_renewers\n" +
        "\t\tAllow credentials to be renewed by any client (not recommended)\n" +
        "\t-r | -retrievable_by <dn>\n" +
        "\t\tAllow specified entity to retrieve credential\n" +
        "\t-R | -renewable_by <dn>\n" +
        "\t\tAllow specified entity to renew credential\n" +
        "\t-x | -regex_dn_match\n" +
        "\t\tSpecifies that the DN used by  options -r and -R\n" +
        "\t\twill be matched as a regular expression\n " +
        "\t-X | -match_cn_only\n" +
        "\t\tSpecifies  that  the  DN  used by options -r and -R\n" +
        "\t\twill be matched against the Common Name (CN) of the\n" +
        "\t\tsubject\n" +
        "\t-n | -no_passphrase\n" +
        "\t\tDisable passphrase authentication\n" +
        "\t-k | -credname <name>\n" +
        "\t\tSpecifies credential name\n" +
        "\t-K | -creddesc <desc>\n" +
        "\t\tSpecifies credential description\n";

    private static final String storeMessage =
        "\n" +
        "Syntax: java MyProxy [common options] store [command options]\n\n" +
        commonOptions +
        "\n" +
        "\tCommand Options:\n" +
        "\t-help\n" +
        "\t\tDisplays usage\n" +
        "\t-cert <certfile>\n" +
        "\t\tNon-standard location of user certificate\n" +
        "\t-key <keyfile>\n" +
        "\t\tNon-standard location of user key\n" +
        "\t-t <hours> | -portal_lifetime <hours>\n" +
        "\t\tLifetime of delegated proxy on\n" +
        "\t\tthe portal (default " + PORTAL_LIFETIME_HOURS + " hours)\n" +
        "\t-a | -allow_anonymous_retrievers\n" +
        "\t\tAllow credentials to be retrieved with just username/passphrase\n" +
        "\t-A | -allow_anonymous_renewers\n" +
        "\t\tAllow credentials to be renewed by any client (not recommended)\n" +
        "\t-r | -retrievable_by <dn>\n" +
        "\t\tAllow specified entity to retrieve credential\n" +
        "\t-R | -renewable_by <dn>\n" +
        "\t\tAllow specified entity to renew credential\n" +
        "\t-x | -regex_dn_match\n" +
        "\t\tSpecifies that the DN used by  options -r and -R\n" +
        "\t\twill be matched as a regular expression\n " +
        "\t-X | -match_cn_only\n" +
        "\t\tSpecifies  that  the  DN  used by options -r and -R\n" +
        "\t\twill be matched against the Common Name (CN) of the\n" +
        "\t\tsubject\n" +
        "\t-k | -credname <name>\n" +
        "\t\tSpecifies credential name\n" +
        "\t-K | -creddesc <desc>\n" +
        "\t\tSpecifies credential description\n";

    private static final String getTrustrootsMessage =
        "\n" +
        "Syntax: java MyProxy [common options] get-trustroots [command options]\n\n" +
        commonOptions +
        "\n" +
        "\tCommand Options:\n" +
        "\t-help\n" +
        "\t\tDisplays usage\n";

    private int port = MYPROXY_SERVER_PORT;
    private String hostname;
    private String username;
    private String subjectDN;
    private boolean debug = false;
    private boolean dnAsUsername = false;
    private boolean stdin = false;
    private boolean wantTrustroots = false;

    protected void parseCmdLine(String [] args) {
        for (int i = 0; i < args.length; i++) {
            
            if (args[i].charAt(0) != '-') {

                CertUtil.init();

                if (args[i].equalsIgnoreCase("get")) {
                    doGet(args, i+1, false);
                } else if (args[i].equalsIgnoreCase("anonget")) {
                    doGet(args, i+1, true);
                } else if (args[i].equalsIgnoreCase("get-trustroots")) {
                    doGetTrustroots(args, i+1, true);
                } else if (args[i].equalsIgnoreCase("put")) {
                    doPut(args, i+1);
                } else if (args[i].equalsIgnoreCase("store")) {
                    doStore(args, i+1);
                } else if (args[i].equalsIgnoreCase("destroy")) {
                    doDestroy(args, i+1);
                } else if (args[i].equalsIgnoreCase("info")) {
                    doInfo(args, i+1);
                } else if (args[i].equalsIgnoreCase("pwd")) {
                    doChangePassword(args, i+1);
                } else {
                    error("Error: unknown command (" + args[i] +")");
                }
            } else if (args[i].equals("-h") || 
                       args[i].equalsIgnoreCase("-host")) {
                ++i;
                if (i == args.length) {
                    error("Error: -h requires hostname");
                } else {
                    this.hostname = args[i]; 
                }
            } else if (args[i].equals("-p") ||
                       args[i].equalsIgnoreCase("-port")) {
                ++i;
                if (i == args.length) {
                    error("Error: -p requires port number");
                } else {
                    this.port = Integer.parseInt(args[i]);
                }
            } else if (args[i].equals("-l") ||
                       args[i].equalsIgnoreCase("-username")) {
                ++i;
                if (i == args.length) {
                    error("Error: -l requires username");
                } else {
                    this.username = args[i]; 
                }
            } else if (args[i].equals("-d") ||
                       args[i].equalsIgnoreCase("-dn_as_username")) {
                this.dnAsUsername = true;
            } else if (args[i].equalsIgnoreCase("-debug")) {
                this.debug = true;
            } else if (args[i].equals("-S") ||
                       args[i].equalsIgnoreCase("-stdin_pass")) {
                this.stdin = true;
            } else if (args[i].equals("-s") ||
                       args[i].equalsIgnoreCase("-subject")) {
                ++i;
                if (i == args.length) {
                    error("Error: -subject requires an argument");
                } else {
                    this.subjectDN = args[i];
                }
            } else if (args[i].equals("-v") ||
                       args[i].equalsIgnoreCase("-version")) {
                
                // display version info
                System.out.println(Version.getVersion());
                System.exit(1);
            } else if (args[i].equalsIgnoreCase("-help") ||
                       args[i].equalsIgnoreCase("-usage")) {
                
                System.err.println(message);
                System.exit(1);
            } else {
                error("Error: argument #" + i + " (" + args[i] +") : unknown");
            }
        }
        error("Error: No command specified");
    }
    
    private String getUsername() {
        if (dnAsUsername) {
            GSSCredential cred = getDefaultCredential();
            try {
                return cred.getName().toString();
            } catch (Exception e) {
                exit("Failed to get credential name: " + e.getMessage(), e);
            }
        } if (this.username == null) {
            return System.getProperty("user.name");
        } else {
            return this.username;
        }
    }

    private void verifyCommonCmdLine() {
        if (this.hostname == null) {
            error("Error: myproxy-server hostname not specified");
        }
    }

    private org.globus.myproxy.MyProxy getMyProxy() {
        org.globus.myproxy.MyProxy myProxy = 
            new org.globus.myproxy.MyProxy(this.hostname, 
                                           this.port); 
        if (this.subjectDN != null) {
            myProxy.setAuthorization(new IdentityAuthorization(this.subjectDN));
        }
        return myProxy;
    }

    protected void doInfo(String args[], int start) {

        for (int i=start;i<args.length;i++) {
            if (args[i].equalsIgnoreCase("-help") ||
                args[i].equalsIgnoreCase("-usage")) {
                System.err.println(infoMessage);
                System.exit(1);
            } else {
                error("Error: info argument #" + i + " (" + args[i] + 
                      ") : unknown");
            }
        }

        verifyCommonCmdLine();

        // load default proxy...
        GSSCredential credential = getDefaultCredential();
        InfoParams infoRequest = new InfoParams();
        infoRequest.setUserName(getUsername());
        infoRequest.setPassphrase("DUMMY-PASSPHRASE");

        String tmp;

        try {
            CredentialInfo[] info = getMyProxy().info(credential, 
                                                      infoRequest);
            
            System.out.println ("Owner: " + info[0].getOwner());
            for (int i=0;i<info.length;i++) {
                tmp = info[i].getName();
                System.out.println ((tmp == null) ? "default:" : tmp +":");
                System.out.println ("\tStart Time  : " + 
                                    info[i].getStartTime());
                System.out.println ("\tEnd Time    : " + 
                                    info[i].getEndTime());
                    
                long now = System.currentTimeMillis();
                if (info[i].getEndTime() > now) {
                    System.out.println ("\tTime left   : " +
                                        Util.formatTimeSec((info[i].getEndTime() - now)/1000));
                } else {
                    System.out.println ("\tTime left   : expired");
                }

                tmp = info[i].getRetrievers();
                if (tmp != null) {
                    System.out.println ("\tRetrievers  : "+tmp);
                }
                tmp = info[i].getRenewers();
                if (tmp != null) {
                    System.out.println ("\tRenewers    : "+tmp);
                }
                tmp = info[i].getDescription();
                if (tmp != null) {
                    System.out.println ("\tDescription : "+tmp);
                }
            }
        } catch(Exception e) {
            exit("Error: " + e.getMessage(), e);
        }
        exit();
    }

    protected void doDestroy(String args[], int start) {
        String credName = null;
        
        for (int i=start;i<args.length;i++) {
            if (args[i].equals("-k") ||
                args[i].equalsIgnoreCase("-credname")) {
                ++i;
                if (i == args.length) {
                    error("Error: -k requires credential name");
                } else {
                    credName = args[i];
                }
            } else if (args[i].equalsIgnoreCase("-help") ||
                       args[i].equalsIgnoreCase("-usage")) {
                System.err.println(destroyMessage);
                System.exit(1);
            } else {
                error("Error: destroy argument #" + i + " (" + args[i] + 
                      ") : unknown");
            }
        }

        verifyCommonCmdLine();

        GSSCredential credential = getDefaultCredential();
        DestroyParams destroyRequest = new DestroyParams();
        destroyRequest.setUserName(getUsername());
        destroyRequest.setCredentialName(credName);
        destroyRequest.setPassphrase("DUMMY-PASSPHRASE");
        
        try {
            getMyProxy().destroy(credential, destroyRequest);
            
            System.out.println("A proxy was succesfully destroyed for user " + 
                               getUsername() + ".");
        } catch(Exception e) {
            exit("Error: " + e.getMessage(), e);
        }
        exit();
    }

    protected void doChangePassword(String args[], int start) {
        String credName = null;
        
        for (int i=start;i<args.length;i++) {
            if (args[i].equals("-k") ||
                args[i].equalsIgnoreCase("-credname")) {
                ++i;
                if (i == args.length) {
                    error("Error: -k requires credential name");
                } else {
                    credName = args[i];
                }
            } else if (args[i].equalsIgnoreCase("-help") ||
                       args[i].equalsIgnoreCase("-usage")) {
                System.err.println(pwdMessage);
                System.exit(1);
            } else {
                error("Error: pwd argument #" + i + " (" + args[i] + 
                      ") : unknown");
            }
        }

        verifyCommonCmdLine();

        // load default proxy...
        GSSCredential credential = getDefaultCredential();

        ChangePasswordParams request =
            new ChangePasswordParams();
        request.setUserName(getUsername());
        request.setCredentialName(credName);

        String prompt1 = "Enter credential pass phrase: ";

        String password = (this.stdin) ?
            Util.getInput(prompt1) : Util.getPrivateInput(prompt1);
        if (password == null) return;
        request.setPassphrase(password);

        String prompt2 = "Enter new pass phrase: ";
        password = (this.stdin) ?
            Util.getInput(prompt2) : Util.getPrivateInput(prompt2);
        if (password == null) return;
        request.setNewPassphrase(password);
        
        try {
            getMyProxy().changePassword(credential,
                                        request);
            
            System.out.println("Password changed successfully.");
        } catch(Exception e) {
            exit("Error: " + e.getMessage(), e);
        }
        exit();
    }

    protected void doGet(String args[], int start, boolean anonymous) {
        String credName = null;
        int lifetime = PORTAL_LIFETIME_HOURS * 3600;
        String outputFile = null;
        GSSCredential authzcreds = null;

        for (int i=start;i<args.length;i++) {
            if (args[i].equals("-k") ||
                args[i].equalsIgnoreCase("-credname")) {
                ++i;
                if (i == args.length) {
                    error("Error: -k requires credential name");
                } else {
                    credName = args[i];
                }
            } else if (args[i].equals("-o") ||
                       args[i].equalsIgnoreCase("-out")) {
                ++i;
                if (i == args.length) {
                    error("Error: -o requires an argument");
                } else {
                    outputFile = args[i];
                }
            } else if (args[i].equals("-t") ||
                       args[i].equalsIgnoreCase("-portal_lifetime")) {
                ++i;
                if (i == args.length) {
                    error("Error: -t requires time argument in hours");
                } else {
                    lifetime = Integer.parseInt(args[i]) * 3600;
                }
            } else if (args[i].equals("-a") ||
                       args[i].equalsIgnoreCase("-authorization")) {
                ++i;
                if (i == args.length) {
                    error("Error: -a requires an argument");
                } else {
                    try {
                        GlobusCredential pkiCred = 
                            new GlobusCredential(args[i]);
                        authzcreds = new GlobusGSSCredentialImpl(
                                                 pkiCred,
                                                 GSSCredential.INITIATE_ONLY);
                    } catch (Exception e) {
                        exit("Failed to load credentials at " + args[i] + ": "
                             + e.getMessage(), e);
                    }
                }
            } else if (args[i].equals("-T") ||
                       args[i].equalsIgnoreCase("-trustroots")) {
                this.wantTrustroots = true;
            } else if (args[i].equalsIgnoreCase("-help") ||
                       args[i].equalsIgnoreCase("-usage")) {
                System.err.println(getMessage);
                System.exit(1);
            } else {
                error("Error: get argument #" + i + " (" + args[i] + 
                      ") : unknown");
            }
        }
        
        verifyCommonCmdLine();

        GSSCredential credential = null;
        if (!anonymous) {
            // load default proxy...
            credential = getDefaultCredential();
        }

        GetParams getRequest = new GetParams();
        getRequest.setUserName(getUsername());
        getRequest.setCredentialName(credName);
        getRequest.setLifetime(lifetime);
        getRequest.setWantTrustroots(this.wantTrustroots);

        if (authzcreds == null) {
            String prompt = "Enter MyProxy Pass Phrase: ";
            
            String password = (this.stdin) ?
                Util.getInput(prompt) : Util.getPrivateInput(prompt);
            if (password == null) return;
            getRequest.setPassphrase(password);
        } else {
            getRequest.setAuthzCreds(authzcreds);
        }
            
        try {
            org.globus.myproxy.MyProxy myProxy = getMyProxy();
            if (this.wantTrustroots) {
                bootstrapIfNeeded(myProxy);
            }

            GSSCredential newCred = myProxy.get(credential,
                                                getRequest);
                
            if (debug) {
                String subject =  newCred.getName().toString();
                System.out.println("Proxy subject name: " + subject);
            }

            if (outputFile == null) {
                CoGProperties properties = CoGProperties.getDefault();
                outputFile = properties.getProxyFile();
            }

            // create a file
            File f = null;
            if (outputFile != null) {
                f = new File(outputFile);
            } else {
                f = File.createTempFile("x509up_", ".pem", new File("."));
            }
            String path = f.getPath(); //f.getAbsolutePath();

            OutputStream out = null;
            try {
                out = new FileOutputStream(path);
                
                // set read only permissions
                Util.setOwnerAccessOnly(path);
                
                // write the contents
                byte [] data =
                    ((ExtendedGSSCredential)newCred).export(ExtendedGSSCredential.IMPEXP_OPAQUE);
                
                out.write(data);
            } finally {
                if (out != null) {
                    try { out.close(); } catch(Exception e) {}
                }
            }
            
            System.out.println("A proxy has been received for user " + 
                               getUsername() + " in " + path);

            if (this.wantTrustroots) {
                if (myProxy.writeTrustRoots()) {
                    System.out.println("Wrote trust roots to "
                                       + myProxy.getTrustRootPath() + ".");
                }
                else {
                    System.out.println(
                        "Received no trust roots from MyProxy server.");
                }
            }


        } catch(Exception e) {
            exit("Error: " + e.getMessage(), e);
        }
        exit();
    }

    protected void doPut(String args[], int start) {
        doPutOrStore(args, start, false);
    }

    protected void doStore(String args[], int start) {
        doPutOrStore(args, start, true);
    }

    protected void doPutOrStore(String args[], int start, boolean storeKey) {
        String userCertFile = null;
        String userKeyFile  = null;
        String credName = null;
        String credDesc = null;
        int lifetime = PORTAL_LIFETIME_HOURS * 3600;
        int credLifetime = CRED_LIFETIME_HOURS * 3600;
        int exprType = MATCH_CN_ONLY;
        String retrievers = null;
        String renewers = null;
        boolean anonRetrievers = false;
        boolean anonRenewers = false;
        boolean useEmptyPwd = false;
        X509Certificate [] userCerts = null;
        OpenSSLKey userKey = null;
        
        for (int i=start;i<args.length;i++) {
            if (args[i].equals("-k") ||
                args[i].equalsIgnoreCase("-credname")) {
                ++i;
                if (i == args.length) {
                    error("Error: -k requires credential name");
                } else {
                    credName = args[i];
                }
            } else if (args[i].equals("-K") ||
                       args[i].equalsIgnoreCase("-creddesc")) {
                ++i;
                if (i == args.length) {
                    error("Error: -K requires credential description");
                } else {
                    credDesc = args[i];
                }
            } else if (args[i].equalsIgnoreCase("-cert")) {
                ++i;
                if (i == args.length) {
                    error("Error: -cert requires filename argument");
                } else {
                    userCertFile = args[i];
                }
            } else if (args[i].equalsIgnoreCase("-key")) {
                ++i;
                if (i == args.length) {
                    error("Error: -key requires filename argument");
                } else {
                    userKeyFile = args[i];
                }
            } else if (args[i].equals("-t") ||
                       args[i].equalsIgnoreCase("-portal_lifetime")) {
                ++i;
                if (i == args.length) {
                    error("Error: -t requires time argument in hours");
                } else {
                    lifetime = Integer.parseInt(args[i]) * 3600;
                }
            } else if (!storeKey &&
                       (args[i].equals("-c") ||
                        args[i].equalsIgnoreCase("-cred_lifetime"))) {
                ++i;
                if (i == args.length) {
                    error("Error: -c requires time argument in hours");
                } else {
                    credLifetime = Integer.parseInt(args[i]) * 3600;
                }
            } else if (args[i].equals("-x") || 
                       args[i].equalsIgnoreCase("-regex_dn_match")) {
                /*set expr type to regex*/
                exprType = REGULAR_EXP;
            } else if (args[i].equals("-X") ||  
                       args[i].equalsIgnoreCase("-match_cn_only")) {
                /*set expr type to common name*/
                exprType = MATCH_CN_ONLY;
            } else if (args[i].equals("-A") ||
                       args[i].equalsIgnoreCase("-allow_anonymous_renewers")) {
                anonRenewers = true;
            } else if (args[i].equals("-a") ||
                       args[i].equalsIgnoreCase("-allow_anonymous_retrievers")) {
                anonRetrievers = true;
            } else if (args[i].equals("-r") ||
                       args[i].equalsIgnoreCase("-retrievable_by")) {
                ++i;
                if (i == args.length) {
                    error("Error: -r requires dn argument");
                } else {
                    if (retrievers == null) {
                        retrievers = args[i];
                        useEmptyPwd = true;
                    } else {
                        error("-r already specified.");
                    }
                }
            } else if (args[i].equals("-R") ||
                       args[i].equalsIgnoreCase("-renewable_by")) {
                ++i;
                if (i == args.length) {
                    error("Error: -R requires dn argument");
                } else {
                    if (renewers == null) {
                        renewers = args[i];
                    } else {
                        error("-R already specified.");
                    }
                }
            } else if (!storeKey &&
                       (args[i].equals("-n") ||
                        args[i].equalsIgnoreCase("-no_passphrase"))) {
                       /* use an empty passwd == require certificate based 
                          authorization while getting the creds */
                useEmptyPwd = true;
            } else if (args[i].equalsIgnoreCase("-help") ||
                       args[i].equalsIgnoreCase("-usage")) {
                if (storeKey) {
                    System.err.println(storeMessage);
                } else {
                    System.err.println(putMessage);
                }
                System.exit(1);
            } else {
                error("Error: put argument #" + i + " (" + args[i] + 
                      ") : unknown");
            }
        }

        verifyCommonCmdLine();

        CoGProperties properties = CoGProperties.getDefault();
        
        if (userKeyFile == null) {
            userKeyFile = properties.getUserKeyFile();
        }

        if (userCertFile == null) {
            userCertFile = properties.getUserCertFile();
        }

        if (renewers != null) {
            if (retrievers != null || anonRetrievers) {
                error("Error: -R in incompatible with -a and -r.");
            }
            if (anonRenewers) {
                error("Error: Only one -A or -R option may be specified.");
            }
        }
        
        if (retrievers != null) {
            if (renewers != null || anonRenewers) {
                error("Error: -r is incompatible with -A and -R.");
            }
            if (anonRetrievers) {
                error("Error: Only one -a or -r option may be specified.");
            }
            if (useEmptyPwd) {
                error("Error: -r in incompatible with -n. " +
                      "A passphrase is required for credential retrieval.");
            }
        }
        
        if (anonRetrievers) {
            if (anonRenewers || renewers != null) {
                error("Error: -a is incompatible with -A and -R.");
            }
            if (useEmptyPwd) {
                error("Error: -a is incompatible with -n. " + 
                      "A passphrase is required for credential retrieval.");
            }
        }
        
        if (anonRenewers && 
            (anonRetrievers || retrievers != null)) {
            error("Error: -A is incompatible with -a and -r.");
        }
            
        if (retrievers != null && exprType == MATCH_CN_ONLY) {
            retrievers = "*/CN=" + retrievers;
        } else if (anonRetrievers) {
            retrievers = "*";
        }
        
        if (renewers != null && exprType == MATCH_CN_ONLY) {
            renewers = "*/CN=" + renewers;
        } else if (anonRenewers) {
            renewers = "*";
        }
        
        if (storeKey) {

            StoreParams storeRequest = new StoreParams();
            storeRequest.setUserName(getUsername());
            storeRequest.setLifetime(lifetime);
            storeRequest.setCredentialName(credName);
            storeRequest.setCredentialDescription(credDesc);
            storeRequest.setRenewer(renewers);
            storeRequest.setRetriever(retrievers);

            try {
                userKey = new BouncyCastleOpenSSLKey(userKeyFile);
            } catch(IOException e) {
                exit("Error: Failed to load key: " + userKeyFile,
                     e);
            } catch(GeneralSecurityException e) {
                exit("Error: Unable to load key: " + e.getMessage(),
                     e);
            }
    
            try {
                userCerts = CertUtil.loadCertificates(userCertFile);
            } catch(IOException e) {
                exit("Error: Failed to load certificate: " + userCertFile,
                     e);
            } catch(GeneralSecurityException e) {
                exit("Error: Unable to load certificate: " + e.getMessage(),
                     e);
            }
        
            // load default proxy...
            GSSCredential credential = getDefaultCredential();

            try {
                getMyProxy().store(credential, userCerts, userKey,
                                   storeRequest);
                System.out.println("Credentials saved to MyProxy server on " + 
                                   hostname + ".");
            } catch(Exception e) {
                exit("Error: " + e.getMessage(), e);
            }

        } else {

            // generate new proxy for the new lifetime and display the
            // right time.
            GSSCredential credential = createNewProxy(userCertFile, userKeyFile,
                                                      credLifetime, this.stdin);

            InitParams initRequest = new InitParams();
            initRequest.setUserName(getUsername());
            initRequest.setLifetime(lifetime);
            initRequest.setCredentialName(credName);
            initRequest.setCredentialDescription(credDesc);
            initRequest.setRenewer(renewers);
            initRequest.setRetriever(retrievers);

            if (!useEmptyPwd) {
                String prompt = "Enter MyProxy Pass Phrase: ";

                String password = (this.stdin) ?
                    Util.getInput(prompt) : Util.getPrivateInput(prompt);

                if (password == null) return;
                initRequest.setPassphrase(password);
            }

            try {
                getMyProxy().put(credential, initRequest);
            
                System.out.println("A proxy valid for " + credLifetime/3600 + 
                                   " hours (" + (credLifetime/(3600*24)) + 
                                   " days) for user " + getUsername() + 
                                   " now exists on " + 
                                   hostname + ".");
            } catch(Exception e) {
                exit("Error: " + e.getMessage(), e);
            }

        }

        exit();
    }

    protected void doGetTrustroots(String args[], int start, boolean anonymous) {
        String outputFile = null;

        for (int i=start;i<args.length;i++) {
            if (args[i].equalsIgnoreCase("-help") ||
                args[i].equalsIgnoreCase("-usage")) {
                System.err.println(getTrustrootsMessage);
                System.exit(1);
            } else {
                error("Error: get argument #" + i + " (" + args[i] + 
                      ") : unknown");
            }
        }

        verifyCommonCmdLine();

        GSSCredential credential = null;
        if (!anonymous) {
            // load default proxy...
            credential = getDefaultCredential();
        }

        GetTrustrootsParams getTrustrootsRequest = new GetTrustrootsParams();

        try
        {
            org.globus.myproxy.MyProxy myProxy = getMyProxy();
            bootstrapIfNeeded(myProxy);
            myProxy.getTrustroots(credential, getTrustrootsRequest);

            if (myProxy.writeTrustRoots()) {
                System.out.println("Wrote trust roots to "
                                   + myProxy.getTrustRootPath() + ".");
            }
            else {
                System.out.println(
                    "Received no trust roots from MyProxy server.");
            }
        } catch(Exception e) {
            exit("Error: " + e.getMessage(), e);
        }
        exit();
    }

    private void bootstrapIfNeeded(org.globus.myproxy.MyProxy myProxy) {
        if (!new File(myProxy.getTrustRootPath()).exists()) {
            System.out.println("Bootstrapping MyProxy server root of trust.");

            try {
                myProxy.bootstrapTrust();
            } catch(Exception e) {
                System.err.println("MyProxy bootstrapTrust failed: " + e);
            }
        }
    }

    private void exit() {
        System.exit(0);
    }

    private void exit(String msg, Exception e) {
        System.err.println(msg);
        if (this.debug) {
            e.printStackTrace();
        }
        System.exit(-1);
    }

    private static void error(String msg) {
        System.err.println(msg);
        displaySyntax();
    }
    
    private static void displaySyntax() {
        System.err.println("\nSyntax : java MyProxy [-help] command [-help]");
        System.err.println();
        System.err.println("Use -help to display full usage.");
        System.exit(1);
    }

    public static void main(String [] args) {
        MyProxy myProxy = new MyProxy();
        myProxy.parseCmdLine(args);
    }
    
    private static GSSCredential getDefaultCredential() {
        GSSManager manager = ExtendedGSSManager.getInstance();
        try {
            return manager.createCredential(GSSCredential.INITIATE_ONLY);
        } catch(GSSException e) {
            System.err.println("Failed to load default credentials: " + 
                               e.getMessage());
            System.exit(-1);
        }
        return null;
    }

    private static GSSCredential createNewProxy(String userCertFile,
                                                String userKeyFile,
                                                int lifetime, 
                                                boolean stdin) {

        X509Certificate [] userCerts = null;
        PrivateKey userKey = null;  

        try {
            OpenSSLKey key = new BouncyCastleOpenSSLKey(userKeyFile);

            if (key.isEncrypted()) {
                String prompt = "Enter GRID pass phrase: ";

                String pwd = (stdin) ? 
                    Util.getInput(prompt) : Util.getPrivateInput(prompt);
                
                if (pwd == null) {
                    System.exit(-1);
                }
        
                key.decrypt(pwd);
            }

            userKey = key.getPrivateKey();
        } catch(IOException e) {
            System.err.println("Error: Failed to load key: " + userKeyFile);
            System.exit(-1);
        } catch(GeneralSecurityException e) {
            System.err.println("Error: Wrong pass phrase");
            System.exit(-1);
        }
    
        try {
            userCerts = CertUtil.loadCertificates(userCertFile);
        } catch(IOException e) {
            System.err.println("Error: Failed to load cert: " + userCertFile);
            System.exit(-1);
        } catch(GeneralSecurityException e) {
            System.err.println("Error: Unable to load user certificate: " +
                               e.getMessage());
            System.exit(-1);
        }
        
        BouncyCastleCertProcessingFactory factory =
            BouncyCastleCertProcessingFactory.getDefault();

        int bits = org.globus.myproxy.MyProxy.DEFAULT_KEYBITS;
        boolean limited = false;

        int proxyType = (limited) ? 
            GSIConstants.DELEGATION_LIMITED :
            GSIConstants.DELEGATION_FULL;
        
        try {
            GlobusCredential proxy = 
                factory.createCredential(userCerts,
                                         userKey,
                                         bits,
                                         lifetime,
                                         proxyType);

            return new GlobusGSSCredentialImpl(proxy,
                                               GSSCredential.INITIATE_ONLY);

        } catch (Exception e) {
            System.err.println("Failed to create a proxy: " + e.getMessage());
            System.exit(-1);
        }
        
        return null;
    }
    
}

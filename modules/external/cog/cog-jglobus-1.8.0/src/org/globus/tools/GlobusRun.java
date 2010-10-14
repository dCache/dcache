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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Iterator;
import java.net.MalformedURLException;

import org.globus.common.Version;
import org.globus.gram.Gram;
import org.globus.gram.GramJob;
import org.globus.gram.GramException;
import org.globus.gram.WaitingForCommitException;
import org.globus.gram.GramJobListener;
import org.globus.rsl.RslNode;
import org.globus.rsl.RSLParser;
import org.globus.rsl.NameOpValue;
import org.globus.rsl.Value;
import org.globus.rsl.Binding;
import org.globus.rsl.Bindings;
import org.globus.rsl.VarRef;
import org.globus.util.deactivator.Deactivator;

import org.ietf.jgss.GSSException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** 
 * GlobusRun command-line tool implementation in Java.
 */
public class GlobusRun {

    private static Log logger =
        LogFactory.getLog(GlobusRun.class.getName());
    
    private static final String message =
	"\n" +
	"Syntax: java GlobusRun [options] [RSL String]\n" +
	"        java GlobusRun -version\n" +
	"        java GlobusRun -help\n\n" +
	"\tOptions\n" +
	"\t-help | -usage\n" +
	"\t\tDisplay help.\n" +
	"\t-v | -version\n" +
	"\t\tDisplay version.\n" + 
	"\t-f <rsl filename> | -file <rsl filename>\n" +
	"\t\tRead RSL from the local file <rsl filename>. The RSL\n" + 
	"\t\tmust be a single job request.\n" +
	"\t-q | -quiet\n" +
	"\t\tQuiet mode (do not print diagnostic messages)\n" +
	"\t-o | -output-enable\n" +
	"\t\tUse the GASS Server library to redirect standout output\n" +
	"\t\tand standard error to globusrun. Implies -quiet.\n" +
	"\t-s | -server\n" +
	"\t\t$(GLOBUSRUN_GASS_URL) can be used to access files local\n" +
	"\t\tto the submission machine via GASS. Implies \n" + 
	"\t\t-output-enable and -quiet.\n" +
	"\t-w | -write-allow\n" +
	"\t\tEnable the GASS Server library and allow writing to\n" +
	"\t\tGASS URLs. Implies -server and -quiet.\n" +
	"\t-r <resource manager> | -resource-manager <resource manager>\n" +
	"\t\tSubmit the RSL job request to the specified resource \n" + 
	"\t\tmanager. A resource manager can be specified in the \n" + 
	"\t\tfollowing ways:  \n" + 
	"\t\t - host\n" +
	"\t\t - host:port\n" +
	"\t\t - host:port/service\n" +
	"\t\t - host/service\n" +
	"\t\t - host:/service\n" +
	"\t\t - host::subject\n" +
	"\t\t - host:port:subject\n" +
	"\t\t - host/service:subject\n" +
	"\t\t - host:/service:subject\n" +
	"\t\t - host:port/service:subject\n" +
	"\t\tFor those resource manager contacts which omit the port, \n" +
	"\t\tservice or subject field the following defaults are used:\n" +
	"\t\tport = 2119\n" +
	"\t\tservice = jobmanager \n" +
	"\t\tsubject = subject based on hostname\n" +
	"\t\tThis is a required argument when submitting a single RSL\n" +
	"\t\trequest.\n" +
	"\t-k | -kill <job ID>\n" +
	"\t\tKill a disconnected globusrun job.\n" +
	"\t-status <job ID>\n" +
	"\t\tPrint the current status of the specified job.\n" +
	"\t-b | -batch\n" +
	"\t\tCause globusrun to terminate after the job is successfully\n" +
	"\t\tsubmitted, without waiting for its completion. Useful \n" + 
	"\t\tfor batch jobs. This option cannot be used together with \n" + 
	"\t\teither -server or -interactive, and is also incompatible \n" + 
	"\t\twith multi-request jobs. The \"handle\" or job ID of the \n" + 
	"\t\tsubmitted job will be written on stdout.\n" +
	"\t-stop-manager <job ID>\n" +
	"\t\tCause globusrun to stop the job manager, without killing \n" + 
	"\t\tthe job. If the save_state RSL attribute is present, then a\n" +
	"\t\tjob manager can be restarted by using the restart RSL \n" + 
	"\t\tattribute.\n" +
	"\t-fulldelegation\n" +
	"\t\tPerform full delegation when submitting jobs.\n\n" +
	"\tDiagnostic Options\n" +
	"\t-p | -parse\n" +
	"\t\tParse and validate the RSL only. Does not submit the job \n" + 
	"\t\tto a GRAM gatekeeper. Multi-requests are not supported.\n" +
	"\t-a | -authenticate-only\n" +
	"\t\tSubmit a gatekeeper \"ping\" request only. Do not parse the\n" +
	"\t\tRSL or submit the job request. Requires the \n" + 
	"\t\t-resource-manger argument.\n" +
	"\t-d | -dryrun\n" +
	"\t\tSubmit the RSL to the job manager as a \"dryrun\" test\n" +
	"\t\tThe request will be parsed and authenticated. The job \n" + 
	"\t\tmanager will execute all of the preliminary operations, \n" + 
	"\t\tand stop just before the job request would be executed.\n\n" +
	"\tNot Supported Options\n" +
	"\t-n | -no-interrupt\n";
    
    public static final int GLOBUSRUN_ARG_QUIET             = 2;
    public static final int GLOBUSRUN_ARG_DRYRUN            = 4;
    public static final int GLOBUSRUN_ARG_PARSE_ONLY        = 8;
    public static final int GLOBUSRUN_ARG_AUTHENTICATE_ONLY = 16;
    public static final int GLOBUSRUN_ARG_USE_GASS          = 32;
    public static final int GLOBUSRUN_ARG_ALLOW_READS       = 64;
    public static final int GLOBUSRUN_ARG_ALLOW_WRITES      = 128;
    public static final int GLOBUSRUN_ARG_BATCH             = 512;
    public static final int GLOBUSRUN_ARG_ALLOW_OUTPUT      = 4096;
    public static final int GLOBUSRUN_ARG_FULL_DELEGATION   = 8192;
    
    private static boolean quiet = false;
    
    private static void status(String handle) {
	GramJob jb = new GramJob("");
	
	try {
	    jb.setID(handle);
	    Gram.jobStatus(jb);
	    System.out.println(jb.getStatusAsString());
	} catch(MalformedURLException e) {
	    System.err.println("Error: Invalid job handle: " + e.getMessage());
	    exit(1);
	} catch(GSSException e) {
	    System.err.println("Security error: " + e.getMessage());
	    exit(1);
	} catch(GramException e) {
	    if (e.getErrorCode() == GramException.ERROR_CONTACTING_JOB_MANAGER) {
		System.out.println("DONE");
		exit(0);
	    } else {
		System.err.println("Failed to get job status: " + e.getMessage());
		exit(1);
	    }
	}
	
	exit(0);
    }
    
    private static void stopManager(String handle) {
	GramJob jb = new GramJob("");
	
	try {
	    jb.setID(handle);
	    jb.signal(GramJob.SIGNAL_STOP_MANAGER);
	} catch(MalformedURLException e) {
	    System.err.println("Error: Invalid job handle: " + e.getMessage());
	    exit(1);
	} catch(GSSException e) {
	    System.err.println("Security error: " + e.getMessage());
	    exit(1);
	} catch(GramException e) {
	    System.err.println("Error stopping job manager: " + 
			       e.getMessage());
	    exit(1);
	}
	
	exit(0);
    }
    
    private static void kill(String handle) {
	GramJob jb = new GramJob("");
	try {
	    jb.setID(handle);
	    Gram.cancel(jb);
	} catch(MalformedURLException e) {
	    System.err.println("Invalid job handle");
	    System.exit(1);
	} catch(Exception e) {
	    System.err.println("GRAM Job cancel failed: " + e.getMessage());
	    System.exit(1);
	}
	
	System.out.println("GRAM Job cancel successful");
	System.exit(0);
    }
    
    private static int ping(String rmc) {
	try {
	    Gram.ping(rmc);
	} catch(Exception e) {
	    System.out.println("GRAM Authentication test failed: " + e.getMessage());
	    return 1;
	}
	
	System.out.println("GRAM Authentication test successful");
	return 0;
    }
    
    private static String readRSL(String file) {
	BufferedReader reader = null;
	String line = null;
	StringBuffer rsl = null;
	try {
	    reader = new BufferedReader(new FileReader(file)); 
	    rsl = new StringBuffer();
	    while( (line = reader.readLine()) != null) {
		rsl.append(line.trim());
	    }
	    return rsl.toString();
	} catch(IOException e) {
	    System.err.println("Failed to read rsl file : " + e.getMessage());
	    System.exit(1);
	    return null;
	} finally {
	    if (reader != null) {
		try{ reader.close(); } catch(IOException ee) {}
	    }
	}
    }
    
    private static void exit(int err) {
	logger.debug("Exiting...");
	Deactivator.deactivateAll();
	System.exit(err);
    }
    
    private static void displaySyntax() {
	System.err.println();
	System.err.println("Syntax : java GlobusRun [-help] [-f RSL file] [-s][-b][-d][...] [-r RM] [RSL]");
	System.err.println();
	System.err.println("Use -help to display full usage.");
	System.exit(1);
    }
    
    public static void main(String args[]) {
	
	String rsl      = null;
	String rmc      = null;
	
	boolean ping    = false;
	boolean error   = false;
	
	int options     = 0;
	
	if (args.length == 0) {
	    System.err.println(message);
	    System.exit(0);
	}
	
	for (int i = 0; i < args.length; i++) {
	    
	    if (args[i].charAt(0) != '-' && i+1 == args.length) {
		// rsl spec
		if (rsl != null) {
		    error = true;
		    System.err.println("Error: RSL already specifed");
		    break;
		}
		
		rsl = args[i];
		
	    } else if (args[i].equalsIgnoreCase("-status")) {
		
		// job status
		++i;
		if (i == args.length) {
		    error = true;
		    System.err.println("Error: -status requires a job handle");
		    break;
		} else {
		    status( args[i] );
		}
		
	    } else if (args[i].equals("-k") || 
		       args[i].equalsIgnoreCase("-kill")) {
		
		// kill job
		++i;
		if (i == args.length) {
		    error = true;
		    System.err.println("Error: -kill requires a job handle");
		    break;
		} else {
		    kill( args[i] );
		}
		
	    } else if (args[i].equals("-r") || 
		       args[i].equalsIgnoreCase("-resource-manager")) {
		
		// resource manager contact
		++i;
		if (i == args.length) {
		    error = true;
		    System.err.println("Error: -r requires resource manager contact");
		    break;
		} else {
		    rmc = args[i]; 
		}
		
	    } else if (args[i].equals("-b") || 
		       args[i].equalsIgnoreCase("-batch")) {
		
		// batch job
		options |= GLOBUSRUN_ARG_BATCH;
		
	    } else if (args[i].equals("-d") ||
		       args[i].equalsIgnoreCase("-dryrun")) {
		
		// dryrun
		options |= GLOBUSRUN_ARG_DRYRUN;
		
	    } else if (args[i].equalsIgnoreCase("-fulldelegation")) {
		
		// perform full delegation
		options |= GLOBUSRUN_ARG_FULL_DELEGATION;
		
	    } else if (args[i].equalsIgnoreCase("-stop-manager")) {
		
		++i;
		if (i == args.length) {
		    error = true;
		    System.err.println("Error: -stop-manager requires job ID");
		    break;
		} else {
		    stopManager(args[i]);
		}
		
	    } else if (args[i].equals("-a") || 
		       args[i].equalsIgnoreCase("-authenticate-only")) {
		
		// ping request
		options |= GLOBUSRUN_ARG_AUTHENTICATE_ONLY;
		
	    } else if (args[i].equals("-o") || 
		       args[i].equalsIgnoreCase("-output-enable")) {
		
		// redirect output
		options |=
		    GLOBUSRUN_ARG_USE_GASS |
		    GLOBUSRUN_ARG_QUIET |
		    GLOBUSRUN_ARG_ALLOW_OUTPUT;
		
	    } else if (args[i].equals("-w") ||
		       args[i].equalsIgnoreCase("-write-allow")) {
		
		options |= 
		    GLOBUSRUN_ARG_ALLOW_WRITES |
		    GLOBUSRUN_ARG_ALLOW_READS |
		    GLOBUSRUN_ARG_USE_GASS |
		    GLOBUSRUN_ARG_QUIET;
		
	    } else if (args[i].equals("-f") || 
		       args[i].equalsIgnoreCase("-file")) {
		
		// read from file
		i++;
		if (i == args.length) {
		    error = true;
		    System.err.println("Error: -file requires a filename");
		    break;
		} else {
		    rsl = readRSL(args[i]);
		}
		
	    } else if (args[i].equals("-s") || 
		       args[i].equalsIgnoreCase("-server")) {
		
		// enable gass url
		options |=
		    GLOBUSRUN_ARG_ALLOW_READS |
		    GLOBUSRUN_ARG_USE_GASS |
		    GLOBUSRUN_ARG_QUIET;
		
	    } else if (args[i].equals("-q") || 
		       args[i].equalsIgnoreCase("-quiet")) {
		
		// quiet mode
		options |= GLOBUSRUN_ARG_QUIET;
		
	    } else if (args[i].equals("-p") ||
		       args[i].equalsIgnoreCase("-parse")) {
		
		// parse only
		options |= GLOBUSRUN_ARG_PARSE_ONLY;
		
	    } else if (args[i].equals("-v") || 
		       args[i].equalsIgnoreCase("-version")) {
		
		// display version info
		System.err.println(Version.getVersion());
		System.exit(1);
		
	    } else if (args[i].equalsIgnoreCase("-help") ||
		       args[i].equalsIgnoreCase("-usage")) {
		
		System.err.println(message);
		System.exit(1);
		
	    } else {
		System.out.println("Error: argument #" + i + " (" +args[i] +") : unknown");
		error = true;
	    }
	}
	
	if ((options & GLOBUSRUN_ARG_BATCH) != 0 &&
	    (options & GLOBUSRUN_ARG_USE_GASS) != 0) {
	    error = true;
	    System.err.println("Error: option -s and -b are exclusive");
	}
	
	if (error) {
	    displaySyntax();
	}
	
	// from C globusrun
	
	if ((options & GLOBUSRUN_ARG_AUTHENTICATE_ONLY) != 0) {
	    
	    if (rmc == null) {
		System.err.println("Error: No resource manager contact specified " +
				   "for authentication test.");
		displaySyntax();
	    }
	    
	    System.exit( ping(rmc) );
	}
	
	if (rsl == null) {
	    System.err.println("Error: Must specify a RSL string.");
	    displaySyntax();
	}
	
	RslNode rslTree = null;
	
	try {
	    rslTree = RSLParser.parse(rsl);
	} catch(Throwable e) {
	    System.err.println("Error: Cannot parse RSL: " + e.getMessage());
	    System.exit(-1);
	}
	
	// check if the rsl is boolean? if not, exit with Bad RSL!
	
	if ((options & GLOBUSRUN_ARG_PARSE_ONLY) != 0) {
	    System.out.println("RSL Parsed Successfully...\n");
	    System.exit(0);
	}
	
	if (rslTree.getOperator() != RslNode.MULTI &&
	    rmc == null) {
	    System.err.println("Error: No resource manager contact");
	    displaySyntax();
	}
	
	if ((options & GLOBUSRUN_ARG_USE_GASS) != 0) {
	    
	    String gassUrl = null;
	    
	    int server_options = 
		org.globus.io.gass.server.GassServer.STDOUT_ENABLE |
		org.globus.io.gass.server.GassServer.STDERR_ENABLE;
	    
	    if ((options & GLOBUSRUN_ARG_ALLOW_READS) != 0) {
		server_options |= 
		    org.globus.io.gass.server.GassServer.READ_ENABLE;
	    }
	    
	    if ((options & GLOBUSRUN_ARG_ALLOW_WRITES) != 0) {
		server_options |=
		    org.globus.io.gass.server.GassServer.WRITE_ENABLE;
	    }
	    
	    try {
		org.globus.io.gass.server.GassServer gassServer 
		    = new org.globus.io.gass.server.GassServer();	
		gassServer.setOptions(server_options);
		gassServer.setTimeout(0); // keep sockets open forever
		
		gassUrl = gassServer.getURL();
		
		gassServer.registerDefaultDeactivator();
		
		logger.debug(gassServer);
		
	    } catch (IOException e) {
		System.err.println("Gass server initialization failed: " + e.getMessage());
		exit(1);
	    }
	    
	    // this will update the tree appriopriately
	    rslOutputSubst(rslTree, gassUrl, (options & GLOBUSRUN_ARG_ALLOW_OUTPUT) != 0);
	    
	}
	
	if ((options & GLOBUSRUN_ARG_DRYRUN) != 0) {
	    rslDryrunSubst(rslTree);
	}
	
	quiet = ((options & GLOBUSRUN_ARG_QUIET) != 0);
	
	String finalRsl = rslTree.toRSL(true);
	logger.debug("RSL: " + finalRsl);
	
	if (rslTree.getOperator() == RslNode.MULTI) {
	    multiRun(rslTree, options);
	} else {
	    gramRun(finalRsl, rmc, options);
	}
	
    }
    
    private static void println(String str) {
	if (!quiet) {
	    System.out.println(str);
	}
    }
    
    private static void gramRun(String rsl, String rmc, int options) {
	JobListener jobListener = null;
	GramJob job = new GramJob(rsl);
	
	println("Job status callback handler enabled.");
	if ((options & GLOBUSRUN_ARG_BATCH) != 0 ) {
	    jobListener = new BatchJobListener();
	} else {
	    jobListener = new InteractiveJobListener(quiet);
	}
	job.addListener(jobListener);
	
	Exception exception = null;
	boolean sendCommit = false;
	
	try {
	    job.request(rmc, false, !((options & GLOBUSRUN_ARG_FULL_DELEGATION) != 0));
	} catch(WaitingForCommitException e) {
            try {
                job.signal(GramJob.SIGNAL_COMMIT_REQUEST);
		sendCommit = true;
            } catch(Exception ee) {
		exception = ee;
	    }
	} catch(Exception e) {
	    exception = e;
	}
	
	if (exception instanceof GramException) {
	    int err = ((GramException)exception).getErrorCode();
	    displayError((GramException)exception, options);
	    exit(err);
	} else if (exception instanceof GSSException) {
	    System.err.println("Security error: " + exception.getMessage());
	    exit(1);
	} else if (exception != null) {
	    logger.error("GRAM Job submission failed", exception);
	    System.err.println("GRAM Job submisson failed: " +
			       exception.getMessage());
            exit(1);
	}
	
	// submission must be successful at this point.
	
	println("GRAM Job submission successful");
	
	if ((options & GLOBUSRUN_ARG_BATCH) != 0) {
	    System.out.println(job.getIDAsString());
	}
	
	if (jobListener instanceof BatchJobListener) {
	    logger.debug("Waiting for the job to start up...");
	} else {
	    logger.debug("Waiting for the job to complete...");
	}
	
	try {
	    jobListener.waitFor();
	} catch (InterruptedException e) {
	}
	
	job.removeListener(jobListener);
	
	int err = jobListener.getError();
	
	if (jobListener.isFinished()) {
	    if (sendCommit) {
		logger.debug("Sending COMMIT_END signal");
		try {
		    job.signal(GramJob.SIGNAL_COMMIT_END);
		} catch (GramException e) {
		    logger.debug("Signal failed", e);
		    System.err.println("Signal failed: " + 
				       e.getMessage());
		    exit(e.getErrorCode());
		} catch (GSSException e) {
		    logger.debug("Signal failed", e);
		    System.err.println("Security error: " + e.getMessage());
		    exit(1);
		}
	    }
	} else if ((options & GLOBUSRUN_ARG_BATCH) != 0) {
	    try {
		job.unbind();
	    } catch (GramException e) {
		logger.debug("Unbind failed", e);
		System.err.println("Unbind failed: " + 
				   e.getMessage());
		exit(e.getErrorCode());
	    } catch (GSSException e) {
		logger.debug("Unbind failed", e);
		System.err.println("Security error: " + e.getMessage());
		exit(1);
	    }
	}
	
	displayError(err, options);
	exit(err);
    }
    
    private static void displayError(int errorCode, int options) {
	if (errorCode == GramException.DRYRUN &&
	    (options & GLOBUSRUN_ARG_DRYRUN) != 0) {
	    println("Dryrun successful");
	} else if (errorCode != 0) {
	    System.err.println("GRAM Job submission failed: " + 
			       GramException.getMessage(errorCode) + " (error code " + 
			       errorCode + ")");
	}
    }
    
    private static void displayError(GramException exception, int options) {
	int errorCode = exception.getErrorCode();
	if (errorCode == GramException.DRYRUN &&
	    (options & GLOBUSRUN_ARG_DRYRUN) != 0) {
	    println("Dryrun successful");
	} else if (errorCode != 0) {
            logger.error("GRAM Job submission failed", exception);
	    System.err.println("GRAM Job submission failed: " + 
			       exception.getMessage());
	}
    }

    private static void multiRun(RslNode rslTree, int options) {
	if ( (options & GLOBUSRUN_ARG_BATCH) != 0 ) {
	    System.err.println("Error: Batch mode (-b) not supported for multi-request");
	    exit(1);
	}
	
	MultiJobListener listener = new MultiJobListener(quiet);
	
        List jobs = rslTree.getSpecifications();
        Iterator iter = jobs.iterator();
        RslNode node;
        NameOpValue nv;
	String rmc;
	String rsl;
        while( iter.hasNext() ) {
            node = (RslNode)iter.next();
	    rsl = node.toRSL(true);
	    nv = node.getParam("resourceManagerContact");
            if (nv == null) {
                System.err.println("Error: No resource manager contact for job.");
		continue;
            } else {
                Object obj = nv.getFirstValue();
                if (obj instanceof Value) {
		    rmc = ((Value)obj).getValue();
		    multiRunSub(rsl, rmc, options, listener);
                }
            }
	}
	
	logger.debug("Waiting for jobs to complete...");
	synchronized(listener) {
	    try {
		listener.wait();
	    } catch(InterruptedException e) {
	    }
	}
	
	logger.debug("Exiting...");
	Deactivator.deactivateAll();
    }
    
    private static void multiRunSub(String rsl, String rmc, int options,
				    MultiJobListener listener) {
	GramJob job = new GramJob(rsl);
	
	job.addListener(listener);
	
	Exception exception = null;
	
        try {
            job.request(rmc, false, !((options & GLOBUSRUN_ARG_FULL_DELEGATION) != 0));
        } catch(WaitingForCommitException e) {
            try {
                job.signal(GramJob.SIGNAL_COMMIT_REQUEST);
            } catch(Exception ee) {
                exception = ee;
            }
        } catch(Exception e) {
            exception = e;
        }
	
        if (exception instanceof GramException) {
            if ( ((GramException)exception).getErrorCode() == GramException.DRYRUN &&
                 (options & GLOBUSRUN_ARG_DRYRUN) != 0) {
                println("Dryrun successful");
            } else {
                System.err.println("GRAM Job submission failed: " +
                                   exception.getMessage() + " (error code " +
                                   ((GramException)exception).getErrorCode() + 
				   ", rsl: " + rsl + ")");
            }
	    return;
        } else if (exception instanceof GSSException) {
            System.err.println("Security error: " + exception.getMessage());
            exit(1);
        } else if (exception != null) {
            System.err.println("GRAM Job submisson failed: " +
                               exception.getMessage());
	    return;
        }
	
	listener.runningJob();
        println("GRAM Job submission successful (rmc: " + rmc + ")");
    }
    
    private static void rslDryrunSubst(RslNode rslTree) {
	if (rslTree.getOperator() == RslNode.MULTI) {
	    List specs = rslTree.getSpecifications();
	    Iterator iter = specs.iterator();
	    RslNode node;
	    while( iter.hasNext() ) {
		node = (RslNode)iter.next();
		rslDryrunSubst(node);
	    }
	} else {
	    rslTree.put(new NameOpValue("dryrun", NameOpValue.EQ, "yes") );
	}
    }
    
    private static void rslOutputSubst(RslNode rslTree, String gassUrl, boolean allowOutput) {
	if (rslTree.getOperator() == RslNode.MULTI) {
            List specs = rslTree.getSpecifications();
	    Iterator iter = specs.iterator();
            RslNode node;
            while( iter.hasNext() ) {
                node = (RslNode)iter.next();
                rslOutputSubst(node, gassUrl, allowOutput);
            }
        } else {
	    Binding bd = new Binding("GLOBUSRUN_GASS_URL", gassUrl);
	    Bindings bind = rslTree.getBindings("rsl_substitution");
	    if (bind == null) {
		bind = new Bindings("rsl_substitution");
		rslTree.put(bind);
	    }
	    bind.add(bd);
	    
	    if (allowOutput) {
		Value value = null;
		value = new VarRef("GLOBUSRUN_GASS_URL",
				   null,
				   new Value("/dev/stdout"));
		
		rslTree.put(new NameOpValue("stdout", 
					    NameOpValue.EQ, 
					    value));
		
		value = new VarRef("GLOBUSRUN_GASS_URL",
				   null,
				   new Value("/dev/stderr"));
		
		rslTree.put(new NameOpValue("stderr",
					    NameOpValue.EQ,
					    value));
	    }
	}
    }
    
    
}


abstract class JobListener implements GramJobListener {
    
    protected int status = 0;
    protected int error = 0;
    
    public abstract void waitFor()
	throws InterruptedException;
    
    public int getError() {
	return error;
    }
    
    public int getStatus() {
	return status;
    }
    
    public boolean isFinished() {
	return (status == GramJob.STATUS_DONE ||
		status == GramJob.STATUS_FAILED);
    }
}

class BatchJobListener extends JobListener {
    
    private boolean called = false;
    
    // waits for first notification
    public synchronized void waitFor() 
	throws InterruptedException {
	while (!called) {
	    wait();
	}
    }
    
    public synchronized void statusChanged(GramJob job) {
	if (!called) {
	    called = true;
	    status = job.getStatus();
	    error = job.getError();
	    notify();
	}
    }
}

class InteractiveJobListener extends JobListener {
    
    private boolean quiet;
    private boolean finished = false;
    
    public InteractiveJobListener(boolean quiet) {
	this.quiet = quiet;
    }
    
    // waits for DONE or FAILED status
    public synchronized void waitFor() 
	throws InterruptedException {
	while (!finished) {
	    wait();
	}
    }
    
    public synchronized void statusChanged(GramJob job) {
	if (!quiet) {
	    System.out.println("Job: "+ job.getStatusAsString());
	}
	status = job.getStatus();
	if (status == GramJob.STATUS_DONE) {
	    finished = true;
	    error = 0;
	    notify();
	} else if (job.getStatus() == GramJob.STATUS_FAILED) {
	    finished = true;
	    error = job.getError();
	    notify();
	}
    }
}

class MultiJobListener implements GramJobListener {
    
    private boolean quiet;
    private int runningJobs = 0;
    
    public MultiJobListener(boolean quiet) {
	this.quiet = quiet;
    }
    
    public void runningJob() {
	runningJobs++;
    }
    
    public synchronized void done() {
	runningJobs--;
	if (runningJobs <= 0) {
	    synchronized(this) {
		notifyAll();
	    }
	}
    }
    
    public void statusChanged(GramJob job) {
	
	if (!quiet) {
	    System.out.println("Job status:");
	    System.out.println("Job ID : " + job.getIDAsString());
	    System.out.println("Status : " + job.getStatusAsString());
	}
	
	if (job.getStatus() == GramJob.STATUS_DONE) {
	    done();
	} else if (job.getStatus() == GramJob.STATUS_FAILED) {
	    System.out.println( job.getError() );
	    done();
	}
    }
    
}

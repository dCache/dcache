/** 
 *  $Id: GramAttributes.java,v 1.9 2006/04/09 05:56:59 gawor Exp $
 */
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
package org.globus.gram;

import java.util.List;
import java.util.Map;

import org.globus.rsl.RslAttributes;
import org.globus.rsl.RslNode;
import org.globus.rsl.ParseException;

/**
 * A convienience class for operating on GRAM-specific RSL attributes.
 * Please note the attribute values for attributes such as setStdout, 
 * setStderr, setStdin, setDirectory, setExecutable, etc. are treated
 * as single arguments. In case the value contains a RSL variable, 
 * the variable will not be properly resolved. For example, if you 
 * set the stdout to:
 * <pre>
 * atts.setStdout("$(MY_URL)/bar");
 * atts.addEnvVariable("MY_URL", "http://foo");
 * </pre>
 * the resulting rsl will look like:
 * <pre>
 * &("stdout"="$(MY_URL)/hello")("environment"=("MY_URL" "http://foo"))
 * </pre>
 * Since the "$(MY_URL)/hello" is in double quotes it will be treated as 
 * a single string and the variable will never be resolved. The parser
 * will set "stdout" to "$(MY_URL)/hello" instead of "http://foo/hello".
 */
public class GramAttributes extends RslAttributes {

    public static final int JOBTYPE_SINGLE   = 1;
    public static final int JOBTYPE_MULTIPLE = 2;
    public static final int JOBTYPE_MPI      = 3;
    public static final int JOBTYPE_CONDOR   = 4;
    
    /**
     * Constructs a new, empty GramAttributes object. 
     */
    public GramAttributes() {
	super();
    }

    /**
     * Constructs a new GramAttributes object initialized with
     * the specified RSL string.
     *
     * @param rsl the rsl string to initialize the class with.
     * @throws ParseException if the rsl cannot be parsed.
     */
    public GramAttributes(String rsl) throws ParseException {
	super(rsl);
    }
    
    /**
     * Constructs a new GramAttributes object initialized with
     * the specified RSL parse tree.
     * 
     * @param rslTree the rsl parse tree to initialize the class with.
     */
    public GramAttributes(RslNode rslTree) {
	super(rslTree);
    }
    
    /**
     * Specify the name of the executable to run
     *
     * @param executable the name of the executable    
     */
    public void setExecutable(String executable) {
	set("executable", executable);
    }

    /**
     * Return executable name
     *
     * @return executable
     */ 
    public String getExecutable() {
	return getSingle("executable");
    }

    /**
     * Specify the directory path the executable will be run in
     *
     * @param directory the directory path on the submission machine    
     */
    public void setDirectory(String directory) {
	set("directory", directory);
    }

    /**
     * Return directory path
     *
     * @return directory
     */
    public String getDirectory() {
	return getSingle("directory");
    }

    /**
     * Specify the location to redirect stdout on the submission machine
     *
     * @param stdout the location to redirect stdout on the submission machine
     */
    public void setStdout(String stdout) {
	set("stdout", stdout);
    }

    /**
     * Return the location used to redirect stdout on the submission machine
     *
     * @return stdout
     */
    public String getStdout() {
	return getSingle("stdout");
    }

     /**
     * Specify the location to redirect stderr on the submission machine
     *
     * @param stderr the location to redirect stderr on the submission machine
     */
    public void setStderr(String stderr) {
	set("stderr", stderr);
    }

    /**
     * Return the location used to redirect stderr on the submission machine
     *
     * @return stderr
     */
    public String getStderr() {
	return getSingle("stderr");
    }

    /**
     * Specify the location to redirect stdin on the submission machine
     *
     * @param stdin the location to redirect stdin on the submission machine
     */
    public void setStdin(String stdin) {
        set("stdin", stdin);
    }

    /**
     * Return the location used to redirect stdin on the submission machine
     *
     * @return stdin
     */
    public String getStdin() {
        return getSingle("stdin");
    }

    /**
     * Sets the dryrun parameter.
     *
     * @param enable true to enable dryrun, false otherwise.
     */
    public void setDryRun(boolean enable) {
	set("dryrun", (enable) ? "yes" : "no");
    }
    
    /**
     * Checks if dryryn is enabled.
     *
     * @return true only if dryrun is enabled. False,
     *         otherwise.
     */
    public boolean isDryRun() {
	String run = getSingle("dryrun");
	if (run == null) return false;
	if (run.equalsIgnoreCase("yes")) return true;
	return false;
    }

    /**
     * Specify the queue name to be used for this job
     *
     * @param queue the queue name to be used for this job
     */
    public void setQueue(String queue) {
	set("queue", queue);
    }

    /**
     * Return the queue name used for this job
     *
     * @return queue
     */
    public String getQueue() {
	return getSingle("queue");
    }

    /**
     * Specify the project to be charged for this job
     *
     * @param project the project to be charged for this job
     */
    public void setProject(String project) {
	set("project", project);
    }
    
    /**
     * Return the project name charged for this job
     *
     * @return project
     */
    public String getProject() {
	return getSingle("project");
    }

    /**
     * Sets a job type.
     *
     * @param jobType type of the job: One of the following:
     *        SINGLE, MULTIPLE, MPI, or CONDOR.
     */
    public void setJobType(int jobType) {
	String type = null;
	switch(jobType) {
	case JOBTYPE_SINGLE:
	    type = "single"; break;
	case JOBTYPE_MULTIPLE:
	    type = "multiple"; break;
	case JOBTYPE_MPI:
	    type = "mpi"; break;
	case JOBTYPE_CONDOR:
	    type = "condor"; break;
	}	
	if (type != null) {
	    set("jobtype", type);
	}
    }

    /**
     * Returns type of the job.
     *
     * @return job type. -1 if not set or job type 
     *         is unknown.
     */
    public int getJobType() {
	String jobType = getSingle("jobtype");
	if (jobType == null) return -1;
	if (jobType.equalsIgnoreCase("single")) {
	    return JOBTYPE_SINGLE;
	} else if (jobType.equalsIgnoreCase("multiple")) {
	    return JOBTYPE_MULTIPLE;
	} else if (jobType.equalsIgnoreCase("mpi")) {
	    return JOBTYPE_MPI;
	} else if (jobType.equalsIgnoreCase("condor")) {
	    return JOBTYPE_CONDOR;
	} else {
	    return -1;
	}
    }

    /**
     * Specify the minimum memory limit for this job
     *
     * @param minmemory the minimum memory limit for this job
     */
    public void setMinMemory(int minmemory) {
	set("minmemory", String.valueOf(minmemory));
    }

    /**
     * Return the minimum memory limit set for the job
     *
     * @return minmemory
     */
    public int getMinMemory() {
	String value = getSingle("minmemory");
        try {
            return Integer.parseInt(value);
        } catch(Exception e) {
            return -1;
        }
    }

    /**
     * Specify the nuber of processors to be used by the current executable
     *
     * @param numprocs the number of processors to use
     */
    public void setNumProcs(int numprocs) {
	set("count", String.valueOf(numprocs));
    }
    
    /**
     * Return the number of processors
     *
     * @return numprocs
     */
    public int getNumProcs() {
	String value = getSingle("count");
	try {
	    return Integer.parseInt(value);
	} catch(Exception e) {
	    return -1;
	}
    }

    /**
     * Specify the maximum wall time limit for this job
     *
     * @param maxwalltime the maximum wall time limit for this job
     */
    public void setMaxWallTime(int maxwalltime) {
	set("maxwalltime", String.valueOf(maxwalltime));
    }

    /**
     * Return the maximum wall time limit set for the job
     *
     * @return maxwalltime
     */
    public int getMaxWallTime() {
        String value = getSingle("maxwalltime");
        try {
            return Integer.parseInt(value);
        } catch(Exception e) {
            return -1;
        }
    }

    /**
     * Specify the maximum cpu time limit for this job
     *
     * @param maxcputime the maximum cpu time limit for this job
     */
    public void setMaxCPUTime(int maxcputime) {
	set("maxcputime", String.valueOf(maxcputime));
    }

    /**
     * Return the maximum cpu time limit set for the job
     *
     * @return maxcputime
     */
    public int getMaxCPUTime() {
	String value = getSingle("maxcputime");
        try {
            return Integer.parseInt(value);
        } catch(Exception e) {
            return -1;
        }
    }

    /**
     * Specify the maximum memory limit for this job
     *
     * @param maxmemory the maximum memory limit for this job
     */
    public void setMaxMemory(int maxmemory) {
	set("maxmemory", String.valueOf(maxmemory));
    }

    /**
     * Return the maximum memory limit set for the job
     *
     * @return maxmemory
     */
    public int getMaxMemory() {
        String value = getSingle("maxmemory");
        try {
            return Integer.parseInt(value);
        } catch(Exception e) {
            return -1;
        }
    }
    
    /**
     * Adds a single argument.
     *
     * @param argument an argument to add. 
     *        It will be treated as a single argument.
     */
    public void addArgument(String argument) {
	add("arguments", argument);
    }

    /**
     * Removes a specific argument from the argument list.
     *
     * @param argument argument to remove.
     * @return true if the argument was removed, 
     *         false otherwise.
     */
    public boolean deleteArgument(String argument) {
	return remove("arguments", argument);
    }
    
    /**
     * Returns a list of arguments.
     *
     * @return list of arguments.
     */
    public List getArguments() {
	return getMulti("arguments");
    }
    
    /**
     * Adds an environment variable.
     *
     * @param varName the variable name.
     * @param value the value of the variable.
     */
    public void addEnvVariable(String varName, String value) {
	addMulti("environment", new String [] {varName, value});
    }
    
    /**
     * Removes a specific environment variable from the
     * environment list.
     *
     * @param varName name of the variable to remove.
     * @return true if the environment variables was removed,
     *         false otherwise.
     */
    public boolean deleteEnvVariable(String varName) {
	return removeMap("environment", varName);
    }
    
    /**
     * Returns a variable/value pair list of environment 
     * variables.
     *
     * @return the association list of environment 
     *         variables.
     */
    public Map getEnvironment() {
	return getMap("environment");
    }
    
}

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

import org.globus.util.deactivator.Deactivator;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;

import org.ietf.jgss.GSSCredential;

public class MultiUserGramTest implements GramJobListener {
				
    public void statusChanged(GramJob job) {
	String subject = "unknown";
	try {
	    subject = job.getCredentials().getName().toString();
	} catch (Exception e) {
	}
	System.out.println(
			   "Job status change \n" +  
			   "    ID     : " + job.getIDAsString() + "\n" + 
			   "    Status : " + job.getStatusAsString() + "\n" + 
			   "   Subject : " + subject);
    }
    
    private static GramJob createJob(GSSCredential proxy, 
				     GramJobListener listener, String dir, int id) {
	String subject = "unknown";
	try {
	    subject = proxy.getName().toString();
	} catch (Exception e) {
	}
	GramJob job = null;
	String env = "(environment=(CERT_SUBJECT \"" + 
	    subject + "\"))";
	job = new GramJob("&(directory=\"" + dir + "\")" +
			  "(stdout=date.out." + id + ")" + 
			  env + 
			  "(executable=\"/bin/env\")");
	job.setCredentials(proxy);
	job.addListener(listener);
	return job;
    }
  
    private static GSSCredential load(String file) {
	try {
	    GlobusCredential cred = new GlobusCredential(file);
	    return new GlobusGSSCredentialImpl(cred, GSSCredential.INITIATE_AND_ACCEPT);
	} catch(Exception e) {
	    System.err.println("Failed to load proxy: " + e.getMessage());
	    System.exit(-1);
	}
	return null;
    }
    
    private static Thread submit(String contact, GramJob job) {
	Thread t = new Thread(new JobRun(contact, job));
	t.start();
	return t;
    }

    public static void main(String argv[]) {

	if (argv.length < 4) {
	    System.out.println("Usage: java MultiUserGramTest" +
			       " contact dir proxy1 proxy2");
	    System.exit(-1);
	}

	String contact = argv[0];
	String dir     = argv[1];

	GSSCredential proxy1, proxy2;
	GramJob job1, job2;
	Thread t1, t2;

	proxy1 = load(argv[2]);
	proxy2 = load(argv[3]);

	MultiUserGramTest test = new MultiUserGramTest();

	job1 = createJob(proxy1, test, dir, 0);
	job2 = createJob(proxy2, test, dir, 1);
    
	t1 = submit(contact, job1);
	t2 = submit(contact, job2);
    
	System.out.println("wait for job completion.");
	while(true) {
	    try {
		Thread.sleep(1000);
	    } catch (InterruptedException e) {}
	    if ( (job1.getStatus() == GramJob.STATUS_DONE || 
		  job1.getStatus() == GramJob.STATUS_FAILED) &&
		 (job2.getStatus() == GramJob.STATUS_DONE || 
		  job2.getStatus() == GramJob.STATUS_FAILED) ) break;
	}
	Deactivator.deactivateAll();
    }
}

class JobRun implements Runnable {

    private String resourceManagerContact = null;
    private GramJob gramJob = null;

    public JobRun( String resourceManagerContact, GramJob gramJob ) {
	this.resourceManagerContact = resourceManagerContact;
	this.gramJob = gramJob;
    }
  
    public void run() {
	try {
	    String jobname = 
		gramJob.getCredentials().getName().toString();
	    System.out.println("Submitting job with subject : " + jobname);
	    Gram.request( resourceManagerContact, gramJob );

	    System.out.println("Job submitted:\n" +
			       "    ID     : "+ gramJob.getIDAsString() + "\n" +
			       "   Subject : "+ jobname);
	} catch( Exception gpe ) {
	    System.err.println( "Error: " + gpe.getMessage() );
	}
    }
}


















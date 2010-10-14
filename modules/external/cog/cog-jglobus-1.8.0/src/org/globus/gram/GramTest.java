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

import org.globus.util.deactivator.*;

public class GramTest {

  public static void main(String [] args) {
    
    GramJob job1 = new GramJob("&(executable=/bin/sleep)(directory=/tmp)(arguments=15)");

    GramJob job2 = new GramJob("&(executable=/bin/sleep)(directory=/tmp)(arguments=25)");

    GramJob job3 = new GramJob("&(executable=/bin/sleep)(directory=/tmp)(arguments=35)");
    
    String contact = null;

    if (args.length == 0) {
      System.err.println("Usage: java GramTest [resource manager]");
      System.exit(1);
    }

    contact = args[0];

    try {
      job1.addListener( new GramJobListener() {
	public void statusChanged(GramJob job) {
	  System.out.println("Job1 status change \n" +  
			     "    ID     : "+ job.getIDAsString() + "\n" + 
			     "    Status : "+ job.getStatusAsString());
	}	
      });

      job3.addListener( new GramJobListener() {
	public void statusChanged(GramJob job) {
	  System.out.println("Job3 status change \n" +  
			     "    ID     : "+ job.getIDAsString() + "\n" + 
			     "    Status : "+ job.getStatusAsString());

	}	
      });

      job2.addListener( new GramJobListener() {
	public void statusChanged(GramJob job) {
	   System.out.println("Job2 status change \n" +  
			     "    ID     : "+ job.getIDAsString() + "\n" + 
			     "    Status : "+ job.getStatusAsString());

	  if (job.getStatus() == 2) {
	    try {
	      System.out.println("disconnecting from job2");
	      job.unbind();
	      System.out.println("canceling job2");
	      job.cancel();
	    } catch(Exception e) {
	      System.out.println(e);
	    }

	  }

	}	
      });


      System.out.println("submitting job1...");
      job1.request(contact);
      System.out.println("job submited: " + job1.getIDAsString());

      System.out.println("submitting job2...");
      job2.request(contact);
      System.out.println("job submited: " + job2.getIDAsString());

      System.out.println("submitting job3 in batch mode...");
      job3.request(contact, true);
      System.out.println("job submited: " + job3.getIDAsString());

      try {  Thread.sleep(2000); }  catch(Exception e) {}

      System.out.println("rebinding to job3..");
      job3.bind();

      try {
	while ( Gram.getActiveJobs() != 0 ) {
	  Thread.sleep(2000); 
	}
      } catch(Exception e) {}

      System.out.println("Test completed.");
    } catch(Exception e) {
      System.out.println(e.getMessage());
    } finally {
	Deactivator.deactivateAll();
    }
    
  }

}





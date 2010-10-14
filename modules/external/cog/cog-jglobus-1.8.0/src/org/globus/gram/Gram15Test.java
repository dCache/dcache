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

import org.globus.io.gass.server.GassServer;
import org.globus.util.deactivator.Deactivator;

import org.ietf.jgss.GSSException;

/**
 * Demonstration of Gram 1.5 protocol features.
 * <BR>
 */
public class Gram15Test {

    private static GramJobListener getListener(final String label) {
	GramJobListener l = ( new GramJobListener() {
                public void statusChanged(GramJob job) {
                    System.out.println(label + " status change \n" +
                                       "    ID     : "+ job.getIDAsString() + "\n" +
                                       "    Status : "+ job.getStatusAsString());
                }
            });
	return l;
    }

    public static boolean test1(String contact, boolean cancelCall) {
	
	GramJob job = new GramJob("&(executable=/bin/sleep)(arguments=100)(twoPhase=yes)");

	job.addListener(getListener("Job Test 1"));

	System.out.println("Submitting job...");
	try {
	    job.request(contact);
	    System.out.println("job submited: " + job.getIDAsString());
	} catch(WaitingForCommitException e) {
	    System.out.println("Two phase commit: sending COMMIT_REQUEST signal");
	    try {
		job.signal(GramJob.SIGNAL_COMMIT_REQUEST);
	    } catch(Exception ee) {
		ee.printStackTrace();
		return false;
	    }
	} catch(GramException e) {
	    e.printStackTrace();
	    return false;
	} catch(GSSException e) {
	    e.printStackTrace();
	    return false;
	}
	
	System.out.println("Sleeping...");
	try {
	    Thread.sleep(2000);
	} catch(Exception e) {
	}
	
	try {
	    if (cancelCall) {
		System.out.println("Canceling job... (cancel call)");
		job.cancel();
	    } else {
		System.out.println("Canceling job... (cancel signal)");
		job.signal(GramJob.SIGNAL_CANCEL, " ");
	    }
	} catch(GramException e) {
	    e.printStackTrace();
	    return false;
	} catch(GSSException e) {
	    e.printStackTrace();
	    return false;
	}

	System.out.println("Two phase commit: sending COMMIT_END signal");
	try {
	    job.signal(GramJob.SIGNAL_COMMIT_END);
	} catch(Exception ee) {
	    ee.printStackTrace();
	    return false;
	}

	return true;
    }
    
    public static boolean test2(String contact) {
	
	GramJob job = new GramJob("&(executable=/bin/sleep)(arguments=20)(twoPhase=yes)");
	
	job.addListener(getListener("Job Test 2"));

	System.out.println("Submitting job...");
	try {
	    job.request(contact);
	    System.out.println("job submited: " + job.getIDAsString());
	} catch(WaitingForCommitException e) {
            System.out.println("Two phase commit: sending COMMIT_EXTEND signal");
            try {
                job.signal(GramJob.SIGNAL_COMMIT_EXTEND, "30");
            } catch(Exception ee) {
                ee.printStackTrace();
		return false;
            }
	} catch(GramException e) {
	    e.printStackTrace();
	    return false;
	} catch(GSSException e) {
	    e.printStackTrace();
	    return false;
	}
	
	System.out.println("Waiting for timeout...");
	try {
	    Thread.sleep(75000);
	} catch(Exception e) {
	}

	if (job.getStatus() == job.STATUS_FAILED) {
	    System.out.println("Error: Timeout expired!");
	    return false;
	} else if (job.getStatus() == job.STATUS_UNSUBMITTED) {
	    return true;
	}

	return true;
    }

    /**
     * Restart example.
     */
    public static boolean test3(String contact) {
	
	String rsl = "&(executable=/bin/sleep)(arguments=50)(saveState=yes)(twoPhase=yes)";

        GramJob job = new GramJob(rsl);

	job.addListener(getListener("Job Test 3"));

        System.out.println("Submitting job...");
        try {
            job.request(contact);
            System.out.println("job submited: " + job.getIDAsString());
        } catch(WaitingForCommitException e) {
            System.out.println("Two phase commit: sending COMMIT_REQUEST signal");
            try {
                job.signal(GramJob.SIGNAL_COMMIT_REQUEST);
            } catch(Exception ee) {
                ee.printStackTrace();
                return false;
            }
        } catch(GramException e) {
            e.printStackTrace();
            return false;
        } catch(GSSException e) {
            e.printStackTrace();
            return false;
        }

	System.out.println("Stopping job manager...");
	try {
	    job.signal(GramJob.SIGNAL_STOP_MANAGER);
	} catch(Exception e) {
	    e.printStackTrace();
	    return false;
	}
	
	System.out.println("Restarting the job...");
	job = new GramJob(rsl + "(restart=" + job.getIDAsString() + ")");
	job.addListener(getListener("Job Test 3"));
	try {
	    job.request(contact);
	    System.out.println("New job id: " +  job.getIDAsString() );
        } catch(WaitingForCommitException e) {
            System.out.println("Two phase commit: sending COMMIT_REQUEST signal");
            try {
                job.signal(GramJob.SIGNAL_COMMIT_REQUEST);
            } catch(Exception ee) {
                ee.printStackTrace();
                return false;
            }
	} catch(GramException e) {
	    e.printStackTrace();
	    return false;
	} catch(GSSException e) {
	    e.printStackTrace();
	    return false;
	}
	
	try {
	    Thread.sleep(5000);
	    System.out.println("Cancelling job...");
	    job.cancel();
	} catch(Exception e) {
	    e.printStackTrace();
	    return false;
	}

	System.out.println("Two phase commit: sending COMMIT_END signal");
        try {
            job.signal(GramJob.SIGNAL_COMMIT_END);
        } catch(Exception ee) {
            ee.printStackTrace();
            return false;
        }
	
	return true;
    }
    

    public static boolean test4(String contact) {

	boolean sendCommit = false;

        String url = null;
        GassServer s = null;
        try {
            s = new GassServer();
	    s.registerDefaultDeactivator();
            url = s.getURL();
        } catch(Exception e) {
            e.printStackTrace();
            return false;
        }

	System.out.println("Gass server running at: " + url);
        String exe = url + "/" + System.getProperty("user.dir") + "/tests/test.sh";
	
        System.out.println(exe);

        GramJob job = new GramJob("&(saveState=yes)(twoPhase=yes)(executable=" + exe + ")(stdout=" + url + "/dev/stdout)(stderr=" + url + "/dev/stderr)");
	
        job.addListener(getListener("Job Test 4"));
	
	try {
            job.request(contact);
            System.out.println("job submitted : " +  job.getIDAsString() );
        } catch(WaitingForCommitException e) {
            System.out.println("Two phase commit: sending COMMIT_REQUEST signal");
            try {
                job.signal(GramJob.SIGNAL_COMMIT_REQUEST);
		sendCommit = true;
            } catch(Exception ee) {
                ee.printStackTrace();
                return false;
            }
        } catch(GramException e) {
            e.printStackTrace();
            return false;
        } catch(GSSException e) {
            e.printStackTrace();
            return false;
        }

	try {
	    Thread.sleep(5000);
	} catch(Exception e) {}

	// what this should do?

	System.out.println("Checking stdio positions/sizes...");
	try {
            job.signal(GramJob.SIGNAL_STDIO_SIZE, "1000 1000");
	    System.out.println("STDIO_SIZE signal should throw an error.");
	    return false;
        } catch(Exception e) {
            e.printStackTrace();
        }
	

	try {
            Thread.sleep(2000);
        } catch(Exception e) {}
	
	System.out.println("Cancelling job...");
        try {
	    job.cancel();
	    if (sendCommit) {
		System.out.println("Two phase commit: sending COMMIT_END signal");
		job.signal(GramJob.SIGNAL_COMMIT_END);
	    }
        } catch(Exception ee) {
            ee.printStackTrace();
            return false;
        }
	
        return true;
    }

    public static boolean test5(String contact) {
	
	String url = null;
	GassServer s = null;
	try {
	    s = new GassServer();
	    s.registerDefaultDeactivator();
	    url = s.getURL();
	} catch(Exception e) {
	    e.printStackTrace();
	    return false;
	}
	
	System.out.println("Gass server running at: " + url);
	String exe = url + "/" + System.getProperty("user.dir") + "/tests/test.sh";

	System.out.println(exe);

        GramJob job = new GramJob("&(twoPhase=yes)(executable=" + exe + ")(stdout=" + url + "/dev/stdout)(stderr=" + url + "/dev/stderr)");
	
        job.addListener(getListener("Job Test 4"));

        System.out.println("Submitting job...");
        try {
            job.request(contact);
            System.out.println("job submitted : " +  job.getIDAsString() );
        } catch(WaitingForCommitException e) {
            System.out.println("Two phase commit: sending COMMIT_REQUEST signal");
            try {
                job.signal(GramJob.SIGNAL_COMMIT_REQUEST);
            } catch(Exception ee) {
                ee.printStackTrace();
                return false;
            }
        } catch(GramException e) {
            e.printStackTrace();
            return false;
        } catch(GSSException e) {
            e.printStackTrace();
            return false;
        }

	try {
	    Thread.sleep(5000);
	} catch(Exception e) {}

	try {
	    s.shutdown();
	    s = new GassServer();
	    s.registerDefaultDeactivator();
	    url = s.getURL();
	    System.out.println("new gass server: " + url);
	    job.signal(GramJob.SIGNAL_STDIO_UPDATE, "&(stdout=" + url + "/dev/stdout)(stdoutPosition=4)(stderrPosition=0)");
	} catch(Exception e) {
	    e.printStackTrace();
	    return false;
	}

	return true;
    }

    public static void main(String [] args) {
    
	String contact = null;

	if (args.length == 0) {
	    System.err.println("Usage: java GramTest [resource manager]");
	    System.exit(1);
	}

	contact = args[0];

	System.out.println("TEST 1 : " + test1(contact, true));
	System.out.println();
	System.out.println("TEST 2 : " + test1(contact, false));
        System.out.println();
	System.out.println("TEST 3 : " + test3(contact));
	System.out.println();
	System.out.println("TEST 4 : " + test2(contact));
	System.out.println();
	System.out.println("TEST 5 : " + test4(contact));
	System.out.println();
	System.out.println("TEST 6 : " + test5(contact));
	System.out.println();

	try {
	    while ( Gram.getActiveJobs() != 0 ) {
		Thread.sleep(2000);
	    }
	} catch(Exception e) {}
	
        Deactivator.deactivateAll();
    }
}

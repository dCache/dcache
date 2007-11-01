//______________________________________________________________________________
//
// $Id: JobPriorityPolicyInterface.java,v 1.1 2006-10-24 07:44:43 litvinse Exp $
// $Author: litvinse $
//
// created 10/06 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

package org.dcache.srm.scheduler.policies;
import org.dcache.srm.scheduler.Job;

public interface JobPriorityPolicyInterface {
        public int evaluateJobPriority(int size, int position, int n_running, int n_max, Job j);
}


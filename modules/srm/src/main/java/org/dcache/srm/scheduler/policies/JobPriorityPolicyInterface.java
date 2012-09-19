//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 10/06 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

package org.dcache.srm.scheduler.policies;
import org.dcache.srm.request.Job;

public interface JobPriorityPolicyInterface {
        public int evaluateJobPriority(int size, int position, int n_running, int n_max, Job j);
}


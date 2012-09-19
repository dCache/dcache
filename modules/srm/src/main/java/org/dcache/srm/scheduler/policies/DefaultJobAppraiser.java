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

public class  DefaultJobAppraiser implements JobPriorityPolicyInterface {
	public DefaultJobAppraiser() {
	}
        @Override
        public int evaluateJobPriority(int size,
					  int position,
					  int n_running,
					  int n_max,
					  Job job) {

		if(n_running>n_max) {
			return size*(job.getPriority()+1)-position;
		}
		return size*2*(job.getPriority()+1)-position;
        }
}

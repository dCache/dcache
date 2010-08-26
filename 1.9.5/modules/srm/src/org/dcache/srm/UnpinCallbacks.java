// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.2  2004/08/06 19:35:22  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.3  2004/06/15 22:15:41  timur
// added cvs logging tags and fermi copyright headers at the top
//

/*
COPYRIGHT STATUS: 
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
*/

/*
 * StageAndPinCompanionCallbacks.java
 *
 * Created on January 2, 2003, 2:10 PM
 */

package org.dcache.srm;


/**
 * This interface is used for asyncronous notification of SRM of the varios 
 * states of actions performed  for "getting"file from the storage
 *
 * Instance of this class is passed to implementation
 * of AbstractStorageElement.prepareToGet(...) method
 * storage should call its methods in this order:
 *
 * if error occured at any time, call GetStorageInfoFailed,
 * SelectReadPoolFailed, PinRequestFailed, Exception, Timeout
 * or Error and do not make any further calls, nullify the 
 * reference to this PrepareToGetCallbacks instance to allow garbage 
 * collection
 *
 * if everything works fine, do the following,
 * discover the info about the requested file
 * create the instance of the FileId for the requested file and call 
 * StorageInfoArrived with FileId and file size as arguments
 *
 * if StorageInfoArrived returns false no futher action is needed
 *
 * if StorageInfoArrived returns true, file has to be brought to the fast access
 * state (staged) . once it is done 
 * call SelectReadPool with the name of the storage element 
 * that will host this file after it is brought to the fast access state (staged)
 * (name of the pool in case of dCache) 
 * the AbstractStorageElement.unPinFile(FileId fileId,String pool) will be called 
 * with after the SRM does not need this resource anymore.
 *
 * if system does not support staging and pinning pass some arbitrary
 * nonnull string.
 *
 * If SelectReadPool returns false no futher action is needed
 *
 * If SelectReadPool returns true, pin the file in the "fast access state"
 * and call Pinned()
 * if system does not support pinning, just call Pinned() anyway
 *
 *Keep the file in the pinned state untill
 *
 *AbstractStorageElement.unPinFile(FileId fileId,String pool) is called.
 *
 *
 *
 * if Storage
 * @author  timur
 */
public interface UnpinCallbacks {
    
    public void UnpinningFailed(String reason);
    
    public void Unpinned(String pinId);
    
    public void Exception(Exception e);
    
    public void Timeout();
    
    public void Error(String error);
}

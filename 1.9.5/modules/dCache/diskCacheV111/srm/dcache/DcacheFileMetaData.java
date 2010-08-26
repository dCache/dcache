// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.4  2005/03/11 21:17:27  timur
// making srm compatible with cern tools again
//
// Revision 1.3  2005/01/25 05:17:31  timur
// moving general srm stuff into srm repository
//
// Revision 1.2  2004/08/06 19:35:22  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.2  2004/06/15 22:15:42  timur
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
 * DcacheFileMetaData.java
 *
 * Created on May 21, 2004, 3:14 PM
 */

package diskCacheV111.srm.dcache;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.util.PnfsId;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.srm.SRMUser;
import org.apache.log4j.Logger;
/**
 *
 * @author  timur
 */
public class DcacheFileMetaData extends org.dcache.srm.FileMetaData {
    private PnfsId pnfsId;
    private StorageInfo storageInfo;
    private diskCacheV111.util.FileMetaData fmd;
    private static final Logger logger =  Logger.getLogger(DcacheFileMetaData.class);
    
    /** Creates a new instance of DcacheFileMetaData */
    public DcacheFileMetaData(PnfsId pnfsId) {
        super();
        this.pnfsId = pnfsId;
        this.fileId = pnfsId.toIdString();
    }
    
    public  DcacheFileMetaData(PnfsId pnfsId,diskCacheV111.srm.FileMetaData fmd) {
        super(fmd);
        this.pnfsId = pnfsId;
        this.fileId = pnfsId.toIdString();
    }
    
    /** Getter for property pnfsId.
     * @return Value of property pnfsId.
     *
     */
    public diskCacheV111.util.PnfsId getPnfsId() {
        return pnfsId;
    }
    
    /** Getter for property storageInfo.
     * @return Value of property storageInfo.
     *
     */
    public diskCacheV111.vehicles.StorageInfo getStorageInfo() {
        return storageInfo;
    }
    
    /** Setter for property storageInfo.
     * @param storageInfo New value of property storageInfo.
     *
     */
    public void setStorageInfo(diskCacheV111.vehicles.StorageInfo storageInfo) {
        this.storageInfo = storageInfo;
    }
    
    /** Getter for property fmd.
     * @return Value of property fmd.
     *
     */
    public diskCacheV111.util.FileMetaData getFmd() {
        return fmd;
    }
    
    /** Setter for property fmd.
     * @param fmd New value of property fmd.
     *
     */
    public void setFmd(diskCacheV111.util.FileMetaData fmd) {
        this.fmd = fmd;
    }
    
    public  boolean isOwner(SRMUser user) {
        try {
            return Integer.parseInt(owner) == ((AuthorizationRecord) user).getUid();
        } catch (NumberFormatException nfe) {
            logger.error("owner is not a number: "+owner,nfe);
            throw nfe;
        } catch (ClassCastException  cce) {
            logger.error("user is not a dCacheUser: "+user,cce);
            throw cce;
        } 
    }
    
    public boolean isGroupMember(SRMUser user) {
        try {
            return Integer.parseInt(group) == ((AuthorizationRecord) user).getGid();
        } catch (NumberFormatException nfe) {
            logger.error("group is not a number: "+group,nfe);
            throw nfe;
        } catch (ClassCastException  cce) {
            logger.error("user is not a dCacheUser: "+user,cce);
            throw cce;
        } 
        
    }

}

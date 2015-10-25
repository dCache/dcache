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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Set;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.auth.Subjects;
import org.dcache.namespace.FileAttribute;
import org.dcache.srm.SRMUser;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TFileLocality;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.namespace.FileAttribute.*;

/**
 *
 * @author  timur
 */
public class DcacheFileMetaData extends org.dcache.srm.FileMetaData {
    private static final long serialVersionUID = 4486472517160693148L;
    private PnfsId pnfsId;
    private FileAttributes attributes;
    private static final Logger logger =  LoggerFactory.getLogger(DcacheFileMetaData.class);

    /** Creates a new instance of DcacheFileMetaData */
    public DcacheFileMetaData(PnfsId pnfsId) {
        super();
        this.pnfsId = pnfsId;
        this.fileId = pnfsId.toIdString();
    }

    public DcacheFileMetaData(PnfsId pnfsId,diskCacheV111.srm.FileMetaData fmd) {
        super(fmd);
        this.pnfsId = pnfsId;
        this.fileId = pnfsId.toIdString();
    }

    public DcacheFileMetaData(FileAttributes attributes)
    {
        super();

        this.attributes = attributes;

        isPinned = false;
        isPermanent = true;
        isRegular = false;
        isDirectory = false;
        isLink = false;
        locality = TFileLocality.NONE;

        for (FileAttribute attribute: attributes.getDefinedAttributes()) {
            switch (attribute) {
            case PNFSID:
                pnfsId = attributes.getPnfsId();
                fileId = pnfsId.toString();
                break;

            case CHECKSUM:
                /* Find the adler32 checksum. If not found, then take
                 * some other checksum.
                 */
                Set<Checksum> checksums = attributes.getChecksums();
                for (Checksum checksum: checksums) {
                    checksumType = checksum.getType().getName().toLowerCase();
                    checksumValue = checksum.getValue();
                    if (checksum.getType() == ChecksumType.ADLER32 ) {
                        break;
                    }
                }
                break;

            case OWNER:
                owner = Integer.toString(attributes.getOwner());
                break;

            case OWNER_GROUP:
                group = Integer.toString(attributes.getGroup());
                break;

            case MODE:
                permMode = attributes.getMode();
                break;

            case TYPE:
                switch (attributes.getFileType()) {
                case REGULAR:
                    isRegular = true;
                    break;
                case DIR:
                    isDirectory = true;
                    break;
                case LINK:
                    isLink = true;
                    break;
                case SPECIAL:
                    break;
                }
                break;

            case ACCESS_TIME:
                lastAccessTime = attributes.getAccessTime();
                break;

            case MODIFICATION_TIME:
                lastModificationTime = attributes.getModificationTime();
                break;

            case CREATION_TIME:
                creationTime = attributes.getCreationTime();
                break;

            case SIZE:
                size = attributes.getSize();
                break;

            case RETENTION_POLICY:
                TAccessLatency latency = null;
                if (attributes.isDefined(ACCESS_LATENCY)) {
                    if (attributes.getAccessLatency().equals(AccessLatency.ONLINE)) {
                        latency = TAccessLatency.ONLINE;
                    } else if (attributes.getAccessLatency().equals(AccessLatency.NEARLINE)) {
                        latency = TAccessLatency.NEARLINE;
                    }
                }

                TRetentionPolicy retention = null;
                if (attributes.getRetentionPolicy().equals(RetentionPolicy.CUSTODIAL)) {
                    retention = TRetentionPolicy.CUSTODIAL;
                } else if (attributes.getRetentionPolicy().equals(RetentionPolicy.REPLICA)) {
                    retention = TRetentionPolicy.REPLICA;
                } else if (attributes.getRetentionPolicy().equals(RetentionPolicy.OUTPUT)) {
                    retention = TRetentionPolicy.OUTPUT;
                }
                // RetentionPolicy is non-nillable element of the
                // TRetentionPolicyInfo, if retetion is null, we shold leave
                // the whole retentionPolicyInfo null
                if (retention != null) {
                    retentionPolicyInfo =
                            new TRetentionPolicyInfo(retention, latency);
                }
                break;

            case STORAGEINFO:
                StorageInfo storage_info =
                        attributes.getStorageInfo();
                isStored = storage_info.isStored();
                if (storage_info.getMap() != null) {
                    String writeToken = storage_info.getMap().get("writeToken");
		    if (writeToken != null) {
                        spaceTokens = new long[1];
                        try {
                            spaceTokens[0] = Long.parseLong(writeToken);
                        } catch (NumberFormatException e) {}
		    }
                }
                break;
            }
        }
    }


    /** Getter for property pnfsId.
     * @return Value of property pnfsId.
     *
     */
    public diskCacheV111.util.PnfsId getPnfsId() {
        return pnfsId;
    }

    public void setFileAttributes(FileAttributes attributes)
    {
        this.attributes = attributes;
    }

    public FileAttributes getFileAttributes()
    {
        return attributes;
    }

    @Override
    public  boolean isOwner(SRMUser user) {
        try {
            long uid = Subjects.getUid(((DcacheUser) user).getSubject());
            return Long.parseLong(owner) == uid;
        } catch (NumberFormatException nfe) {
            logger.error("owner is not a number: "+owner,nfe);
            throw nfe;
        } catch (ClassCastException  cce) {
            logger.error("user is not a dCacheUser: "+user,cce);
            throw cce;
        }
    }

    @Override
    public boolean isGroupMember(SRMUser user) {
        try {
            long gid = Subjects.getPrimaryGid(((DcacheUser) user).getSubject());
            return Long.parseLong(group) == gid;
        } catch (NumberFormatException nfe) {
            logger.error("group is not a number: "+group,nfe);
            throw nfe;
        } catch (ClassCastException  cce) {
            logger.error("user is not a dCacheUser: "+user,cce);
            throw cce;
        }

    }

    /**
     * Returns the set of FileAttributes understood by this class.
     */
    public static Set<FileAttribute> getKnownAttributes()
    {
        return EnumSet.of(PNFSID, STORAGEINFO, CHECKSUM,
                OWNER, OWNER_GROUP, MODE, TYPE, SIZE,
                ACCESS_TIME, MODIFICATION_TIME, CREATION_TIME,
                ACCESS_LATENCY, RETENTION_POLICY);
    }
}

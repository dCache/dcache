//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 10/06 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

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
 * SrmGetPermissionClientV2.java
 *
 * Created on June 23, 2006, 4:33 PM
 */

package gov.fnal.srm.util;

import org.apache.axis.types.URI;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;

import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.v2_2.ArrayOfTGroupPermission;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.SrmSetPermissionRequest;
import org.dcache.srm.v2_2.SrmSetPermissionResponse;
import org.dcache.srm.v2_2.TGroupPermission;
import org.dcache.srm.v2_2.TPermissionMode;
import org.dcache.srm.v2_2.TPermissionType;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

public class SRMSetPermissionClientV2 extends SRMClient {
    //
    // SRM v2.2 WSDL srmSetPermission requires non-nullable groupid string
    // dCache SRM ignores this value and uses group id it retrieves from
    // file metadata on the server side. SRM developers agreed to treat hyphen
    // as "unspecified". No change of ownership happens.
    //
    private static final String DEFAULT_DUMMY_GROUP_ID = "-";
    private GSSCredential cred;
    private GlobusURL surl;
    private String surl_string;
    private ISRM isrm;

    public SRMSetPermissionClientV2(Configuration configuration,
                                    GlobusURL surl, String surl_string) {
        super(configuration);
        this.surl       = surl;
        this.surl_string = surl_string;
        try {
            cred = getGssCredential();
        }
        catch (Exception e) {
            cred = null;
            System.err.println("Couldn't getGssCredential.");
        }
    }

    @Override
    public void connect() throws Exception {
        GlobusURL srmUrl = surl;
        isrm = new SRMClientV2(srmUrl,
                getGssCredential(),
                configuration.getRetry_timeout(),
                configuration.getRetry_num(),
                doDelegation,
                fullDelegation,
                gss_expected_name,
                configuration.getWebservice_path(),
                configuration.getTransport());
    }

    @Override
    public void start() throws Exception {
        try {
            if (cred.getRemainingLifetime() < 60) {
                throw new Exception(
                        "Remaining lifetime of credential is less than a minute.");
            }
        }
        catch (GSSException gsse) {
            throw gsse;
        }
        URI uri = new URI(surl_string);
        SrmSetPermissionRequest req = new SrmSetPermissionRequest();
        req.setSURL(uri);
        TPermissionType type = TPermissionType.fromString(configuration.getSetPermissionType());
        req.setPermissionType(type);
        TPermissionMode mode = null;
        if ( configuration.getSetOwnerPermissionMode() != null ) {
            mode = TPermissionMode.fromString(configuration.getSetOwnerPermissionMode());
        }
        req.setOwnerPermission(mode);
        ArrayOfTGroupPermission arrayOfGroupPermissions = new ArrayOfTGroupPermission();
        TGroupPermission grouppermissions[] = null;
        if ( configuration.getSetGroupPermissionMode()!=null ) {
            grouppermissions = new  TGroupPermission[1];
            grouppermissions[0] = new TGroupPermission();
            grouppermissions[0].setMode(TPermissionMode.fromString(configuration.getSetGroupPermissionMode()));
            grouppermissions[0].setGroupID(DEFAULT_DUMMY_GROUP_ID);
        }
        arrayOfGroupPermissions.setGroupPermissionArray(grouppermissions);
        req.setArrayOfGroupPermissions(arrayOfGroupPermissions);
        TPermissionMode other = null;
        if ( configuration.getSetOtherPermissionMode()!=null) {
            other = TPermissionMode.fromString(configuration.getSetOtherPermissionMode());
        }
        req.setOtherPermission(other);
        SrmSetPermissionResponse resp = isrm.srmSetPermission(req);
        try {
            TReturnStatus rs   = resp.getReturnStatus();
            if (rs.getStatusCode() != TStatusCode.SRM_SUCCESS) {
                TStatusCode rc  = rs.getStatusCode();
                StringBuilder sb = new StringBuilder();
                sb.append("Return code: ").append(rc.toString()).append("\n");
                sb.append("Explanation: ").append(rs.getExplanation())
                        .append("\n");
                System.out.println(sb.toString());
                System.exit(1);
            }
            else {
                System.exit(0);
            }
        }
        catch (Exception e) {
            throw e;
        }
    }

}

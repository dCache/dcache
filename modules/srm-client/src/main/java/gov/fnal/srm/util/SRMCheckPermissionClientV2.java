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
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ArrayOfTSURLPermissionReturn;
import org.dcache.srm.v2_2.SrmCheckPermissionRequest;
import org.dcache.srm.v2_2.SrmCheckPermissionResponse;
import org.dcache.srm.v2_2.TPermissionMode;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLPermissionReturn;
import org.dcache.srm.v2_2.TStatusCode;

public class SRMCheckPermissionClientV2 extends SRMClient {

    private final java.net.URI[] surls;
    private final String[] surl_string;

    public SRMCheckPermissionClientV2(Configuration configuration,
          java.net.URI[] surls, String[] surl_string) {
        super(configuration);
        this.surls = surls;
        this.surl_string = surl_string;
    }

    @Override
    protected java.net.URI getServerUrl() {
        return surls[0];
    }

    @Override
    public void start() throws Exception {
        checkCredentialValid();
        ArrayOfAnyURI surlarray = new ArrayOfAnyURI();
        URI[] uriarray = new URI[surl_string.length];
        URI uri;
        for (int i = 0; i < uriarray.length; i++) {
            uri = new URI(surl_string[i]);
            uriarray[i] = uri;
        }
        surlarray.setUrlArray(uriarray);
        SrmCheckPermissionRequest req = new SrmCheckPermissionRequest();
        req.setArrayOfSURLs(surlarray);
        configuration.getStorageSystemInfo().ifPresent(req::setStorageSystemInfo);
        SrmCheckPermissionResponse resp = srm.srmCheckPermission(req);
        try {
            TReturnStatus rs = resp.getReturnStatus();
            if (rs.getStatusCode() != TStatusCode.SRM_SUCCESS) {
                TStatusCode rc = rs.getStatusCode();
                StringBuilder sb = new StringBuilder();
                sb.append("Return code: ").append(rc.toString()).append("\n");
                sb.append("Explanation: ").append(rs.getExplanation())
                      .append("\n");
                System.out.println(sb.toString());
            }
            ArrayOfTSURLPermissionReturn permissions = resp.getArrayOfPermissions();
            TSURLPermissionReturn[] permissionarray = permissions.getSurlPermissionArray();
            StringBuilder txt = new StringBuilder();
            for (TSURLPermissionReturn permission : permissionarray) {
                txt.append("# file  : ").append(permission.getSurl())
                      .append("\n");
                if (rs.getStatusCode() != TStatusCode.SRM_SUCCESS) {
                    txt.append("Return code: ")
                          .append(permission.getStatus()
                                .getStatusCode().toString()).append("\n");
                    txt.append("Explanation: ")
                          .append(permission.getStatus()
                                .getExplanation()).append("\n");
                    if (permission.getStatus()
                          .getStatusCode() != TStatusCode.SRM_SUCCESS) {
                        continue;
                    }
                }
                TPermissionMode mode = permission.getPermission();
                txt.append("permission mode:").append(mode.toString())
                      .append("\n");
            }
            System.out.println(txt.toString());
            if (rs.getStatusCode() != TStatusCode.SRM_SUCCESS) {
                System.exit(1);
            } else {
                System.exit(0);
            }
        } catch (Exception e) {
            throw e;
        }
    }
}

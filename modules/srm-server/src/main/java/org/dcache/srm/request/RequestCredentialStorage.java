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
 * JobCreatorStorage.java
 *
 * Created on May 13, 2004, 10:14 AM
 */

package org.dcache.srm.request;

import java.io.IOException;
import org.dcache.util.SqlGlob;


/**
 * @author timur
 */

public interface RequestCredentialStorage {

    RequestCredential getRequestCredential(Long requestCredentialId);

    void saveRequestCredential(RequestCredential requestCredential);

    /**
     * Return the credential with the longest remaining lifetime that matches the given name and
     * fqan exactly.  If role is null then only credentials that have no primary FQAN match.
     */
    RequestCredential getRequestCredential(String name, String role);

    /**
     * Return the credential with the longest remaining lifetime that matches the given name.  The
     * returned credential may have any primary FQAN or have no primary FQAN at all.  If there is no
     * match then null is returned.
     */
    RequestCredential getRequestCredential(String name);

    /**
     * Search for the credential with the longest remaining lifetime that matches the name and role
     * Globs.  If role is null then only a credential with no primary FQAN is returned.  If role is
     * specified then only a credential with a matching primary FQAN is returned.
     */
    RequestCredential searchRequestCredential(SqlGlob name, SqlGlob role);

    /**
     * Check where there is any stored credential for this user.  If role is non-null then the role
     * must match.  If role is null then any stored credential for this user will match.
     */
    boolean hasRequestCredential(String credentialName, String role)
          throws IOException;

    /**
     * Delete all delegated credentials that match the credential name and role.  If role is null
     * then only the credential name is used as a predicate.  Returns true if one or more
     * credentials were deleted.
     */
    boolean deleteRequestCredential(String credentialName, String role)
          throws IOException;
}

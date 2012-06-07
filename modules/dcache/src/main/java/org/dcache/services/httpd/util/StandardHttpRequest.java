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
package org.dcache.services.httpd.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.Map;
import java.util.StringTokenizer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import dmg.protocols.kerberos.Base64;
import dmg.util.CollectionFactory;
import dmg.util.HttpRequest;

/**
 * Basic implementation of internal interface used to hold objects for processing
 * an Http request.
 *
 * @author arossi
 */
public class StandardHttpRequest implements HttpRequest {
    private final static Splitter PATH_SPLITTER
        = Splitter.on('/').omitEmptyStrings();
    private final static Logger logger
        = LoggerFactory.getLogger(StandardHttpRequest.class);

    private final OutputStream out;
    private final PrintWriter pw;
    private final HttpServletRequest request;
    private final HttpServletResponse response;
    private final Map<String, String> map = CollectionFactory.newHashMap();
    private final int tokenOffset;
    private final String[] tokens;
    private final boolean isDirectory;

    private String userName;
    private String password;
    private boolean authDone = false;

    public StandardHttpRequest(HttpServletRequest request,
                    HttpServletResponse response) throws IOException, URISyntaxException {
        this.request = request;
        this.response = response;
        out = response.getOutputStream();
        pw = new PrintWriter(new OutputStreamWriter(out));
        final Enumeration<String> names = request.getHeaderNames();
        while (names.hasMoreElements()) {
            final String name = names.nextElement();
            map.put(name, request.getHeader(name));
        }
        final String path = new URI(request.getRequestURI()).getPath();
        isDirectory = path.endsWith("/");
        tokens = Iterables.toArray(PATH_SPLITTER.split(path), String.class);
        tokenOffset = 1;
        setContentType("text/html");
    }

    @Override
    public OutputStream getOutputStream() {
        return out;
    }

    @Override
    public String getParameter(String parameter) {
        return request.getParameter(parameter);
    }

    @Override
    public String getPassword() {
        doAuthorization();
        return password;
    }

    @Override
    public PrintWriter getPrintWriter() {
        return pw;
    }

    @Override
    public Map<String, String> getRequestAttributes() {
        return map;
    }

    @Override
    public int getRequestTokenOffset() {
        return tokenOffset;
    }

    @Override
    public String[] getRequestTokens() {
        return tokens;
    }

    @Override
    public String getUserName() {
        return userName;
    }

    @Override
    public boolean isAuthenticated() {
        doAuthorization();
        return userName != null;
    }

    @Override
    public boolean isDirectory() {
        return isDirectory;
    }

    @Override
    public void printHttpHeader(int size) {
        if (size > 0) {
            response.setContentLength(size);
        }
    }

    @Override
    public void setContentType(String type) {
        response.setContentType(type);
    }

    private synchronized void doAuthorization() {
        if (authDone) {
            return;
        }
        authDone = true;
        String auth = request.getHeader("Authorization");
        if (auth == null) {
            return;
        }
        StringTokenizer st = new StringTokenizer(auth);
        if (st.countTokens() < 2) {
            return;
        }
        if (!st.nextToken().equals("Basic")) {
            return;
        }
        auth = new String(Base64.decode(st.nextToken()));
        logger.info("Authentication : >{}<", auth);
        st = new StringTokenizer(auth, ":");
        if (st.countTokens() < 2) {
            return;
        }
        userName = st.nextToken();
        password = st.nextToken();
    }
}

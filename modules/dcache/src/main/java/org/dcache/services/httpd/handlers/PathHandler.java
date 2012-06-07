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
package org.dcache.services.httpd.handlers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.FileNameMap;
import java.net.URLConnection;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.dcache.services.httpd.util.StandardHttpRequest;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import dmg.util.HttpException;
import dmg.util.HttpRequest;

public class PathHandler extends AbstractHandler {

    private static final FileNameMap mimeTypeMap = URLConnection.getFileNameMap();

    private File path;

    public PathHandler(File path) {
        this.path = path;
    }

    @Override
    public void handle(String target, Request baseRequest,
                    HttpServletRequest request, HttpServletResponse response)
                    throws IOException, ServletException {
        HttpRequest proxy = null;
        try {
            proxy = new StandardHttpRequest(request, response);
            sendFile(path, proxy);
        } catch (Exception t) {
            throw new ServletException("PathHandler failure", t);
        }
    }


    private void sendFile(File base, HttpRequest proxy) throws Exception {
        String filename = null;
        String[] tokens = proxy.getRequestTokens();
        if (tokens.length < 2) {
            filename = "index.html";
        } else {
            final StringBuilder sb = new StringBuilder();
            sb.append(tokens[1]);
            for (int i = 2; i < tokens.length; i++) {
                sb.append("/").append(tokens[i]);
            }
            filename = sb.toString();
        }
        final File f = base.isFile() ? base : new File(base, filename);
        if (!f.getCanonicalFile().getAbsolutePath().startsWith(
                        base.getCanonicalFile().getAbsolutePath())) {
            throw new HttpException(HttpServletResponse.SC_FORBIDDEN,
                            "Forbidden");
        }

        if (!f.isFile()) {
            throw new HttpException(HttpServletResponse.SC_NOT_FOUND,
                            "Not found : " + filename);
        }

        final FileInputStream binary = new FileInputStream(f);
        try {
            int rc = 0;
            final byte[] buffer = new byte[4 * 1024];
            proxy.setContentType(getContentTypeFor(filename));
            while ((rc = binary.read(buffer, 0, buffer.length)) > 0) {
                proxy.getOutputStream().write(buffer, 0, rc);
            }
        } finally {
            proxy.getOutputStream().flush();
            try {
                binary.close();
            } catch (final IOException e) {
            }
        }
    }

    private String getContentTypeFor(String fileName) {
        if (fileName.endsWith(".html")) {
            return "text/html";
        } else if (fileName.endsWith(".css")) {
            return "text/css";
        } else {
            return mimeTypeMap.getContentTypeFor(fileName);
        }
    }
}

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
package org.dcache.web;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;

/**
 * Utility class used for servlet related operations
 */
public class Viewer0 extends HttpServlet {

    /**
     *
     */
    private static final long serialVersionUID = 145956555146546185L;

    public Viewer0() {
        super();
    }

    public void init() throws ServletException {
        return;
    }

    public void service(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException, FileNotFoundException
    {
        String imageDir = null;
        try {
            imageDir = getServletContext().getInitParameter("image.home");
        } catch (Exception e) {
            e.printStackTrace();
        }

        HttpSession session = request.getSession();
        String filename = request.getParameter("name");
        if (filename == null) {
            throw new ServletException("Parameter 'name' must be supplied");
        }
        int idx = filename.lastIndexOf("/");
        if (idx != -1) {
            filename = filename.substring(idx+1);
        }
        idx = filename.lastIndexOf("%2F");
        if (idx != -1) {
            filename = filename.substring(idx+3);
        }
        idx = filename.lastIndexOf("%2f");
        if (idx != -1) {
            filename = filename.substring(idx+3);
        }
        String realPath = getServletContext().getRealPath("/");

        // Process raw data request
        String rawData = request.getParameter("txt");
        if (rawData != null) {
            String query = request.getQueryString();
            getServletContext().getRequestDispatcher("/raw?"+query).forward(request, response);
            return;
        }

        // Process raw data request
        String xmlData = request.getParameter("xml");
        if (xmlData != null) {
            String query = request.getQueryString();
            getServletContext().getRequestDispatcher("/xml?"+query).forward(request, response);
            return;
        }

        // Process statistics request
        String statData = request.getParameter("stat");
        if (statData != null) {
            String query = request.getQueryString();
            getServletContext().getRequestDispatcher("/stats?"+query).forward(request, response);
            return;
        }

        // Process request for particular date
        String day = request.getParameter("date");
        if (day != null) {
            String fmt = request.getParameter("fmt");
            if (fmt.startsWith("log")) {
                String query = request.getQueryString();
                getServletContext().getRequestDispatcher("/servlet/GBilling?"+query).forward(request, response);
            }
        }

        filename = realPath + imageDir + "/" + filename;
        File file = new File(filename);

        if (!file.exists()) {
            // Forward request to 'billing' servlet if file does not exist
            String query = request.getQueryString();
            getServletContext().getRequestDispatcher("/billing?"+query).forward(request, response);
            return;
        }

        // Process regular request
        if (filename.indexOf(".jpg") > 0 || filename.indexOf(".jpeg") > 0) {
            response.setContentType("image/jpeg");
        } else if (filename.indexOf(".png") > 0) {
            response.setContentType("image/png");
        } else if (filename.indexOf(".ps") > 0 || filename.indexOf(".eps") > 0) {
            response.setContentType("application/postscript");
        }
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
        BufferedOutputStream bos = new BufferedOutputStream(response.getOutputStream());
        byte[] input = new byte[1024];
        boolean eof = false;
        while (!eof) {
            int length = bis.read(input);
            if (length == -1) {
                eof = true;
            } else {
                bos.write(input,0,length);
            }
        }
        bis.close();
        bos.close();
    }
}

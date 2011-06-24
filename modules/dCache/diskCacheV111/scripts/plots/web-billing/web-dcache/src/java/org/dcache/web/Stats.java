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
import java.util.ArrayList;

/**
 * Utility class used for servlet related operations
 */
public class Stats extends HttpServlet {

    /**
     *
     */
    private static final long serialVersionUID = -8242002183323110501L;


    public Stats() {
        super();
    }


    public void init() throws ServletException {
        return;
    }


    public void service(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException, FileNotFoundException
    {
        PlotStorage plotStorage = (PlotStorage)getServletContext().getAttribute("org.dcache.web.PlotStorage");
        PrintWriter out = response.getWriter();
        HttpSession session = request.getSession();

        String vrb = request.getParameter("verb");

        String name = request.getParameter("name");
        if (name == null) {
            throw new ServletException("Parameter 'name' must be supplied");
        }
        Plot plot = (Plot) PlotStorage.getPlotSpecifier(name);

        if (vrb != null) {
            response.setContentType("text/plain");
            out.printf("plot.getDsLen()=%d%n", plot.getDsLen());
            for (int i = 0; i < plot.getDsLen(); i++) {
                DataSrc ds = plot.getDataSrc(i);
                ArrayList<Integer> selected = ds.getSelected();
                out.printf("Title %d=%s%n", i, ds.getTitle(i));
                for (int j = 0; j < selected.size(); j++) {
                    Integer s = selected.get(j)-1;
                    out.printf("Index for min/avg/max=%s%n", s);
                }
            }
            return;
        }

        response.setContentType("text/xml");

        out.printf("<?xml version='1.0' encoding='UTF-8'?>%n");
        out.printf("<stats>%n");
        for (int i = 0; i < plot.getDsLen(); i++) {
            DataSrc ds = plot.getDataSrc(i);
            ArrayList<Integer> selected = ds.getSelected();
            out.printf("<val>%s</val>%n", ds.getTitle(i));
            for (int j = 0; j < selected.size(); j++) {
                Integer s = selected.get(j)-1;
//              out.printf("<val name='Min%d'>%g</val>%n",i+1, ds.getMins(s));
//              out.printf("<val name='Avg%d'>%g</val>%n",i+1, ds.getAvgs(s));
//              out.printf("<val name='Max%d'>%g</val>%n",i+1, ds.getMaxs(s));
                out.printf("<val name='Min'>%g</val>%n", ds.getMins(s));
                out.printf("<val name='Avg'>%g</val>%n", ds.getAvgs(s));
                out.printf("<val name='Max'>%g</val>%n", ds.getMaxs(s));
            }
        }
        out.printf("</stats>%n");
        out.close();
        return;
    }
}

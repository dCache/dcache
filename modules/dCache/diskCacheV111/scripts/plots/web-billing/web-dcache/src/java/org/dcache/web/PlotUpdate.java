/*
 * $Log: PlotUpdate.java,v $
 * Revision 1.3  2006/07/26 19:46:51  podstvkv
 * Cleanup the code
 *
 * Revision 1.2  2006/07/26 19:35:58  podstvkv
 * Initial import into CVS
 *
 */
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


import java.io.*;
//import java.net.URL;
import java.sql.*;
import javax.naming.*;
import javax.sql.*;
import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.xmlbeans.XmlException;
import org.apache.xmlbeans.XmlObject;

//import java.util.Calendar;
//import java.util.GregorianCalendar;


public class PlotUpdate extends HttpServlet implements Runnable
{

    /**
     *
     */
    private static final long serialVersionUID = -9094230422321694548L;

    interface Command
    {
        public void execute()
        throws ServletException, IOException;
    }

    /**
     * Work with a database table
     */

    public void init(ServletConfig config) throws ServletException
    {
        super.init(config);

        try {
            Context init = new InitialContext();
            Context ctx = (Context) init.lookup("java:comp/env");
            dataSource = (DataSource) ctx.lookup("jdbc/postgres");
        }
        catch (NamingException ex) {
            throw new ServletException("Cannot retrieve java:comp/env/jdbc/postgres",ex);
        }

        try {
            imageDir = getServletContext().getInitParameter("image.home");
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            scriptName = getServletContext().getInitParameter("script.name");
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            confName = getServletContext().getInitParameter("plot.conf");
        } catch (Exception e) {
            e.printStackTrace();
        }

        PlotStorage plotStorage = (PlotStorage)getServletContext().getAttribute("org.dcache.web.PlotStorage");

        if (plotStorage == null) {
            getServletContext().setAttribute("org.dcache.web.PlotStorage", new PlotStorage());
        }

        realPath = getServletContext().getRealPath("/");

        // Create a background thread that will rebuild the plots
        // from the database every half an hour

        updateThread = new Thread(this);
        updateThread.start();
    }

    public void doPost(final HttpServletRequest req, final HttpServletResponse res)
    throws ServletException, IOException
    {
        doGet(req, res);
    }

    /**
     * This methods is called to process the http request, here is the start point
     */
    public void doGet(final HttpServletRequest req, final HttpServletResponse res)
    throws ServletException, IOException
    {

        res.setHeader("pragma", "no-cache");
        res.setHeader("Cache-Control", "no-cache") ;
        res.setDateHeader("Expires", 0);

        res.setContentType("text/html");
        PrintWriter out = res.getWriter();
        out.println("<h3>PlotUpdate Info</h3>");
        out.println("<e>Registered plots:</e><br/>");
        for (Object o : PlotStorage.getStorageKeys()) {
            out.println(o+"<br/>");
        }
        out.close();
        return;
    }


    /**
     * Creates plot for several datasets with individual Xs,Ys and puts it into the file
     * This is the analog of savePlot (which works periodically only) but it works on demand only
     * @param filename
     * @param dataSet
     * @param gnuSetup
     * @throws ServletException
     * @throws IOException
     */
    private void createPlot(final String filename, final DataSrc[] dataSet, final GnuSetup gnuSetup)
    throws ServletException, IOException
    {
        try {
            Process p = Runtime.getRuntime().exec("gnuplot");
            PrintWriter stdOutput = new PrintWriter(new BufferedWriter(new OutputStreamWriter(p.getOutputStream())));

            for (int i = 0; i < gnuSetup.size(); i++) {
                stdOutput.println(gnuSetup.get(i));
            }
            for (int i = 0; i < dataSet.length; i++) {
                stdOutput.print((i==0 ? "plot " : ", ") + dataSet[i].getStyle());
            }
            stdOutput.println();
            try {
                for (int i = 0; i < dataSet.length; i++) {
                    DataSrc cSet = dataSet[i];
                    for (String row = null; cSet.next(); ) {
                        row = cSet.getRow();
                        stdOutput.println(row);
                        log(row);
                    }
                    stdOutput.println("e"); log("e");
                }
                stdOutput.println("quit");
                stdOutput.close(); stdOutput = null;
            }
            catch (SQLException ex) {
                System.out.println("exception happened - here's what I know: ");
                ex.printStackTrace();
                throw new ServletException("Exception in database operation");
            }
            try {
                p.waitFor();
            }
            catch (InterruptedException x) {}

            log("File "+filename+".eps is ready");

            Process pc1 = Runtime.getRuntime().exec("convert -depth 8 -density 100x100 -modulate 95,95 "+filename+".eps "+filename+".png");

            try {
                pc1.waitFor();
            }
            catch (InterruptedException x) {}

            log("File "+filename+".png is ready");

//            sendFile(filename+".png");
            System.gc();
        }
        catch (IOException ex) {
            System.out.println("exception happened - here's what I know: ");
            ex.printStackTrace();
            throw new ServletException("Exception in gnuplot execution");
        }
    }

    /**
     * Creates plot for several datasets with individual Xs,Ys and puts it into the file
     * This is the analog of savePlot (which works periodically only) but it works on demand only
     * @param filename
     * @param styles
     * @param gnuSetup
     * @throws ServletException
     * @throws IOException
     */
    private void createPlot(final String filename, final String[] styles, GnuSetup gnuSetup)
    throws ServletException, IOException
    {
        try {
            Process p = Runtime.getRuntime().exec("gnuplot");
            PrintWriter stdOutput = new PrintWriter(new BufferedWriter(new OutputStreamWriter(p.getOutputStream())));

            String s;
            for (int i = 0; i < gnuSetup.size(); i++) {
                if ((s = gnuSetup.get(i)) != null)
                    stdOutput.println(s);
            }
            for (int i = 0; i < styles.length; i++) {
                stdOutput.print((i==0 ? "plot " : ", ") + styles[i]);
            }
            stdOutput.println();
            stdOutput.println("quit");
            stdOutput.close(); stdOutput = null;
            try {
                p.waitFor();
            }
            catch (InterruptedException x) {}

            log("File "+filename+".eps is ready");

            Process pc1 = Runtime.getRuntime().exec("convert -depth 8 -density 100x100 -modulate 95,95 "+filename+".eps "+filename+".png");

            try {
                pc1.waitFor();
            }
            catch (InterruptedException x) {}

            log("File "+filename+".png is ready");

//            sendFile(filename+".png");
            System.gc();
        }
        catch (IOException ex) {
            System.out.println("exception happened - here's what I know: ");
            ex.printStackTrace();
            throw new ServletException("Exception in gnuplot execution");
        }
    }


    /**
     * This is the method run by the update thread, it calls build*Plots methods once an hour.
     */
    public void run()
    {
        try {
            DigestTableConfig tableConfig = new DigestTableConfig("tableRules.xml");
            tableConfig.digest("tableConfig.xml");
            for (;;) {
                initPlotConf();
                tableConfig.execute(this.dataSource);  // Update DB
                //
                buildStdPlots();
                // Wait half an hour between updates
                Thread.sleep(1000*60*30);
            }
        }
        catch (InterruptedException x) {}
        catch (ServletException x) {
            System.err.println("ServletException while updating the plots");
            x.printStackTrace();
        }
        catch (IOException x) {
          System.err.println("IOException while updating the plots");
          x.printStackTrace();
        }
    }

    /**
     * Initialize Plot Configuration
     */
    private void initPlotConf() {
        try {
            File cdir = new File(".");
            String[] flist = cdir.list();
            for (String n : flist) {
                System.err.println("What initPlotConf has found around: "+n);
            }

//            URL scriptUrl = this.getClass().getClassLoader().getResource(scriptName);
//            System.err.println("Url=" + scriptUrl);
//            URL dotUrl = this.getClass().getClassLoader().getResource(".");
//            System.err.println(".=" + dotUrl);
//            System.err.println("Url.getPath()=" + scriptUrl.getPath());
            System.err.println("About to process: "+realPath+scriptName);
            plotConfig = new PlotConfig(realPath+scriptName);        // Requires JavaScript script name
        } catch (Exception e) {
            System.err.println("Exception happened during script initialization");
            e.printStackTrace();
        }
        try {
            String fname = realPath+confName;
            XmlObject xo = plotConfig.getXmlDoc(fname);  // Requires XML configuration file name
            plotConfig.processXml(xo);
        } catch (IOException e) {
            System.err.println("I/O Exception happened during configuration processing");
            e.printStackTrace();
        } catch (XmlException e) {
            System.err.println("XML Exception happened during configuration processing");
            e.printStackTrace();
        }
    }

    public void destroy()
    {
        updateThread.interrupt();
        updateThread = null;
        super.destroy();
    }

    class PlotLog {
        //
        public PlotLog(String nm, long tm) {
            filename = nm;
            modified = tm;
        }

        public long lastModified() {
            return modified;
        }

        public String getFileName() {
            return filename;
        }

        private long modified;
        private String filename;
    }



    /**
     * Builds the set of annual plots, is called from servlet init method and from periodic update thread
     * This methods is called periodically only
     * @throws ServletException
     * @throws IOException
     */
    private synchronized void buildStdPlots()
    throws ServletException, IOException
    {
        Connection conn = null;
        try {
            synchronized (dataSource) {
                conn = dataSource.getConnection();
            }
            log("Connected to DB");
            // Get plot IDs
            String[] plotIds = plotConfig.getIds();
            // Loop for the plots in the configuration file
            for (String plotId : plotIds) {
                System.out.println("Building plot '" + plotId + "'");
                String fname = realPath+imageDir+"/"+plotId;

                // Get plot Title
                String plotTitle = plotConfig.getPlotTitle(plotId);         //   System.out.println("Title: '" + plotTitle + "'");
                // Get datasources
                String[][] dataSrc = plotConfig.getPlotDataSource(plotId);  //   FIXME: for (String ps : plotSource) { System.out.println("Source: '" + ps + "'"); }
                // Get Gnuplot setup
                String[] cmd = plotConfig.getGnuSetup(plotId);              //   for (String cc : cmd) { System.out.println("Command: '" + cc + "'"); }
                GnuSetup gnuSetup = new GnuSetup(cmd); gnuSetup.setTitle(plotTitle).setOutput(fname);
                // Get dataseries
                String[][] dataSeries = plotConfig.getGnuDataSet(plotId);   //   for (String ds : dataSeries) { System.out.println("Dataseries: '" + ds + "'"); }
                for (String[] ds : dataSeries) {
                    gnuSetup.addDataSrcName(ds[0]);                             // Store data source name
                    gnuSetup.addDataSetTitle(ds[1]);                           // Store dataset title
                    gnuSetup.addDataStyle(ds[2]+" title '"+ds[1]+"'");      // Store dataset plot style string
                }

                System.err.println("filename: "+fname);
//                System.err.println("dds: "+dataSeries);
                System.err.println("gnu: "+gnuSetup);
                // Here we have everything for the plot - title, []datasource, gnusetup, []dataset
                Plot plot = new Plot(conn, plotId, plotTitle, dataSrc);
                plot.register();        // Put the plot into the registry
                plot.buildPlot(fname, new EpsPlotBuilder(), gnuSetup);
                log("Save "+fname);
            }
            for (Object k : PlotStorage.getStorageKeys()) {
                System.err.println("Registered: "+k);
            }
        }
        catch (SQLException x) {
            log("Unable to update plot data", x);
        }
        finally {
            try {
                if (conn != null) conn.close();
            }
            catch (SQLException x) {}
        }
    }


    protected DataSource dataSource;
    private Thread updateThread;

    private String imageDir;
    private String scriptName;
    private String confName;
    private String realPath;
    private PlotConfig plotConfig;
}

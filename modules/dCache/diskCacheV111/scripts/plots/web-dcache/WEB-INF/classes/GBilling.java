import java.sql.*;
import java.io.*;
import java.awt.*;
import java.awt.image.*;
import javax.swing.*;
import javax.swing.border.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.File;
import java.util.Date;
import java.util.Calendar;
import gov.fnal.isd.*;

public class GBilling extends HttpServlet implements Runnable
{

    interface Command
    {
	public void execute()
	    throws ServletException, IOException;
    }

    public void init(ServletConfig config) throws ServletException
    {
	try {
	    m_driver = Class.forName("org.postgresql.Driver");
	}
	catch (ClassNotFoundException x) {
	    throw new ServletException("Could not load database driver");
	}

	super.init(config);

	try {
	    DBurl = getServletContext().getInitParameter("DBurl");
	} catch (Exception e) {
	    e.printStackTrace();
	}

	try {
	    DBusr = getServletContext().getInitParameter("DBusr");
	} catch (Exception e) {
	    e.printStackTrace();
	}

	try {
	    DBpwd = getServletContext().getInitParameter("DBpwd");
	} catch (Exception e) {
	    e.printStackTrace();
	}

	try {
	    imageDir = getServletContext().getInitParameter("image.home");
	} catch (Exception e) {
	    e.printStackTrace();
	}

	realPath = getServletContext().getRealPath("/");

	log("Call fillHistograms() from init");
	try {
	    fillHistograms();
	}
	catch (IOException x) {
	    throw new ServletException("Could not fill histograms");
	}

	// Create a background thread that will refill the histograms
	// from the database each hour

	update_thread = new Thread(this);
	update_thread.start();
    }

    public void doPost(final HttpServletRequest req, final HttpServletResponse res)
	throws ServletException, IOException
    {
	doGet(req, res);
    }

    public void doGet(final HttpServletRequest req, final HttpServletResponse res)
	throws ServletException, IOException
    {

	res.setHeader("pragma", "no-cache");
	res.setHeader("Cache-Control", "no-cache") ;
	res.setDateHeader("Expires", 0);
	Command c = new Command() {
		private String filename = req.getParameter("filename");

		private String[] items;
		private Date start = null;
		private Date end = null;

		private String gtitle = "Title title title";
		private String ylabel = "Bytes";
		private String xlabel = "Date";
		private Histogram1D dcHist, enHist;
		private String pltFmt;

		public void execute() throws ServletException, IOException
		{
		    if (filename == null) {
			res.setContentType("text/html");
			PrintWriter out = res.getWriter();
			out.println("<h4>Filename parameter has to be specified</h4>");
			return;
		    }

		    String dayOfMonth = req.getParameter("day");
		    if (dayOfMonth == null) {
			throw new ServletException("'day' parameter has to be specified");
		    }

		    pltFmt = req.getParameter("fmt");
		    if (pltFmt == null) {
			pltFmt = "lin";
		    }

		    String[] items = filename.split("[-.]"); // Split the filename into the items

		    int nyy = new Integer(items[1]).intValue()-1900;
		    int nmm = new Integer(items[2]).intValue()-1;
		    int nday = new Integer(dayOfMonth).intValue();
		    String htype = items[4];

		    start = new Date(nyy, nmm, nday);
		    end   = new Date(nyy, nmm, nday+1);

		    log("Start="+start+" End="+end+" # items="+items.length);

		    for (int i = 0; i < items.length; i++)
			log("'"+items[i]+"'");

		    if (htype.startsWith("brd")) {
			gtitle = "Bytes Read";
			ylabel = "Bytes";
		    }
		    else if (htype.startsWith("bwr")) {
			gtitle = "Bytes Written";
			ylabel = "Bytes";
		    }
		    else if (htype.startsWith("trd")) {
			gtitle = "Read Transfers";
			ylabel = "Transfers";
		    }
		    else if (htype.startsWith("twr")) {
			gtitle = "Write Transfers";
			ylabel = "Transfers";
		    } else {
			throw new ServletException("Unknown file type");
		    }

		    log("gtitle="+gtitle);
		    log("xlabel="+xlabel);

		    time1 = new Date().getTime();
		    time2 = new Date().getTime();
		    fillHistogram(htype, start, end);
		    //createPlot();
		    //sendAsGIF(req,res,image);
		    time3 = new Date().getTime();
		}
		//
		private void fillHistogram(String htype, Date sdate, Date edate)
		    throws ServletException, IOException
		{
		    // Open the database.

		    Connection con = null;
		    try
			{
			    con = DriverManager.getConnection(DBurl, DBusr, DBpwd);

			    Statement stmt = con.createStatement();		    log("Connected to DB");
			    // Create simple table for plot creation log
			    try {
				stmt.executeQuery("SELECT * FROM PlotLog");
			    }
			    catch (Exception ex) {
				stmt.executeUpdate("CREATE TABLE PlotLog (Year int, Month int, dateStamp TIMESTAMP, fileName VARCHAR(255))");
			    }

			    Calendar rightNow = Calendar.getInstance();
			    int yyyy = rightNow.get(Calendar.YEAR);
			    int yy = yyyy - 1900;
			    int mm   = rightNow.get(Calendar.MONTH);

			    PlotLog[] plotlog = new PlotLog[mm];

			    ResultSet rset = stmt.executeQuery("SELECT dateStamp,action,transferSize,isNew FROM billingInfo" +
							       " WHERE errorcode=0 and dateStamp > '" + sdate + "' and datestamp < '" + edate + "' order by dateStamp");
			    log("rset fetchsize="+rset.getFetchSize());


			    Timestamp tsStart = new Timestamp(sdate.getTime());

			    Timestamp tsEnd = new Timestamp(edate.getTime());

			    boolean isBrd = htype.startsWith("brd");
			    boolean isBwr = htype.startsWith("bwr");
			    boolean isTrd = htype.startsWith("trd");
			    boolean isTwr = htype.startsWith("twr");
			    String dcTitle = "XX", enTitle = "YY";

			    if (isBrd) {
				dcTitle = "Bytes read from dCache";
				enTitle = "Bytes read from enstore";
			    } else if (isBwr) {
				dcTitle = "Bytes written to dCache";
				enTitle = "Bytes written to enstore";
			    } else if (isTrd) {
				dcTitle = "Read transfers from dCache";
				enTitle = "Read transfers from enstore";
			    } else if (isTwr) {
				dcTitle = "Write transfers to dCache";
				enTitle = "Write transfers to enstore";
			    }

			    dcHist = new Histogram1D(dcTitle, tsStart, tsEnd);
			    enHist = new Histogram1D(enTitle, tsStart, tsEnd);
			    log("Process query");

			    Timestamp date;
			    String action;
			    double transferSize;
			    boolean isNew;

			    while (rset.next()) {
				date = rset.getTimestamp("dateStamp");
				if (date == null) {
				    log("No date"); continue;
				}
				action = rset.getString("action");
				//transferSize = (double)rset.getInt("transferSize");
				transferSize = rset.getDouble("transferSize");
				isNew = rset.getBoolean("isNew");

				if (isBrd) {
				    if (action.startsWith("transfer") && !isNew) {
					dcHist.fill(date, transferSize);  // Select reads from dCache
				    }
				} else if (isTrd) {
				    if (action.startsWith("transfer") && !isNew) {
					dcHist.fill(date);                // Select reads from dCache
				    }
				} else if (isBwr) {
				    if (action.startsWith("transfer") && isNew) {
					dcHist.fill(date, transferSize);  // Select writes to dCache
				    }
				} else if (isTwr) {
				    if (action.startsWith("transfer") && isNew) {
					dcHist.fill(date);                // Select writes to dCache
				    }
				}
			    }
			    rset.close();

			    rset = stmt.executeQuery("SELECT dateStamp,action,fullSize FROM storageInfo" +
						     " WHERE errorcode=0 and dateStamp > '" + sdate + "' and datestamp < '" + edate + "' order by dateStamp");
			    log("rset fetchsize="+rset.getFetchSize());

			    log("Process query");

			    while (rset.next()) {
				date = rset.getTimestamp("dateStamp");
				if (date == null) {
				    log("No date"); continue;
				}
				action = rset.getString("action");
				//transferSize = (double)rset.getInt("fullSize");
				transferSize = rset.getDouble("fullSize");

				if (isBrd) {
				    if (action.startsWith("restore")) {
					enHist.fill(date, transferSize);  // Select reads from enstore
				    }
				} else if (isTrd) {
				    if (action.startsWith("restore")) {
					enHist.fill(date);                // Select reads from enstore
				    }
				} else if (isBwr) {
				    if (action.startsWith("store")) {
					enHist.fill(date, transferSize);  // Select writes to enstore
				    }
				} else if (isTwr) {
				    if (action.startsWith("store")) {
					enHist.fill(date);                // Select writes to enstore
				    }
				}
			    }
			    rset.close();

			    stmt.close();
			    log("dcHist.entries="+dcHist.getEntries());
			    log("enHist.entries="+enHist.getEntries());

			    log("dcHist.NumberOfBins="+dc_bwr.getNumberOfBins());
			    log("dcHist.NumberOfBins="+dc_twr.getNumberOfBins());
			    /*
			      for (int ibin = 1; ibin <= dcHist.getNumberOfBins(); ibin++) {
			      log("dcHist["+ibin+"]="+dcHist.getY(ibin));
			      }

			      double[] ddd = dcHist.getBins();

			      log("dcHist.getBins().length="+ddd.length);

			      for (int ibin = 0; ibin < ddd.length; ibin++) {
			      log("dcHist["+ibin+"]="+ddd[ibin]);
			      }
			    */

			    String fnam = "/tmp/img-"+rightNow.getTimeInMillis();
			    log("Save "+fnam+".eps");
			    createPlot(fnam,
				       new String[] {"XXXXXXXXXXXXXXXXX","Date","Bytes"},
				       new Histogram1D[] {dcHist, enHist}, sdate, edate);
			}
		    catch (SQLException x)
			{
			    log("Unable to get data from DB", x);
			}
		    finally
			{
			    try
				{
				    if (con != null) con.close();
				}
			    catch (SQLException x) {}
			}
		}

		/**
		 * Send the file to the browser
		 *
		 */
		private void sendFile(final String filename)
		    throws ServletException, IOException
		{
		    File file = new File(filename);
		    if (file.exists()) {
			//res.setContentType("image/jpeg");
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
			BufferedOutputStream bos = new BufferedOutputStream(res.getOutputStream());
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
			bos.flush();
			bis.close();
			bos.close();
		    } else {
			throw new ServletException("File not found: "+file.getAbsolutePath());
		    }
		}

		private void createPlot(final String filename,
					final String[] titles,
					final Histogram1D[] hist,
					final Date start,
					final Date end)
		    throws ServletException, IOException
		{
		    try {
			Process p = Runtime.getRuntime().exec("./gnuplot -");

			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

			BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

			PrintWriter stdOutput = new PrintWriter(new BufferedWriter(new OutputStreamWriter(p.getOutputStream())));

			// Initial settings for gnuplot
			log("start gnuplot for "+filename);
			//stdOutput.println("set size 2,2");
			//stdOutput.println("set terminal postscript eps color solid 'Arial' 32");
			stdOutput.println("set size 1.5,1.5");
			stdOutput.println("set terminal postscript eps color solid 'Arial' 24");
			//stdOutput.println("set xlabel '"+xlabel+": "+start+"'");
			String[] df = start.toString().split(" ");
			stdOutput.println("set xlabel '"+xlabel+": "+df[0]+" "+df[1]+" "+df[2]+" "+df[5]+" "+"'");
			stdOutput.println("set timefmt '%H:%M'");
			stdOutput.println("set xdata time");
			//stdOutput.println("set xrange ['"+start.getHours()+":"+start.getMinutes()+"':'"+end.getHours()+":"+end.getMinutes()+"']");
			stdOutput.println("set xrange [ : ]");
			stdOutput.println("set grid");
			if (pltFmt.startsWith("log")) {
			    stdOutput.println("set log y");
			    stdOutput.println("set yrange [1: ]");
			} else {
			    stdOutput.println("set nolog y");
			    stdOutput.println("set yrange [ : ]");
			}
			stdOutput.println("set format x '%H:%M'");

			stdOutput.println("set output '"+filename+".eps'");
			Date now = new Date();
			stdOutput.println("set title '"+gtitle+"(Plotted: "+now+")'");
			int ih;
			/*
			long nEnt = 0;
			double totl = 0.0;
			for (ih = 0; ih < hist.length-1; ih++) {
			    nEnt += hist[ih].getEntries();
			    totl += hist[ih].getTotal();
			}
			stdOutput.println("set key right top Right samplen 1 title 'Total: "+totl+"\nNumber of entries: "+nEnt+"'");
			*/
			stdOutput.println("set ylabel '"+ylabel+"'");

			int lw;
			lw = 40;
			stdOutput.print("plot ");
			for (ih = 0; ih < hist.length-1; ih++) {
			    //stdOutput.print("'-' using 1:2 t '"+hist[ih].getName()+"'  with im lw "+lw+" "+(ih+1)+", ");
			    if (pltFmt.startsWith("log")) {
				log("'-' using 1:2 t '"+hist[ih].getName()+"'  with steps lw 3 "+(2*ih+1)+" , ");
				stdOutput.print("'-' using 1:2 t '"+hist[ih].getName()+"'  with steps lw 3 "+(2*ih+1)+" , ");
			    } else {
				log("'-' using 1:2 t '"+hist[ih].getName()+"'  with boxes "+(2*ih+1)+" , ");
				stdOutput.print("'-' using 1:2 t '"+hist[ih].getName()+"'  with boxes "+(2*ih+1)+" , ");
			    }
			}
			//stdOutput.println("'-' using 1:2 t '"+hist[ih].getName()+"'  with im lw "+lw+" "+(ih+1));
			if (pltFmt.startsWith("log")) {
			    log("'-' using 1:2 t '"+hist[ih].getName()+"'  with steps lw 3 "+(2*ih+1));
			    stdOutput.println("'-' using 1:2 t '"+hist[ih].getName()+"'  with steps lw 3 "+(2*ih+1));
			} else {
			    log("'-' using 1:(-$2) t '"+hist[ih].getName()+"'  with boxes "+(2*ih+1));
			    stdOutput.println("'-' using 1:(-$2) t '"+hist[ih].getName()+"'  with boxes "+(2*ih+1));
			}

			Timestamp ix;
			double iy;
			Histogram1D ihist;
			long nEnt = 0;
			double totl = 0.0;

			for (ih = 0; ih < hist.length; ih++) {
			    ihist = hist[ih];
			    for (int ib = 1; ib <= ihist.getNumberOfBins(); ib++) {
				ix = ihist.getX(ib);
				iy = ihist.getY(ib);
				nEnt++;
				totl += iy;
				stdOutput.println(ix.getHours()+":"+ix.getMinutes()+" "+iy);
				//log(ix.getHours()+":"+ix.getMinutes()+" "+iy);
			    }
			    stdOutput.println("e");
			    log("# Entries="+nEnt+" Total="+totl);
			    //log("e");
			    stdOutput.flush();
			}

			stdOutput.flush();
			stdOutput.close();

			try {
			    p.waitFor();
			}
			catch (InterruptedException x) {}

			log("File "+filename+".eps is ready");

			Process pc1 = Runtime.getRuntime().exec("convert -geometry 720x720 -modulate 95,95 "+filename+".eps "+filename+".png");

			try {
			    pc1.waitFor();
			}
			catch (InterruptedException x) {}

			log("File "+filename+".png is ready");

			//getServletContext().getRequestDispatcher("/servlet/gov.fnal.isd.Viewer0?filename="+filename+".png").include(req, res);

			sendFile(filename+".png");
		    }
		    catch (IOException ex) {
			System.out.println("exception happened - here's what I know: ");
			ex.printStackTrace();
			//System.exit(-1);
			throw new ServletException("Exception in gnuplot execution");
		    }
		}
	    };
	c.execute();
    }


   /**
    * This is the method run by the update thread, it just calls fillHistograms
    * once an hour.
    */
    public void run()
    {
	try {
	    for (;;)
		{
		    // Wait an hour between updates
		    Thread.sleep(1000*60*60);
		    fillHistograms();
		}
	}
	catch (InterruptedException x) {}
	catch (ServletException x) {}
	catch (IOException x) {}
    }

    public void destroy()
    {
	update_thread.interrupt();
	update_thread = null;
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

    /* This code is here just for references
       CREATE TABLE billingInfo (                      CREATE TABLE storageInfo (
       dateStamp TIMESTAMP,			        dateStamp TIMESTAMP,
       cellName CHAR(64),			        cellName CHAR(64),
       action CHAR(40),				        action CHAR(40),
       transaction CHAR(64),			        transaction CHAR(64),
       pnfsID CHAR(24),				        pnfsID CHAR(24),
       fullSize numeric,			        fullSize numeric,
       transferSize numeric,
       storageClass CHAR(40),			        storageClass CHAR(40),
       isNew BOOLEAN,
       client CHAR(40),
       connectionTime numeric,			        connectionTime numeric,
                            			        queuedTime numeric,
       errorCode numeric,    			        errorCode numeric,
       errorMessage CHAR(255)				errorMessage VARCHAR(255)
       );                                               );
    */

   private synchronized void fillHistograms()
       throws ServletException, IOException
   {
      // Open the database.

      Connection con = null;
      try
	  {
	      con = DriverManager.getConnection(DBurl, DBusr, DBpwd);

	      Statement stmt = con.createStatement();
	      log("Connected to DB");
	      // Create simple table for plot creation log
	      try {
		  stmt.executeQuery("SELECT * FROM PlotLog");
	      }
	      catch (Exception ex) {
		  stmt.executeUpdate("CREATE TABLE PlotLog (Year int, Month int, dateStamp TIMESTAMP, fileName VARCHAR(255))");
	      }

	      Calendar rightNow = Calendar.getInstance();
	      int yyyy = rightNow.get(Calendar.YEAR);
	      int yy = yyyy - 1900;
	      int mm   = rightNow.get(Calendar.MONTH);

	      PlotLog[] plotlog = new PlotLog[mm];

	      // Process billingInfo
	      ResultSet rset = stmt.executeQuery("SELECT dateStamp,action,transferSize,isNew FROM billingInfo" +
						 " WHERE errorcode=0 and dateStamp LIKE '" + yyyy + "-%' order by dateStamp");
	      log("rset fetchsize="+rset.getFetchSize());

	      Timestamp tsStart = new Timestamp(new Date(yy, 0, 1).getTime());

	      //rset.last();
	      //Timestamp tsEnd = rset.getTimestamp("dateStamp");
	      rightNow.add(Calendar.DATE, 1);  // Add one day to have todays statistics in the histogram
	      int eyy = rightNow.get(Calendar.YEAR)-1900;
	      int emm = rightNow.get(Calendar.MONTH);
	      int edd = rightNow.get(Calendar.DATE);
	      Timestamp tsEnd = new Timestamp(new Date(eyy, emm, edd).getTime());
	      // Special case - beginning of new year
	      if ((tsEnd.getTime()-tsStart.getTime())/(1000*3600*24) < 28) {
		  tsEnd = new Timestamp(new Date(eyy, emm+1, 1).getTime());
	      }

	      log("eyy="+eyy+" emm="+emm+" edd="+edd);
	      log("tsEnd="+tsEnd);

	      dc_brd = new Histogram1D("Bytes read from dCache", tsStart, tsEnd); // Bytes read
	      en_brd = new Histogram1D("Bytes read from enstore", tsStart, tsEnd);

	      dc_bwr = new Histogram1D("Bytes written to dCache", tsStart, tsEnd); // Bytes written
	      en_bwr = new Histogram1D("Bytes written to enstore", tsStart, tsEnd);

	      dc_trd = new Histogram1D("Read transfers from dCache", tsStart, tsEnd); // Read transfers
	      en_trd = new Histogram1D("Read transfers from enstore", tsStart, tsEnd);

	      dc_twr = new Histogram1D("Write transfers to dCache", tsStart, tsEnd); // Write transfers
	      en_twr = new Histogram1D("Write transfers to enstore", tsStart, tsEnd);

	      log("Process billingInfo");

	      //rset.beforeFirst();

	      while (rset.next()) {
		  Timestamp date = rset.getTimestamp("dateStamp");
		  if (date == null) {
		      log("No date"); continue;
		  }
		  String action = rset.getString("action");
		  //double transferSize = (double)rset.getInt("transferSize");
		  double transferSize = rset.getDouble("transferSize");
		  boolean isNew = rset.getBoolean("isNew");

		  if (action.startsWith("transfer") && isNew) {
		      dc_bwr.fill(date, transferSize);  // Select writes to dCache
		      dc_twr.fill(date);
		  } else if (action.startsWith("transfer") && !isNew) {
		      dc_brd.fill(date, transferSize);  // Select reads from dCache
		      dc_trd.fill(date);
		  }
	      }
	      rset.close();

	      // Now process storageInfo
	      rset = stmt.executeQuery("SELECT dateStamp,action,fullSize FROM storageInfo" +
				       " WHERE errorcode=0 and dateStamp LIKE '" + yyyy + "-%' order by dateStamp");
	      log("rset fetchsize="+rset.getFetchSize());

	      log("Process storageInfo");

	      while (rset.next()) {
		  Timestamp date = rset.getTimestamp("dateStamp");
		  if (date == null) {
		      log("No date"); continue;
		  }
		  String action = rset.getString("action");
		  //double transferSize = (double)rset.getInt("fullSize");
		  double transferSize = rset.getDouble("fullSize");

		  if (action.startsWith("store")) {
		      en_bwr.fill(date, transferSize);  // Select writes to enstore
		      en_twr.fill(date);
		  } else if (action.startsWith("restore")) {
		      en_brd.fill(date, transferSize);  // Select reads from enstore
		      en_trd.fill(date);
		  }
	      }
	      rset.close();

	      stmt.close();

	      log("Updated dc_bwr.entries="+dc_bwr.getEntries());
	      log("Updated dc_twr.entries="+dc_twr.getEntries());
	      log("Updated dc_brd.entries="+dc_brd.getEntries());
	      log("Updated dc_trd.entries="+dc_trd.getEntries());

	      log("Updated en_bwr.entries="+en_bwr.getEntries());
	      log("Updated en_twr.entries="+en_twr.getEntries());
	      log("Updated en_brd.entries="+en_brd.getEntries());
	      log("Updated en_trd.entries="+en_trd.getEntries());

	      log("Updated dc_bwr.NumberOfBins="+dc_bwr.getNumberOfBins());
	      log("Updated dc_twr.NumberOfBins="+dc_twr.getNumberOfBins());
	      log("Updated dc_brd.NumberOfBins="+dc_brd.getNumberOfBins());
	      log("Updated dc_trd.NumberOfBins="+dc_trd.getNumberOfBins());

	      log("Updated en_bwr.NumberOfBins="+en_bwr.getNumberOfBins());
	      log("Updated en_twr.NumberOfBins="+en_twr.getNumberOfBins());
	      log("Updated en_brd.NumberOfBins="+en_brd.getNumberOfBins());
	      log("Updated en_trd.NumberOfBins="+en_trd.getNumberOfBins());
	      /*
	      for (int ibin = 1; ibin <= dc_trd.getNumberOfBins(); ibin++) {
		  log("dc_trd["+ibin+"]="+dc_trd.getY(ibin));
	      }

	      double[] ddd = dc_trd.getBins();

	      log("dc_trd.getBins().length="+ddd.length);

	      for (int ibin = 0; ibin < ddd.length; ibin++) {
		  log("dc_trd["+ibin+"]="+ddd[ibin]);
	      }
	      */

	      String fname = "billing-"+yyyy+".daily.eps";


	      //	      Date Jan1 = new Date(yy, 0, 1);
	      //	      Date now  = new Date();
	      fname = realPath+imageDir + "/billing-"+yyyy;

	      saveHistograms(fname+".bwr",
			     new String[] {"Bytes Written","Date","Bytes"},
			     new Histogram1D[] {dc_bwr, en_bwr}, tsStart, tsEnd);
	      //			     new Histogram1D[] {dc_bwr, en_bwr}, Jan1, now);
	      log("Save "+fname+".bwr.eps");

	      saveHistograms(fname+".brd",
			     new String[] {"Bytes Read","Date","Bytes"},
			     new Histogram1D[] {dc_brd, en_brd}, tsStart, tsEnd);
	      log("Save "+fname+".brd.eps");

	      saveHistograms(fname+".twr",
			     new String[] {"Write Transfers","Date","Transfers"},
			     new Histogram1D[] {dc_twr, en_twr}, tsStart, tsEnd);
	      log("Save "+fname+".twr.eps");

	      saveHistograms(fname+".trd",
			     new String[] {"Read Transfers","Date","Transfers"},
			     new Histogram1D[] {dc_trd, en_trd}, tsStart, tsEnd);
	      log("Save "+fname+".trd.eps");

	      //
	      // Here we have to save the histograms for the current month
	      //

	      // Check if the plots exist for all months up to the current one
	      File file = null;
	      for (int ii = 1; ii <= mm+1; ii++) {
		  Date mmStart = new Date(yy, ii-1, 1);
		  Date mmEnd   = new Date((ii == 12) ? yy+1 : yy, (ii == 12) ? 0 : ii, 1);
		  log("Start="+mmStart);
		  log("End  ="+mmEnd);
		  fname = realPath+imageDir + "/billing-"+yyyy+".";
		  if (ii < 10) fname += "0";
		  fname += ii + ".daily.bwr";
		  file = new File(fname+".eps");
		  if (ii > mm || !file.exists()) {
		      // plotlog[ii-1] = new PlotLog(fname, file.lastModified());
		      saveHistograms(fname,
				     new String[] {"Bytes Written","Date","Bytes"},
				     new Histogram1D[] {dc_bwr, en_bwr}, mmStart, mmEnd);
		  }
		  fname = fname.replaceFirst("bwr","brd");
		  file = new File(fname+".eps");
		  if (ii > mm || !file.exists()) {
		      saveHistograms(fname,
				     new String[] {"Bytes Read","Date","Bytes"},
				     new Histogram1D[] {dc_brd, en_brd}, mmStart, mmEnd);
		  }
		  fname = fname.replaceFirst("brd","twr");
		  file = new File(fname+".eps");
		  if (ii > mm || !file.exists()) {
		      saveHistograms(fname,
				     new String[] {"Write Transfers","Date","Transfers"},
				     new Histogram1D[] {dc_twr, en_twr}, mmStart, mmEnd);
		  }
		  fname = fname.replaceFirst("twr","trd");
		  file = new File(fname+".eps");
		  if (ii > mm || !file.exists()) {
		      saveHistograms(fname,
				     new String[] {"Read Transfers","Date","Transfers"},
				     new Histogram1D[] {dc_trd, en_trd}, mmStart, mmEnd);
		  }
	      }
	  }
      catch (SQLException x)
	  {
	      log("Unable to update histograms", x);
	  }
      finally
	  {
	      try
		  {
		      if (con != null) con.close();
		  }
	      catch (SQLException x) {}
	  }
   }
      /*
	ResultSet rsetLog = stmt.executeQuery("SELECT * FROM PlotLog" +
	" WHERE Year=" + yyyy +
	" ORDER BY Month");

	while (rsetLog.next())
	{
	}
      */


    private synchronized void saveHistograms(final String filename,
					     final String[] titles,
					     final Histogram1D[] hist,
					     final Date start,
					     final Date end
					     )
	throws ServletException, IOException
    {
	/*
set size 1.5,1.5
set terminal postscript eps color solid 'Arial' 18
#
set xlabel 'Date'
set timefmt '%m-%d'
set xdata time
set xrange ['11-01':'11-31']
set grid
set yrange [ : ]
set format x '%m-%d'

set output 'billing-2002.11.daily.brd.2.eps'
set title 'Total Bytes Read Per Day (Plotted: Thu Nov  7 09:05:01 2002)'
#set key right top Right samplen 1 title 'Total Bytes : 4.33e+13\nMean Xfer Size : 3.80e+08\n Number of Xfers : 114174L'
set ylabel 'Bytes'
plot 'billing-2002.11.daily' using 1:($3)  t 'reads from dCache'  with im lw 40 2, \
     'billing-2002.11.daily' using 1:(-$5) t 'reads from Enstore' with im lw 40 4

#
set output 'billing-2002.11.daily.bwr.2.eps'
set title 'Total Bytes Written Per Day (Plotted: Thu Nov  7 09:05:01 2002)'
#set key right top Right samplen 1 title 'Total Bytes : 1.98e+13\n Number of Xfers : 41190'
set ylabel 'Bytes'
plot 'billing-2002.11.daily' using 1:($2)  t 'writes to dCache'  with im lw 40 2, \
     'billing-2002.11.daily' using 1:(-$4) t 'writes to Enstore' with im lw 40 4

#
set output 'billing-2002.11.daily.frd.2.eps'
set title 'Total File Reads Per Day (Plotted: Thu Nov  7 09:05:01 2002)'
#set key right top Right samplen 1 title 'Total Bytes : 1.98e+13\n Number of Xfers : 41190'
set ylabel 'Reads'
plot 'billing-2002.11.daily' using 1:($10)  t 'reads from dCache'  with im lw 40 2, \
     'billing-2002.11.daily' using 1:(-$12) t 'reads from Enstore' with im lw 40 4
#
	*/
	Command c = new Command() {
		private String gtitle = titles[0];
		private String xlabel = titles[1];
		private String ylabel = titles[2];

		public void execute() throws ServletException, IOException
		{
		    try {
			Process p = Runtime.getRuntime().exec("./gnuplot -");

			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

			BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

			PrintWriter stdOutput = new PrintWriter(new BufferedWriter(new OutputStreamWriter(p.getOutputStream())));

			// Initial settings for gnuplot
			log("start gnuplot for "+filename);
			//stdOutput.println("set size 2,2");
			//stdOutput.println("set terminal postscript eps color solid 'Arial' 32");
			stdOutput.println("set size 1.5,1.5");
			stdOutput.println("set terminal postscript eps color solid 'Arial' 24");
			stdOutput.println("set xlabel '"+xlabel+"'");
			stdOutput.println("set timefmt '%Y-%m-%d'");
			stdOutput.println("set xdata time");
			stdOutput.println("set xrange ['"+(start.getYear()+1900)+"-"+(start.getMonth()+1)+"-"+start.getDate()+"':'"+(end.getYear()+1900)+"-"+(end.getMonth()+1)+"-"+end.getDate()+"']");
			log("set xrange ['"+(start.getYear()+1900)+"-"+(start.getMonth()+1)+"-"+start.getDate()+"':'"+(end.getYear()+1900)+"-"+(end.getMonth()+1)+"-"+end.getDate()+"']");
			stdOutput.println("set grid");
			stdOutput.println("set yrange [ : ]");
			stdOutput.println("set format x '%m-%d'");

			stdOutput.println("set output '"+filename+".eps'");
			Date now = new Date();
			stdOutput.println("set title '"+gtitle+"(Plotted: "+now+")'");
			//stdOutput.println("set key right top Right samplen 1 title 'Total Bytes : 4.33e+13\nMean Xfer Size : 3.80e+08\n Number of Xfers : 114174L'");
			stdOutput.println("set ylabel '"+ylabel+"'");

			int ih;
			int ndays = (int)((end.getTime()-start.getTime())/1000/3600/24);
			int lw;
			if (ndays > 31)
			    lw = 8;
			else
			    lw = 40;
			stdOutput.print("plot ");
			for (ih = 0; ih < hist.length-1; ih++) {
			    //stdOutput.print("'-' using 1:2 t '"+hist[ih].getName()+"'  with im lw "+lw+" "+(ih+1)+", ");
			    stdOutput.print("'-' using 1:2 t '"+hist[ih].getName()+"'  with boxes "+(ih+1)+", ");
			}
			//stdOutput.println("'-' using 1:2 t '"+hist[ih].getName()+"'  with im lw "+lw+" "+(ih+1));
			    stdOutput.println("'-' using 1:(-$2) t '"+hist[ih].getName()+"'  with boxes "+(ih+1));

			Timestamp ix;
			double iy;
			Histogram1D ihist;
			long nEnt = 0;
			double totl = 0.0;

			for (ih = 0; ih < hist.length; ih++) {
			    ihist = hist[ih];
			    int nbins = ihist.getNumberOfBins();
			    log("saveHistograms: hist="+ih+" nbins="+nbins);
			    for (int ib = 1; ib <= ihist.getNumberOfBins(); ib++) {
				ix = ihist.getX(ib);
				iy = ihist.getY(ib);
				nEnt++;
				totl += iy;
				stdOutput.println((ix.getYear()+1900)+"-"+(ix.getMonth()+1)+"-"+ix.getDate()+" "+iy);
				//stdOutput.println(ib+" "+iy);
				//log(ib+" "+iy);
				//log((ix.getYear()+1900)+"-"+(ix.getMonth()+1)+"-"+ix.getDate()+" "+iy);
				log("X "+ix+": "+iy);
			    }
			    stdOutput.println("e");
			    log("# Entries="+nEnt+" Total="+totl);
			    //log("e");
			    stdOutput.flush();
			}

			stdOutput.flush();
			stdOutput.close();
			try {
			    p.waitFor();
			}
			catch (InterruptedException x) {}
			log("File "+filename+".eps is ready");
			Process pc1 = Runtime.getRuntime().exec("convert -geometry 720x720 -modulate 95,95 "+filename+".eps "+filename+".png");
			Process pc2 = Runtime.getRuntime().exec("convert -geometry 120x120 -modulate 90,50 "+filename+".eps png:"+filename+".pre");

			log("File "+filename+".{png,pre} are ready");
		    }
		    catch (IOException ex) {
			System.out.println("exception happened - here's what I know: ");
			ex.printStackTrace();
			//System.exit(-1);
			throw new ServletException("Exception in gnuplot execution");
		    }
		}
	    };
	c.execute();
    }

    private Histogram1D dc_brd; // Bytes read
    private Histogram1D en_brd;

    private Histogram1D dc_bwr; // Bytes written
    private Histogram1D en_bwr;

    private Histogram1D dc_trd; // Read transfers
    private Histogram1D en_trd;

    private Histogram1D dc_twr; // Write transfers
    private Histogram1D en_twr;

    //
    private static Object m_driver;
    private Thread update_thread;

    private long time0;
    private long time1;
    private long time2;
    private long time3;

    private String DBurl;
    private String DBusr;
    private String DBpwd;
    private String imageDir;
    private String realPath;
}

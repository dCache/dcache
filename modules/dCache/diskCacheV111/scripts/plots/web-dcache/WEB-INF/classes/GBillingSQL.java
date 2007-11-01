/*
  $Log: not supported by cvs2svn $
  Revision 1.5  2003/02/12 16:03:02  cvs
  Implement connection pooling with DataSource

  Revision 1.4  2003/01/30 18:31:17  cvs
  Change the query for ratio table to use index search for performance

  Revision 1.3  2003/01/24 23:54:10  cvs
  Add CVS Log

 */
import java.sql.*;
import java.io.*;
import java.sql.*;
import java.awt.*;
import java.awt.image.*;
import javax.swing.*;
import javax.swing.border.*;
import javax.naming.*;
import javax.sql.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.File;
//import java.util.Date;
import java.util.Calendar;
import gov.fnal.isd.*;

/*
  SQL commands to process the billing info

  1. Create new daily table for dcache:
       select date(datestamp),
         count(datestamp),
	 sum(fullsize) as fullSize,
	 sum(transfersize) as transferSize 
	 into dc_rd_daily
	 from billinginfo 
	 where isnew='f' group by date(datestamp);

  2. Delete the record for the current date:
       delete from dc_rd_daily where date=current_date;

  3. Insert updated record for the current date:
       insert into dc_rd_daily
         select date(datestamp),
	 count(datestamp),
	 sum(fullsize) as fullSize,
	 sum(transfersize) as transferSize 
	 from billinginfo 
	 where date(datestamp)=current_date and isnew='f' group by date(datestamp);

  4. Create new daily table for enstore:
       select date(datestamp),
	 count(datestamp),
         sum(fullsize) as fullSize 
	 into en_rd_daily 
	 from storageinfo 
	 where action='restore' group by date(datestamp);

  4. Generage hourly table:
       select date_trunc('hour',datestamp),
         count(datestamp),
	 sum(fullsize) as fullSize,
	 sum(transfersize) as transferSize 
	 into dc_rd_hourly
	 from billinginfo 
	 where datestamp > current_date and isnew='f' group by date_trunc('hour',datestamp);
 */

public class GBillingSQL extends HttpServlet implements Runnable
{      
   
    interface Command
    {
		public void execute()
			throws ServletException, IOException;
    }

	/**
	 * Work with a table with two columns of data
	 */
    private class DataSet2
    {
		public DataSet2(Connection extCon, String tableName, String where)
		{
			// If connection is not open yet, open it
			if (extCon == null) {
				try	{
					synchronized (dataSource) {
						con = dataSource.getConnection();
					}
				}
				catch (SQLException x) { 
					log("Unable to open connection", x); 
					return;
				}
			} else {
				con = extCon;
			}

			try	{
				// Prepare and execute query
				Statement stmt = con.createStatement();
				
				log("SELECT * FROM "+tableName+" WHERE "+where);
				rset = stmt.executeQuery("SELECT * FROM "+tableName+" WHERE "+where);
				int resultSize = rset.getFetchSize();
				log(tableName+" fetchsize="+resultSize);
				
				dates = new Timestamp[resultSize];
				vals1 = new double[resultSize];
				vals2 = new double[resultSize];

				// Copy the data into internal arrays
				for (int i = 0; rset.next(); i++) {
					dates[i] = rset.getTimestamp(1);
					vals1[i] = rset.getDouble(2);
					vals2[i] = rset.getDouble(3);
				}
			}
			catch (SQLException ex) { 
				System.out.println("exception happened - here's what I know: ");
				ex.printStackTrace();
			}
		}

		public DataSet2(Connection con, String tableName, Date sdate, Date edate)
		{
			// Query can be "date between '" + sdate + "' and '" + edate + "'"
			this(con, tableName, "date >= '"+sdate+"' and date < '"+edate+"'");
		}


		public Timestamp[] getDates()
		{
			return dates;
		}

		public double[] getVals1()
		{
			return vals1;
		}

		public double[] getVals2()
		{
			return vals2;
		}

		private Connection con;
		protected ResultSet rset;
		private Timestamp[] dates;
		private double[] vals1;
		private double[] vals2;
    }

	/**
	 * Work with a table with three columns of data
	 */
    private class DataSet3 extends DataSet2
    {
		public DataSet3(Connection extCon, String tableName, String where)
		{
			super(extCon, tableName, where);
			try	{
				int resultSize = rset.getFetchSize();
				vals3 = new double[resultSize];

				rset.beforeFirst();
				for (int i = 0; rset.next(); i++) {
					vals3[i] = rset.getDouble(4);
				}
			}
			catch (SQLException ex) { 
				System.out.println("exception happened - here's what I know: ");
				ex.printStackTrace();
			}
		}

		public DataSet3(Connection con, String tableName, Date sdate, Date edate)
		{
			this(con, tableName, "date >= '"+sdate+"' and date < '"+edate+"'");
		}

		public double[] getVals3()
		{
			return vals3;
		}

		private double[] vals3;
    }


    public void init(ServletConfig config) throws ServletException
    {
		try {
			Context init = new InitialContext();
			Context ctx = (Context) init.lookup("java:comp/env");
			dataSource = (DataSource) ctx.lookup("jdbc/postgres");
		}
        catch (NamingException ex) {
			throw new ServletException("Cannot retrieve java:comp/env/jdbc/postgres",ex);
        }
	
		super.init(config);
		try {
			imageDir = getServletContext().getInitParameter("image.home");
		} catch (Exception e) {
			e.printStackTrace();
		}
	
		realPath = getServletContext().getRealPath("/");

		try {
			updateDB();
		}
		catch (IOException x) {
			throw new ServletException("Could not build plots");
		}

		log("Call buildPlots() from init");
		try {
			buildPlots();
			buildMonthlyPlots();
		}
		catch (IOException x) {
			throw new ServletException("Could not build plots");
		}
	
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

    public void doGet(final HttpServletRequest req, final HttpServletResponse res)
		throws ServletException, IOException 
    {

		res.setHeader("pragma", "no-cache");
		res.setHeader("Cache-Control", "no-cache") ;
		res.setDateHeader("Expires", 0);  
		Command c = new Command() {
				private String filename = req.getParameter("filename");
		
				private String[] items;
				private Date sDate = null;
				private Date eDate = null;

				private String gtitle = null;
				private String ylabel = null;
				private String xlabel = "Date";
				private String title1 = null;
				private String title2 = null;
				private String pltFmt = null;
				private String ratio = null;

				public void execute() throws ServletException, IOException
				{
					if (filename == null) {
						res.setContentType("text/html");
						PrintWriter out = res.getWriter();
						out.println("<h4>Filename parameter must be specified</h4>");
						return;
					}

					String dayOfMonth = req.getParameter("day");
					if (dayOfMonth == null) {
						throw new ServletException("'day' parameter must be specified");
					}

					pltFmt = req.getParameter("fmt");
					if (pltFmt == null) {
						pltFmt = "lin";
					}

					ratio = req.getParameter("ratio");

					String[] items = filename.split("[-.]"); // Split the filename into the items

					int nyy = new Integer(items[1]).intValue()-1900;
					int nmm = new Integer(items[2]).intValue()-1;
					int nday = new Integer(dayOfMonth).intValue();
					String htype = items[4];

					sDate = new Date(nyy, nmm, nday);
					eDate = new Date(nyy, nmm, nday+1);

					log("sDate="+sDate+" eDate="+eDate+" # items="+items.length);
// 					for (int i = 0; i < items.length; i++) 
// 						log("'"+items[i]+"'");
					log("ratio="+ratio);
					if (ratio == null || ratio.startsWith("0")) 
						buildDailyPlot(htype, sDate, eDate);
					else {
						if (htype.startsWith("brd")) {
							gtitle = "Transfer size/Full size "; xlabel = "Ratio";    ylabel = " ";
							buildRatioPlot(sDate, eDate);
						} else if (htype.startsWith("cst")) {
							gtitle = "Transfer Cost Distribution "; xlabel = "Cost";    ylabel = " ";
							buildCostPlot(sDate, eDate);
						}
					}
				}
				//
				private void buildDailyPlot(String htype, Date sdate, Date edate)
					throws ServletException, IOException
				{
					// Open the database.      
      
					Connection con = null;
					try	{
						synchronized (dataSource) {
							con = dataSource.getConnection();
						}
	      
						Calendar rightNow = Calendar.getInstance();
	      
						String fnam = "/tmp/img-"+rightNow.getTimeInMillis();

						DataSet3 dcData = null;
						DataSet2 enData = null;

						log("Process query");

						if (htype.startsWith("brd")) {
							dcData = new DataSet3(con, "dc_rd_hourly", "datestamp >= '"+sDate+"' and datestamp < '"+eDate+"'");
							enData = new DataSet2(con, "en_rd_hourly", "datestamp >= '"+sDate+"' and datestamp < '"+eDate+"'");
							gtitle = "Bytes Read";	    ylabel = "Bytes"; title1 = "dCache"; title2 = "Enstore"; 
							createPlot(fnam, dcData.getDates(), dcData.getVals3(), enData.getDates(), enData.getVals2(), sdate, edate);
						} else if (htype.startsWith("bwr")) {
							dcData = new DataSet3(con, "dc_wr_hourly", "datestamp >= '"+sDate+"' and datestamp < '"+eDate+"'");
							enData = new DataSet2(con, "en_wr_hourly", "datestamp >= '"+sDate+"' and datestamp < '"+eDate+"'");
							gtitle = "Bytes Written";	ylabel = "Bytes"; title1 = "dCache"; title2 = "Enstore"; 
							createPlot(fnam, dcData.getDates(), dcData.getVals3(), enData.getDates(), enData.getVals2(), sdate, edate);
						} else if (htype.startsWith("trd")) {
							dcData = new DataSet3(con, "dc_rd_hourly", "datestamp >= '"+sDate+"' and datestamp < '"+eDate+"'");
							enData = new DataSet2(con, "en_rd_hourly", "datestamp >= '"+sDate+"' and datestamp < '"+eDate+"'");
							gtitle = "Read Transfers";	ylabel = "Transfers"; title1 = "dCache"; title2 = "Enstore"; 
							createPlot(fnam, dcData.getDates(), dcData.getVals1(), enData.getDates(), enData.getVals1(), sdate, edate);
						} else if (htype.startsWith("twr")) {
							dcData = new DataSet3(con, "dc_wr_hourly", "datestamp >= '"+sDate+"' and datestamp < '"+eDate+"'");
							enData = new DataSet2(con, "en_wr_hourly", "datestamp >= '"+sDate+"' and datestamp < '"+eDate+"'");
							gtitle = "Write Transfers";	ylabel = "Transfers"; title1 = "dCache"; title2 = "Enstore"; 
							createPlot(fnam, dcData.getDates(), dcData.getVals1(), enData.getDates(), enData.getVals1(), sdate, edate);
						} else if (htype.startsWith("hit")) {
							dcData = new DataSet3(con, "hits_hourly", "datestamp >= '"+sDate+"' and datestamp < '"+eDate+"'");
							gtitle = "Cache Hits";	ylabel = "# Hits"; title1 = "Cached"; title2 = "Not Cached"; 
							createPlot(fnam, dcData.getDates(), dcData.getVals3(), dcData.getDates(), dcData.getVals2(), sdate, edate);
						} else if (htype.startsWith("cst")) {
							dcData = new DataSet3(con, "cost_hourly", "datestamp >= '"+sDate+"' and datestamp < '"+eDate+"'");
							gtitle = "Transfer Cost";	ylabel = "Cost"; title1 = "Cost"; title2 = null; 
							createPlot(fnam, dcData.getDates(), dcData.getVals2(),              null, dcData.getVals3(), sdate, edate);
						}
						log("Save "+fnam+".eps");
					}
					catch (SQLException x) { 
						log("Unable to get data from DB", x); 
					}
					finally {
						try	{
							if (con != null) con.close();
						}
						catch (SQLException x) {}
					}
				}

				/**
				 * 
				 *
				 */
				private void buildRatioPlot(Date sdate, Date edate)
					throws ServletException, IOException
				{
					// Open the database.      
      
					Connection con = null;
					try	{
						synchronized (dataSource) {
							con = dataSource.getConnection();
						}
	      
						Calendar rightNow = Calendar.getInstance();
						String fnam = "/tmp/img-"+rightNow.getTimeInMillis();

						log("Process query");

						// Performance String query = "select ratio,count(*) from transfer_ratio where date(datestamp) >= ? and date(datestamp) < ? group by ratio";
						String query = "select ratio,count(*) from transfer_ratio where datestamp >= ? and datestamp < ? group by ratio";
						PreparedStatement pstmt = con.prepareStatement(query);
						pstmt.setObject(1, sdate);
						pstmt.setObject(2, edate);

						ResultSet rset = pstmt.executeQuery();
						int resultSize = rset.getFetchSize();   log("transfer_ratio fetchsize="+resultSize);
			    
						float[] rr = new float[resultSize];
						int[] cc = new int[resultSize];

						for (int i = 0; rset.next(); i++) {
							rr[i] = rset.getFloat(1);  cc[i] = rset.getInt(2);
						}

						createRPlot(fnam, rr, cc, sdate, edate);
						log("Save "+fnam+".eps");
					}
					catch (SQLException x) { 
						log("Unable to get data from DB", x); 
					}
					finally	{
						try	{
							if (con != null) con.close();
						}
						catch (SQLException x) {}
					}
				}

				/**
				 * 
				 *
				 */
				private void buildCostPlot(Date sdate, Date edate)
					throws ServletException, IOException
				{
					// Open the database.      
      
					Connection con = null;
					try	{
						synchronized (dataSource) {
							con = dataSource.getConnection();
						}
	      
						Calendar rightNow = Calendar.getInstance();
						String fnam = "/tmp/img-"+rightNow.getTimeInMillis();

						log("Process query");

						// Performance String query = "select ratio,count(*) from transfer_ratio where date(datestamp) >= ? and date(datestamp) < ? group by ratio";
						String query = "select cost,count(*) from cost_hist where datestamp >= ? and datestamp < ? group by cost";
						PreparedStatement pstmt = con.prepareStatement(query);
						pstmt.setObject(1, sdate);
						pstmt.setObject(2, edate);

						ResultSet rset = pstmt.executeQuery();
						int resultSize = rset.getFetchSize();   log("cost_hist fetchsize="+resultSize);
			    
						float[] rr = new float[resultSize];
						int[] cc = new int[resultSize];

						for (int i = 0; rset.next(); i++) {
							rr[i] = rset.getFloat(1);  cc[i] = rset.getInt(2);
						}

						createHPlot(fnam, rr, cc, sdate, edate);
						log("Save "+fnam+".eps");
					}
					catch (SQLException x) { 
						log("Unable to get data from DB", x); 
					}
					finally	{
						try	{
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
						if (filename.indexOf(".jpg") > 0 || filename.indexOf(".jpeg") > 0) {
							res.setContentType("image/jpeg");
						} else if (filename.indexOf(".png") > 0) {
							res.setContentType("image/png");
						} else if (filename.indexOf(".ps") > 0 || filename.indexOf(".eps") > 0) {
							res.setContentType("application/postscript");
						}
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
										final Timestamp[] x1, final double[] y1,
										final Timestamp[] x2, final double[] y2,
										final Date sDate, final Date eDate)
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
						stdOutput.println("set xlabel '"+xlabel+": "+sDate+"'");
						//String[] df = sDate.toString().split(" ");
						//stdOutput.println("set xlabel '"+xlabel+": "+df[0]+" "+df[1]+" "+df[2]+" "+df[5]+" "+"'");
						stdOutput.println("set timefmt '%H:%M'");
						stdOutput.println("set xdata time");
						//stdOutput.println("set xrange ['"+sDate.getHours()+":"+sDate.getMinutes()+"':'"+eDate.getHours()+":"+eDate.getMinutes()+"']");
						stdOutput.println("set xrange ['00:00' : '23:59']");
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
						//Date now = new Date();
						Calendar now = Calendar.getInstance();
						stdOutput.println("set title '"+gtitle+"(Plotted: "+now.getTime()+")'");
//  					int ih;
// 						long nEnt = 0;
// 						double totl = 0.0;
// 						for (ih = 0; ih < hist.length-1; ih++) {
// 							nEnt += hist[ih].getEntries();
// 							totl += hist[ih].getTotal();
// 						}
// 						stdOutput.println("set key right top Right samplen 1 title 'Total: "+totl+"\nNumber of entries: "+nEnt+"'");
						stdOutput.println("set ylabel '"+ylabel+"'");

						
						Timestamp ix; double iy, ie; long nEnt = 0; double totl = 0.0;
						
						if (x2 != null) {
							if (pltFmt.startsWith("log")) {
								stdOutput.println("plot '-' using 1:2 t '"+title1+"' with steps lw 3 1, '-' using 1:2 t '"+title2+"' with steps lw 3 2");
							} else {
							  //stdOutput.println("plot '-' using 1:2 t '"+title1+"' with boxes 1    , '-' using 1:(-$2) t '"+title2+"' with boxes 3");
								stdOutput.println("plot '-' using 1:2 t '"+title1+"' with imp lw 60 1, '-' using 1:(-$2) t '"+title2+"' with imp lw 60 3");
							}
							for (int ib = 0; ib < x1.length; ib++) {
								ix = x1[ib];    iy = y1[ib];
								nEnt++;    totl += iy;
								stdOutput.println(ix.getHours()+":"+ix.getMinutes()+" "+iy);
								log(ix.getHours()+":"+ix.getMinutes()+" "+iy);
							}
							stdOutput.println("e");
							log("e");
							
							for (int ib = 0; ib < x2.length; ib++) {
								ix = x2[ib];    iy = y2[ib];
								nEnt++;    totl += iy;
								stdOutput.println(ix.getHours()+":"+ix.getMinutes()+" "+iy);
								log(ix.getHours()+":"+ix.getMinutes()+" "+iy);
							}
						} else {
							//stdOutput.println("plot '-' using 1:2:3 t '"+title1+"' with error lw 4 1 ");
							stdOutput.println("plot '-' using 1:2 t '"+title1+"' with linespoints lw 4 1 ");
							for (int ib = 0; ib < x1.length; ib++) {
								ix = x1[ib];    iy = y1[ib];    ie = y2[ib];
								nEnt++;    totl += iy;
								stdOutput.println(ix.getHours()+":"+ix.getMinutes()+" "+iy+ " "+ie);
								log(ix.getHours()+":"+ix.getMinutes()+" "+iy+ " "+ie);
							}
						}
						stdOutput.println("e");
						log("e");
						stdOutput.println("quit");

						log("# Entries="+nEnt+" Total="+totl);
						stdOutput.flush();
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
			
						sendFile(filename+".png");
					}
					catch (IOException ex) {
						System.out.println("exception happened - here's what I know: ");
						ex.printStackTrace();
						//System.exit(-1);
						throw new ServletException("Exception in gnuplot execution");
					}
				}

				private void createRPlot(final String filename, 
										 final float[] xValues, final int[] yValues,
										 final Date sDate, final Date eDate)
					throws ServletException, IOException
				{
					try {
						Process p = Runtime.getRuntime().exec("./gnuplot -");
            
						BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
						BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
						PrintWriter stdOutput = new PrintWriter(new BufferedWriter(new OutputStreamWriter(p.getOutputStream())));

						// Initial settings for gnuplot
						log("start gnuplot for "+filename);
						stdOutput.println("set size 1.5,1.5");
						stdOutput.println("set terminal postscript eps color solid 'Arial' 24");
						stdOutput.println("set xlabel '"+xlabel+" (For "+sDate+")'");
						stdOutput.println("set xrange [ : ]");
						stdOutput.println("set grid");
						stdOutput.println("set log y");
						stdOutput.println("set yrange [1: ]");

						stdOutput.println("set output '"+filename+".eps'");

						Calendar now = Calendar.getInstance();
						stdOutput.println("set title '"+gtitle+"(Plotted: "+now.getTime()+")'");
						stdOutput.println("set ylabel '"+ylabel+"'");

						stdOutput.println("plot '-' using 1:2 t 'Transfer size/Full size' with imp lw 10 1");
			
						float ix; int iy; long nEnt = 0; double totl = 0.0;

						for (int ib = 0; ib < xValues.length; ib++) {
							ix = xValues[ib];    iy = yValues[ib];
							nEnt++;    totl += iy;
							stdOutput.println(ix+" "+iy);
						}
						stdOutput.println("e");

						log("# Entries="+nEnt+" Total="+totl);
						stdOutput.flush();
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
			
						sendFile(filename+".png");
					}
					catch (IOException ex) {
						System.out.println("exception happened - here's what I know: ");
						ex.printStackTrace();
						//System.exit(-1);
						throw new ServletException("Exception in gnuplot execution");
					}
				}

				private void createHPlot(final String filename, 
										 final float[] xValues, final int[] yValues,
										 final Date sDate, final Date eDate)
					throws ServletException, IOException
				{
					try {
						Process p = Runtime.getRuntime().exec("./gnuplot -");
            
						BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
						BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
						PrintWriter stdOutput = new PrintWriter(new BufferedWriter(new OutputStreamWriter(p.getOutputStream())));

						// Initial settings for gnuplot
						log("start gnuplot for "+filename);
						stdOutput.println("set size 1.5,1.5");
						stdOutput.println("set terminal postscript eps color solid 'Arial' 24");
						stdOutput.println("set xlabel '"+xlabel+" (For "+sDate+")'");
						stdOutput.println("set xrange [ : ]");
						stdOutput.println("set grid");
						stdOutput.println("set log y");
						stdOutput.println("set yrange [1: ]");

						stdOutput.println("set output '"+filename+".eps'");

						Calendar now = Calendar.getInstance();
						stdOutput.println("set title '"+gtitle+"(Plotted: "+now.getTime()+")'");
						stdOutput.println("set ylabel '"+ylabel+"'");

						stdOutput.println("plot '-' using 1:2 t 'Transfer cost' with imp lw 16 1");
						// stdOutput.println("plot '-' using 1:2 t 'Transfer cost' with boxes lw 4 1");
						// stdOutput.println("plot '-' using 1:2 t 'Transfer cost' with step lw 4 1");
			
						float ix; int iy; long nEnt = 0; double totl = 0.0;

						for (int ib = 0; ib < xValues.length; ib++) {
							ix = xValues[ib];    iy = yValues[ib];
							nEnt++;    totl += iy;
							stdOutput.println(ix+" "+iy);
						}
						stdOutput.println("e");

						log("# Entries="+nEnt+" Total="+totl);
						stdOutput.flush();
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
	 * This is the method run by the update thread, it just calls buildPlots
	 * once an hour.
	 */
    public void run()
    {
		try {
			for (;;) {
				// Wait half an hour between updates
				Thread.sleep(1000*60*30);
				updateDB();
				buildPlots();
				buildMonthlyPlots();
			}
		}
		catch (InterruptedException x) {}
		catch (ServletException x) {}
		catch (IOException x) {}
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

    /* This code is here just for references
       CREATE TABLE billingInfo (           CREATE TABLE storageInfo (
           dateStamp TIMESTAMP,                 dateStamp TIMESTAMP,
           cellName CHAR(64),                   cellName CHAR(64),
           action CHAR(40),                     action CHAR(40),
           transaction CHAR(64),                transaction CHAR(64),
           pnfsID CHAR(24),                     pnfsID CHAR(24),
           fullSize numeric,                    fullSize numeric,
           transferSize numeric,
           storageClass CHAR(40),               storageClass CHAR(40),
           isNew BOOLEAN,
           client CHAR(40),
           connectionTime numeric,              connectionTime numeric,
           queuedTime numeric,
           errorCode numeric,                   errorCode numeric,
           errorMessage CHAR(255)               errorMessage VARCHAR(255)
       );                                   );
    */

	/**
	 * Update database
	 */
	private synchronized void updateDB()
		throws ServletException, IOException
	{
		// Open the database.      
      
		Connection con = null;
      
		try	{
			synchronized (dataSource) {
				con = dataSource.getConnection();
			}
			
			Statement stmt = con.createStatement();
			log("Connected to DB");
			try {
				// Check if daily tables exist
				stmt.executeQuery("select date from dc_rd_daily where date=(current_date - interval '1 day')");
			}
			catch (Exception ex) {
				// and create them if doesn't
				stmt.executeUpdate("select date(datestamp), count(*), sum(fullsize) as fullSize, sum(transfersize) as transferSize " +
								   "into dc_rd_daily from billinginfo " +
								   "where isnew='f' and errorcode=0 group by date(datestamp)");
				log("Create dc_rd_daily");
			}
			
			try {
				stmt.executeQuery("select date from dc_wr_daily where date=(current_date - interval '1 day')");
			}
			catch (Exception ex) {
				stmt.executeUpdate("select date(datestamp), count(*), sum(fullsize) as fullSize, sum(transfersize) as transferSize " +
								   "into dc_wr_daily from billinginfo " +
								   "where isnew='t' and errorcode=0 group by date(datestamp)");
				log("Create dc_wr_daily");
			}
			
			try {
				stmt.executeQuery("select date from en_rd_daily where date=(current_date-interval '1 day')");
			}
			catch (Exception ex) {
				stmt.executeUpdate("select date(datestamp), count(*), sum(fullsize) as fullSize " +
								   "into en_rd_daily from storageinfo " +
								   "where action='restore' and errorcode=0 group by date(datestamp)");
				log("Create en_rd_daily");
			}
			
			try {
				stmt.executeQuery("select date from en_wr_daily where date=(current_date-interval '1 day')");
			}
			catch (Exception ex) {
				stmt.executeUpdate("select date(datestamp), count(*), sum(fullsize) as fullSize " +
								   "into en_wr_daily from storageinfo " +
								   "where action='store' and errorcode=0 group by date(datestamp)");
				log("Create en_wr_daily");
			}
			
			// New tables: cache hits and cost
			try {
				stmt.executeQuery("select date from hits_daily where date=(current_date-interval '1 day')");
			}
			catch (Exception ex) {
				stmt.executeUpdate("select date(datestamp), count(*), " +
								   "count(nullif(filecached='f','f')) as notcached, " +
								   "count(nullif(filecached='t','f')) as cached " +
								   "into hits_daily from hitinfo " +
								   "where errorcode=0 group by date(datestamp)");
				log("Create hits_daily");
			}
			
			try {
				stmt.executeQuery("select date from cost_daily where date=(current_date-interval '1 day')");
			}
			catch (Exception ex) {
				stmt.executeUpdate("select date(datestamp), count(*), sum(cost), stddev(cost) " +
								   "into cost_daily from costinfo " +
								   "where errorcode=0 group by date(datestamp)");
				log("Create cost_daily");
			}
			
			// At this point we have all tables, so we can safely update the records for the current date
			Calendar cal = Calendar.getInstance(); cal.add(Calendar.DATE, -7);
			Date weekAgo = new Date(cal.getTimeInMillis());
			
			stmt.executeUpdate("delete from dc_rd_daily where date>='"+weekAgo+"'");
			log("delete from dc_rd_daily where date>='"+weekAgo+"'");
			stmt.executeUpdate("insert into dc_rd_daily " +
							   "select date(datestamp), count(datestamp), sum(fullsize) as fullSize, sum(transfersize) as transferSize " +
							   "from billinginfo " +
							   "where datestamp>='"+weekAgo+"' and isnew='f'  and errorcode=0 group by date(datestamp)");
			log("Update dc_rd_daily");
			
			stmt.executeUpdate("delete from dc_wr_daily where date>='"+weekAgo+"'");
			log("delete from dc_wr_daily where date>='"+weekAgo+"'");
			stmt.executeUpdate("insert into dc_wr_daily " +
							   "select date(datestamp), count(datestamp), sum(fullsize) as fullSize, sum(transfersize) as transferSize " +
							   "from billinginfo " +
							   "where datestamp>='"+weekAgo+"' and isnew='t'  and errorcode=0 group by date(datestamp)");
			log("Update dc_wr_daily");
			
			stmt.executeUpdate("delete from en_rd_daily where date>='"+weekAgo+"'");
			log("delete from en_rd_daily where date>='"+weekAgo+"'");
			stmt.executeUpdate("insert into en_rd_daily " +
							   "select date(datestamp), count(datestamp), sum(fullsize) as fullSize " +
							   "from storageinfo " +
							   "where datestamp>='"+weekAgo+"' and action='restore'  and errorcode=0 group by date(datestamp)");
			log("Update en_rd_daily");

			stmt.executeUpdate("delete from en_wr_daily where date>='"+weekAgo+"'");
			log("delete from en_wr_daily where date>='"+weekAgo+"'");
			stmt.executeUpdate("insert into en_wr_daily " +
							   "select date(datestamp), count(datestamp), sum(fullsize) as fullSize " +
							   "from storageinfo " +
							   "where datestamp>='"+weekAgo+"' and action='store'  and errorcode=0 group by date(datestamp)");
			log("Update en_wr_daily");

			// New tables: cache hits and cost
			stmt.executeUpdate("delete from hits_daily where date>='"+weekAgo+"'");
			log("delete from hits_daily where date>='"+weekAgo+"'");
			stmt.executeUpdate("insert into hits_daily " +
							   "select date(datestamp), count(*), " +
							   "count(nullif(filecached='f','f')) as notcached, " +
							   "count(nullif(filecached='t','f')) as cached " +
							   "from hitinfo " +
							   "where errorcode=0 group by date(datestamp)");
			log("Update hits_daily");

			stmt.executeUpdate("delete from cost_daily where date>='"+weekAgo+"'");
			log("delete from cost_daily where date>='"+weekAgo+"'");
			stmt.executeUpdate("insert into cost_daily " +
							   "select date(datestamp), count(*), sum(cost), stddev(cost) " +
							   "from costinfo " +
							   "where errorcode=0 group by date(datestamp)");
			log("Update cost_daily");
		}
		catch (SQLException x) { 
			log("Unable to update DB", x); 
		}
		finally	{
			try	{
				if (con != null) con.close();
			}
			catch (SQLException x) {}
		}
	}

	private synchronized void buildPlots()
		throws ServletException, IOException
	{
		// Open the database.      
      
		Connection con = null;

		Calendar rightNow = Calendar.getInstance();
      
		rightNow.add(Calendar.DATE, 1);                // Add one day to have the current day statistics in the histogram
		int eyyyy = rightNow.get(Calendar.YEAR);
		int eyy = eyyyy-1900;
		int emm = rightNow.get(Calendar.MONTH);
		int edd = rightNow.get(Calendar.DATE);
		Date eDate = new Date(eyy, emm, edd);
      
		rightNow.add(Calendar.YEAR, -1);             // Subtract one year
		int syyyy = rightNow.get(Calendar.YEAR);
		int syy = syyyy-1900;
		int smm = rightNow.get(Calendar.MONTH);
		int sdd = rightNow.get(Calendar.DATE);
		Date sDate = new Date(syy, smm, sdd);
      
		log("sDate="+sDate+" eDate="+eDate);

		try	{
			synchronized (dataSource) {
				con = dataSource.getConnection();
			}
			log("Connected to DB");
	      
			String fname = realPath+imageDir + "/billing";

			// Process reads from dCache
			log("Process dc_rd_daily");
			DataSet3 dc_rd = new DataSet3(con, "dc_rd_daily", "date > '"+sDate+"'");
	      
			// Process reads from enstore
			log("Process en_rd_daily");
			DataSet2 en_rd = new DataSet2(con, "en_rd_daily", "date > '"+sDate+"'");

			savePlots(fname+".brd",
					  new String[] {"Bytes Read","Date","Bytes","dCache","Enstore"}, 
					  //                transfersize                        fullsize
					  dc_rd.getDates(), dc_rd.getVals3(), en_rd.getDates(), en_rd.getVals2(), sDate, eDate);
			log("Save "+fname+".brd.eps");

			savePlots(fname+".trd", 
					  new String[] {"Read Transfers","Date","Transfers","dCache","Enstore"}, 
					  //                counts                              counts
					  dc_rd.getDates(), dc_rd.getVals1(), en_rd.getDates(), en_rd.getVals1(), sDate, eDate);
			log("Save "+fname+".trd.eps");


			// Process writes to dCache
			log("Process dc_wr_daily");
			DataSet3 dc_wr = new DataSet3(con, "dc_wr_daily", "date > '"+sDate+"'");

			// Process writes to enstore
			log("Process en_wr_daily");
			DataSet2 en_wr = new DataSet2(con, "en_wr_daily", "date > '"+sDate+"'");

			savePlots(fname+".bwr",
					  new String[] {"Bytes Written","Date","Bytes","dCache","Enstore"}, 
					  //                transfersize                        fullsize
					  dc_wr.getDates(), dc_wr.getVals3(), en_wr.getDates(), en_wr.getVals2(), sDate, eDate);
			log("Save "+fname+".bwr.eps");

			savePlots(fname+".twr", 
					  new String[] {"Write Transfers","Date","Transfers","dCache","Enstore"}, 
					  //                counts                              counts
					  dc_wr.getDates(), dc_wr.getVals1(), en_wr.getDates(), en_wr.getVals1(), sDate, eDate);
			log("Save "+fname+".twr.eps");


			// Process cache hits
			log("Process hits_daily");
			DataSet3 hitsDaily = new DataSet3(con, "hits_daily", "date > '"+sDate+"'");
			savePlots(fname+".hit",
					  new String[] {"Cache Hits","Date","# Hits","Cached","Not Cached"}, 
					  //                    not cached                                  cached  
					  hitsDaily.getDates(), hitsDaily.getVals3(), hitsDaily.getDates(), hitsDaily.getVals2(), sDate, eDate);
			log("Save "+fname+".hit.eps");


			// Process cost
			log("Process cost_daily");
			DataSet3 costDaily = new DataSet3(con, "cost_daily", "date > '"+sDate+"'");
			savePlots(fname+".cst",
					  new String[] {"Transaction Cost","Date","Cost","Cost"," "}, 
					  //                    cost                                        standard deviation
					  costDaily.getDates(), costDaily.getVals2(),                 null, costDaily.getVals3(), sDate, eDate);
			log("Save "+fname+".cst.eps");

		}
		catch (SQLException x) { 
			log("Unable to update plot data", x); 
		}
		finally	{
			try	{
				if (con != null) con.close();
			}
			catch (SQLException x) {}
		}
	}

	private synchronized void buildMonthlyPlots()
		throws ServletException, IOException
	{
		// Open the database.      
      
		Connection con = null;
		String fname;

		try	{
			synchronized (dataSource) {
				con = dataSource.getConnection();
			}
			log("Connected to DB");
			
			Calendar sCal = Calendar.getInstance(); sCal.set(Calendar.DATE, 1);
			Calendar eCal = Calendar.getInstance(); eCal.set(Calendar.DATE, 1); eCal.add(Calendar.MONTH, 1);
			Date sDate, eDate;
			
			for (int ii = 0; ii < 12; ii++) {
				sDate =  new Date(sCal.getTimeInMillis());
				eDate =  new Date(eCal.getTimeInMillis());
				log("sDate="+sDate+" eDate="+eDate);
				
				int yyyy = sCal.get(Calendar.YEAR);
				int mm   = sCal.get(Calendar.MONTH)+1;
				fname = realPath+imageDir + "/billing-"+yyyy+".";
				if (mm < 10) fname += "0"; 
				fname += mm;
				
				File brdFile = new File(fname+".daily.brd.eps");
				File trdFile = new File(fname+".daily.trd.eps");
				if (ii == 0 || !brdFile.exists() || !trdFile.exists()) {
					// Process reads from dCache
					DataSet3 dc_rd = new DataSet3(con, "dc_rd_daily", sDate, eDate);
					
					// Process reads from enstore
					DataSet2 en_rd = new DataSet2(con, "en_rd_daily", sDate, eDate);
					
					savePlots(fname+".daily.brd", new String[] {"Bytes Read","Date","Bytes","dCache","Enstore"}, 
							  dc_rd.getDates(), dc_rd.getVals3(), en_rd.getDates(), en_rd.getVals2(), sDate, eDate);      log("Save "+fname+".brd.eps");
					
					savePlots(fname+".daily.trd", new String[] {"Read Transfers","Date","Transfers","dCache","Enstore"}, 
							  dc_rd.getDates(), dc_rd.getVals1(), en_rd.getDates(), en_rd.getVals1(), sDate, eDate);      log("Save "+fname+".trd.eps");
					
				}
				
				File bwrFile = new File(fname+".daily.bwr.eps");
				File twrFile = new File(fname+".daily.twr.eps");
				if (ii == 0 || !bwrFile.exists() || !twrFile.exists()) {
					// Process writes to dCache
					DataSet3 dc_wr = new DataSet3(con, "dc_wr_daily", sDate, eDate);
					
					// Process writes to enstore
					DataSet2 en_wr = new DataSet2(con, "en_wr_daily", sDate, eDate);
					
					savePlots(fname+".daily.bwr", new String[] {"Bytes Written","Date","Bytes","dCache","Enstore"}, 
							  dc_wr.getDates(), dc_wr.getVals3(), en_wr.getDates(), en_wr.getVals2(), sDate, eDate);      log("Save "+fname+".bwr.eps");
					
					savePlots(fname+".daily.twr", new String[] {"Write Transfers","Date","Transfers","dCache","Enstore"}, 
							  dc_wr.getDates(), dc_wr.getVals1(), en_wr.getDates(), en_wr.getVals1(), sDate, eDate);      log("Save "+fname+".twr.eps");
					
				}
				
				File hitFile = new File(fname+".daily.hit.eps");
				if (ii == 0 || !hitFile.exists()) {
					// Process cache hits
					DataSet3 hitsDaily = new DataSet3(con, "hits_daily", sDate, eDate);
					
					savePlots(fname+".daily.hit", new String[] {"Cache Hits","Date","# Hits","Cached","Not Cached"}, 
							  hitsDaily.getDates(), hitsDaily.getVals3(), hitsDaily.getDates(), hitsDaily.getVals2(), sDate, eDate); log("Save "+fname+".hit.eps");
				}

				File cstFile = new File(fname+".daily.cst.eps");
				if (ii == 0 || !cstFile.exists()) {
					// Process transaction cost
					DataSet3 costDaily = new DataSet3(con, "cost_daily", sDate, eDate);
					
					savePlots(fname+".daily.cst", new String[] {"Transaction Cost","Date","Cost","Cost"," "}, 
							  costDaily.getDates(), costDaily.getVals2(),                 null, costDaily.getVals3(), sDate, eDate); log("Save "+fname+".cst.eps");
				}

				sCal.add(Calendar.MONTH, -1);
				eCal.add(Calendar.MONTH, -1);
			}
		}
		catch (SQLException x) { 
			log("Unable to update plot data", x); 
		}
		finally	{
			try	{
				if (con != null) con.close();
			}
			catch (SQLException x) {}
		}
	}
   
    private synchronized void savePlots(final String filename, 
										final String[] titles, 
										final Timestamp[] x1, final double[] y1,
										final Timestamp[] x2, final double[] y2,
										final Date sDate, final Date eDate)
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
		String gtitle = titles[0];
		String xlabel = titles[1];
		String ylabel = titles[2];
		String title1 = titles[3];
		String title2 = titles[4];
		
		try {
			Process pGnuplot = Runtime.getRuntime().exec("./gnuplot -");
            
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(pGnuplot.getInputStream()));
			
			BufferedReader stdError = new BufferedReader(new InputStreamReader(pGnuplot.getErrorStream()));
			
			PrintWriter stdOutput = new PrintWriter(new BufferedWriter(new OutputStreamWriter(pGnuplot.getOutputStream())));
			
			// Initial settings for gnuplot
			log("start gnuplot for "+filename);
			//stdOutput.println("set size 2,2");
			//stdOutput.println("set terminal postscript eps color solid 'Arial' 32");
			stdOutput.println("set size 1.5,1.5");
			stdOutput.println("set terminal postscript eps color solid 'Arial' 24");
			stdOutput.println("set xlabel '"+xlabel+"'");
			stdOutput.println("set timefmt '%Y-%m-%d'");
			stdOutput.println("set xdata time");
			stdOutput.println("set xrange ['"+(sDate.getYear()+1900)+"-"+(sDate.getMonth()+1)+"-"+sDate.getDate()+"':'"+(eDate.getYear()+1900)+"-"+(eDate.getMonth()+1)+"-"+eDate.getDate()+"']");
			log("set xrange ['"+(sDate.getYear()+1900)+"-"+(sDate.getMonth()+1)+"-"+sDate.getDate()+"':'"+(eDate.getYear()+1900)+"-"+(eDate.getMonth()+1)+"-"+eDate.getDate()+"']");
			stdOutput.println("set grid");
			stdOutput.println("set yrange [ : ]");
			stdOutput.println("set format x '%m/%d'");
			
			stdOutput.println("set output '"+filename+".eps'");
			
			Calendar now = Calendar.getInstance();
			stdOutput.println("set title '"+gtitle+" (Plotted: "+now.getTime()+")'");
			//// stdOutput.println("set key right top Right samplen 1 title 'Total Bytes : 4.33e+13\nMean Xfer Size : 3.80e+08\n Number of Xfers : 114174L'");
			stdOutput.println("set ylabel '"+ylabel+"'");
			
			int lw = ((eDate.getTime()-sDate.getTime())/1000/3600/24 > 32) ? 5 : 50;
			
			Timestamp ix; double iy, ie; long nEnt = 0; double totl = 0.0;
			
			if (x2 != null) {
				//stdOutput.println("plot '-' using 1:2 t 'dCache'  with boxes 1, '-' using 1:(-$2) t 'Enstore'  with boxes 3");
				stdOutput.println("plot '-' using 1:2 t '"+title1+"'  with imp lw "+lw+" 1, '-' using 1:(-$2) t '"+title2+"'  with imp lw "+lw+" 3");
				for (int ib = 0; ib < x1.length; ib++) {
					ix = x1[ib];    iy = y1[ib];
					nEnt++;    totl += iy;
					stdOutput.println((ix.getYear()+1900)+"-"+(ix.getMonth()+1)+"-"+ix.getDate()+" "+iy);
					log("X1 "+ix+": "+iy);
				}
				stdOutput.println("e");
				log("e");
			
				for (int ib = 0; ib < x2.length; ib++) {
					ix = x2[ib];    iy = y2[ib];
					nEnt++;    totl += iy;
					stdOutput.println((ix.getYear()+1900)+"-"+(ix.getMonth()+1)+"-"+ix.getDate()+" "+iy);
					log("X2 "+ix+": "+iy);
				}
			} else {
				// stdOutput.println("plot '-' using 1:2:3 t '"+title1+"' with error lw "+lw+" 1");
				// stdOutput.println("plot '-' using 1:2:3 t '"+title1+"' with error lw 2 1");
				stdOutput.println("plot '-' using 1:2 t '"+title1+"' with linespoints lw 2 1");
				for (int ib = 0; ib < x1.length; ib++) {
					ix = x1[ib];    iy = y1[ib];    ie = y2[ib];
					nEnt++;    totl += iy;
					stdOutput.println((ix.getYear()+1900)+"-"+(ix.getMonth()+1)+"-"+ix.getDate()+" "+iy+ " "+ie);
					log("X "+ix+": "+iy+"+/-"+ie);
				}
			}
			stdOutput.println("e");
			log("e");
			stdOutput.println("quit");
			
			log("# Entries="+nEnt+" Total="+totl);
			
			stdOutput.flush();
			stdOutput.flush();
			stdOutput.flush();
			stdOutput.close();
			try {
				pGnuplot.waitFor();
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

    private DataSource dataSource;
    private Thread updateThread;

    private String imageDir;
    private String realPath;
}

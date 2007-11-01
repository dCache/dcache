<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<%@ page import="java.util.*,java.io.File,gov.fnal.isd.*" %>
<html>
  <head>
    <title>Directory List</title>
  </head>

  <body>
    <h1>dCache Plots</h1>
    <h4><a href="/dcache/lsplots">Standard View.</a> Expert View.</h4>
    <%
        //out.println("<h4>Hello from first JSP</h4>");
      
	String srvurl = null;
	try {
	    srvurl = getServletContext().getInitParameter("srvurl");
	} catch (Exception e) {
	    e.printStackTrace();
	}
	
	final String[] months = {"", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
	
	// Prepare month number as a two digit string
	Calendar rightNow = Calendar.getInstance();
	int yyyy = rightNow.get(Calendar.YEAR);
	int yy = yyyy - 1900;
	int mm   = rightNow.get(Calendar.MONTH)+1;
	String smm = "";
	if (mm < 10) 
	    smm = "0";
	smm += mm;

	// Declare the filename filter
	ExampleFileFilter filter = new ExampleFileFilter("pre", "Image Previews");

	// Get the image file list
	String realPath = getServletContext().getRealPath("/");
	File dir = new File(realPath+"Images");
	Viewer0 v = new Viewer0();
	// String[] flist = dir.list();
	// File[] flist = dir.listFiles();
	File[] flist = dir.listFiles(filter);

	// Process file list
	if (flist.length > 0) {
    %>
      <table>
	<%
	    Arrays.sort(flist);
	    String comment = "";

	    for (int i = 0; i < flist.length; i++) {
  	        File ifile = flist[i];
		    if (ifile.isDirectory())               // Skip directories
		        continue;
	        String preName = ifile.getName();
	        if (preName.indexOf(".daily.") > 0)    // Skip all daily plots
			    continue;
	        if (preName.indexOf(".week.") > 0)    // Skip all week plots
			    continue;
			// Make the name for normal plot from preview name
	        String pngName = preName.substring(0, preName.lastIndexOf(".pre"))+".png";

			// Make the name for current month preview plot from preview name
	        String dpreName = preName.replaceFirst("[.]","-"+yyyy+"."+smm+".daily.");

			// Make the name for current month normal plot from current month preview name
	        String dpngName = dpreName.substring(0, dpreName.lastIndexOf(".pre"))+".png";

			// Make the name for current week preview plot from preview name
	        String wpreName = preName.replaceFirst("[.]",".week.");

			// Make the name for current week normal plot from current month preview name
	        String wpngName = wpreName.substring(0, wpreName.lastIndexOf(".pre"))+".png";

			// Split preview name into pieces
   	        String[] items = preName.split("[-.]");
	
			// Select the right comment
	        if      (items[1].startsWith("brd"))
	            comment = "Bytes read";
	        else if (items[1].startsWith("bwr"))
	            comment = "Bytes written";
	        else if (items[1].startsWith("trd"))
	            comment = "Read transfers";
	        else if (items[1].startsWith("twr"))
	            comment = "Write transfers";
	        else if (items[1].startsWith("hit"))
	            comment = "Cache hits";
	        else if (items[1].startsWith("cst"))
	            comment = "Transaction cost";
	        else if (items[1].startsWith("tim"))
	            comment = "Connection Time";
  	        else
	            comment = items[2];
	%>
	<tr>
	  <!-- <td>Year: <%= items[1] %></td> -->
	  <!-- <td><%= ifile.length() %></td> -->
	  <td><%= comment %></td>
	  <!-- <td><img src="/dcache/Viewer?filename=<%= ifile %>" width=120 height=84 ></td> -->
	  <!-- Show plot for the whole year -->
	  <td>
	    <a href="/dcache/outplot?lvl=1&filename=<%= pngName %>">
	      <img src="/dcache/Viewer?filename=<%= preName %>"  width=120 height=84 >
	    </a>
	  </td>
	  <!-- Show plot for the current month -->
	  <td>&nbsp;&nbsp;&nbsp;Current month:(<%= months[mm]+" "+yyyy %>)</td>
	  <td>
	    <a href="/dcache/outplot?lvl=1&filename=<%= dpngName %>">
	      <img src="/dcache/Viewer?filename=<%= dpreName %>"  width=120 height=84 >
	    </a>
	  </td>
	  <!-- Show plot for the current week -->
	  <td>&nbsp;&nbsp;&nbsp;Current week</td>
	  <td>
	    <a href="/dcache/outplot?lvl=1&filename=<%= wpngName %>">
	      <img src="/dcache/Viewer?filename=<%= wpreName %>"  width=120 height=84 >
	    </a>
	  </td>
	</tr>
	<%
	    } // for
	%>
      </table>
	<%
	} // if (flist.length > 0) 
      
    %>
    <!--
    <hr>
    Here you can see: <a href="http://<%=srvurl%>/dcache/dcache_plots_cgi.html">The Old Version Of Plots</a>
    -->
    <hr>
    <address><a href="mailto:podstvkv@fnal.gov">Comments?</a></address>
<!-- Created: Thu Oct 31 17:31:10 CST 2002 -->
<!-- hhmts start -->
Last modified: Thu Sep 11 16:20:58 CDT 2003
<!-- hhmts end -->
  </body>
</html>


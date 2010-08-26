<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<%@ page import="java.util.*" %>
<html>
  <head>
    <title>Output plot</title>
    <%
	// this prevents caching in NS and IE
	response.setHeader("Pragma" , "No-cache") ;
	response.setHeader("Cache-Control", "no-cache") ;
	response.setDateHeader("Expires", 0);  
    %>
  </head>

<!--  <body background="/dcache/bgcolor.jpg"> -->
<body background="/dcache/bgcolor2.jpg">
    <h1>Output plot</h1>
    <%
    String mainJsp = null;
    String level = request.getParameter("lvl");
    if (level == null) {
        level = "0";
    }
    if      (level.startsWith("1")) {
        mainJsp = "/dcache/explots";
    } else {
        mainJsp = "/dcache/lsplots";
    }

    String filename = request.getParameter("filename");
    if (filename == null) {
	throw new ServletException("Parameter 'filename' must be supplied");
    }
    String pltFmt = request.getParameter("fmt");
    String newFmt = null;
    if (pltFmt == null) pltFmt = "lin";
    newFmt = pltFmt.startsWith("lin") ? "log" : "lin"; 
    String dayOfMonth = request.getParameter("day");
    String psname = filename.replaceFirst(".png",".eps");
    String ratio = request.getParameter("ratio");
    if (ratio == null) ratio = "0";

    out.println("<h4>PLot "+filename+"</h4>");
    boolean monthly = false;
    boolean daily = false;
    boolean hourly = false;
    int span = 12;
    if (filename.indexOf(".daily.") > 0) {
        daily = true;
        span = 31;
    }
    if (filename.indexOf(".week.") <= 0) {
        monthly = true;
        span = 31;
    }
    String[] fnFlds = filename.split("[-.]");
    String YYYY = fnFlds[1];

    Calendar rightNow = Calendar.getInstance();
    int cyyyy  = rightNow.get(Calendar.YEAR)-1;
    int cmm  = rightNow.get(Calendar.MONTH)+1;

    %>
    <table>
      <tr>
	  <%
	    if (dayOfMonth==null) {
	        hourly = false;
	  %>
  	  <!-- <td><a href="/dcache/outplot?lvl=<%= level %>&filename=<%= filename %>&fmt=<%= newFmt %>">Log/Lin</a></td> -->
  	  <td></td>
	  <td colspan="<%= span %>" >
	  <img src="/dcache/Viewer?filename=<%= filename %>&fmt=<%= pltFmt %>" width="720" height="504" >
	  <% 
	    } else {
	        hourly = true;
	  %>
  	  <td><a href="/dcache/outplot?lvl=<%= level %>&filename=<%= filename %>&day=<%= dayOfMonth %>&fmt=<%= newFmt %>">Log/Lin</a></td>
	  <td colspan="<%= span %>" >
	  <% 
	     if (ratio.startsWith("0") && fnFlds[4].startsWith("brd")) {
	  %>
	  <a href="/dcache/outplot?lvl=<%= level %>&filename=<%= filename %>&day=<%= dayOfMonth %>&fmt=<%= pltFmt %>&ratio=1" title="Show Transfer_size/Full_size Ratio Plot">
	  <img src="/dcache/GBilling?filename=<%= filename %>&day=<%= dayOfMonth %>&fmt=<%= pltFmt %>&ratio=<%= ratio %>" width="720" height="504" >
	  </a>
	  <%
	     } else if (ratio.startsWith("0") && fnFlds[4].startsWith("cst")) {
	  %>
	  <a href="/dcache/outplot?lvl=<%= level %>&filename=<%= filename %>&day=<%= dayOfMonth %>&fmt=<%= pltFmt %>&ratio=1" title="Show Cost Distribution Plot">
	  <img src="/dcache/GBilling?filename=<%= filename %>&day=<%= dayOfMonth %>&fmt=<%= pltFmt %>&ratio=<%= ratio %>" width="720" height="504" >
	  </a>
	  <%
  	     } else {
	  %>
	  <img src="/dcache/GBilling?filename=<%= filename %>&day=<%= dayOfMonth %>&fmt=<%= pltFmt %>&ratio=<%= ratio %>" width="720" height="504" >
	  <%
  	     } 
	  %>
	  <%
	    }
	  %>
	</td>
      </tr>
      <tr>
	  <%
	  final String[] months = {"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};
	  if (daily) {
	     // We need days table
	     //String fnMonthly = fnFlds[0]+"-"+fnFlds[1]+"."+fnFlds[4]+"."+fnFlds[5];
	     String fnMonthly = fnFlds[0]+"."+fnFlds[4]+"."+fnFlds[5];
	     if (hourly) {
 	     %>
	       <td><a href="/dcache/outplot?lvl=<%= level %>&filename=<%= filename %>&fmt=<%= pltFmt %>"><%= months[Integer.parseInt(fnFlds[2])-1]+" "+YYYY %></a></td>
	     <%
	     } else {
 	     %>
	       <!-- <td><a href="/dcache/outplot?lvl=<%= level %>&filename=<%= fnMonthly %>&fmt=<%= pltFmt %>"><%= YYYY %></a></td> -->
	       <td><a href="/dcache/outplot?lvl=<%= level %>&filename=<%= fnMonthly %>&fmt=<%= pltFmt %>">Up</a></td>
	     <%
	     }
	     for (int i = 1; i <= 31; i++) {
	      %>
	      <td><a href="/dcache/outplot?lvl=<%= level %>&filename=<%= filename %>&day=<%= i %>&fmt=<%= pltFmt %>"><%= i %></a></td>
	      <%
	     }
	  } else if (monthly) {
	     // We need months table
	     %>
	       <!-- <td><a href="<%= mainJsp %>">All:<%= YYYY %></a></td> -->
	       <td><a href="<%= mainJsp %>">Up</a></td>
	     <%
	     int ix = filename.indexOf(".");
	     String s0 = filename.substring(0, ix);
	     String s1 = filename.substring(ix);
	     for (int i = 0; i < 12; i++) {
	         String smm = "";
	         if (cmm < 9) smm += "0";
	         smm += (cmm+1);
  	         String fname = s0 + "-" + cyyyy + "." + smm + ".daily" + s1;
	      %>
	      <td><a href="/dcache/outplot?lvl=<%= level %>&filename=<%= fname %>&fmt=<%= pltFmt %>"><%= months[cmm]%></a></td>
	      <%
		 if (++cmm == 12) {
		     cmm = 0;  cyyyy++;
		 }
	     }
	  } else {
	     // We need months table
	     %>
	       <td><a href="<%= mainJsp %>">Up</a></td>
	     <%
	  }
	  %>
      </tr>
      <%
	if (!hourly) {
      %>
      <tr>
	<td><a href="/dcache/Viewer?filename=<%= psname %>">Postscript</a></td>
      </tr>
      <%
	}
      %>
    </table>

    <hr>
    <address><a href="mailto:podstvkv@fnal.gov">Comments?</a></address>
<!-- Created: Tue Nov 12 11:18:00 CST 2002 -->
<!-- hhmts start -->
Last modified: Mon Nov  3 10:09:01 CST 2003
<!-- hhmts end -->
  </body>
  <%
	// this helps prevent caching in IE
	response.setHeader("Pragma" , "No-cache") ;
	response.setHeader("Cache-Control", "no-cache") ;
	response.setDateHeader("Expires", 0);  
  %>
</html>

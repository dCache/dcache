<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<%@ page import="java.util.*,java.io.*" %>
<html>
  <head>
    <title>DC_CHECK EXECUTION TIME PLOTS</title>
  </head>

  <body background="/dc_check_plots/bg.svg" link=red vlink=red alink=red>
    <% 

       String thisPath;
       if(request instanceof javax.servlet.http.HttpServletRequest)
       {
          javax.servlet.http.HttpServletRequest httprequest=(javax.servlet.http.HttpServletRequest) request;
          thisPath =   httprequest.getRequestURL().toString();
       }
       else
       {
         thisPath="/dc_check_plots/lsplots";
       }
       String the_path=request.getParameter("the_path"); 

     %>
      <%-- <h1> thepath = <%=the_path%> </h1>
       <h1> thisPath = <%=thisPath%> </h1> --%>
       
    <% 
       if(the_path != null && !the_path.equals("")) 
       {
         for(int ntp=0;ntp<1;++ntp)
         {
           the_path=application.getRealPath(the_path);
              File date_dir= new File(the_path);
              if(!date_dir.isDirectory())
              {
                continue;
              }
              String date_dir_name =date_dir.getName();
              if(date_dir_name.equals(""))
              {
                continue;
              }
              
              StringTokenizer st = new StringTokenizer(date_dir_name,"-");

              if(!st.hasMoreTokens())
              {
                continue;
              }
              String year = st.nextToken();
              if(!st.hasMoreTokens())
              {
                continue;
              }
              String month= st.nextToken();
              if(!st.hasMoreTokens())
              {
                continue;
              }
              String date = st.nextToken();

         
    %>
              <h3>   <a href="<%=thisPath%>">
                     Back to all dates 
                     </a> </h3>
         <h1> Plots of the dependency of 
             cd_check execution time on the time of the day 
              for <%=month%>/<%=date%>/<%=year%>
             </h1>
          <h2> each plot covers 4 hours period </h2>
      <p>    
    <table border="1" cellpadding="3" cellspacing="5">
          <%
             File url_plots[] = date_dir.listFiles(
               new FilenameFilter()
               {
                 public boolean accept(File dir,
                      String name)
                 {
                    return name.indexOf("url") != -1;
                 }

               }
              );
              Arrays.sort(url_plots);
             for ( int i=(url_plots.length-1) ; i>=0 ;--i )
             {
                String image_file_name = url_plots[i].getName();
              StringTokenizer st1 = new StringTokenizer(image_file_name,".");

              if(!st1.hasMoreTokens())
              {
                continue;
              }
              String hour  = st1.nextToken();
              if(!st1.hasMoreTokens())
              {
                continue;
              }
              String minute= st1.nextToken();
              if(!st1.hasMoreTokens())
              {
                continue;
              }
              String second = st1.nextToken();
              String bgcolor="#efefef";
              if(i%2 == 0)
              {
                 bgcolor="#bebebe";
              }
         
    %>
     <tr  border=1 cellpadding=4 cellspacing=0 width="90%"> 
       <td bgcolor="<%=bgcolor%>" align=center>&nbsp;ending at <b><%=hour%>:<%=minute%>:<%=second%></b>&nbsp;</td>
       <td bgcolor="<%=bgcolor%>" align=center>&nbsp;<a href="images/<%=date_dir_name%>/<%=image_file_name%>">dc_check with url syntax<a>&nbsp;</td>
       <td bgcolor="<%=bgcolor%>" align=center>&nbsp;<a href="images/<%=date_dir_name%>/<%=image_file_name.replaceFirst("url","pnfs")%>">dc_check with pnfs syntax <a>&nbsp;</td>
     </tr>
    <% 
             }//for(int i=0;i<url_plots.length;++i)
     %>
     </table>
       <p>
       <p> 
              <h3>   <a href="<%=thisPath%>">
                     Back to all dates 
                     </a> </h3>
     <%
         } //for(int ntp=0;ntp<1;++ntp)
       }
       else
       {
         String images_path = application.getRealPath("images");
  %>
     <h1>Plots of the dependency of  
     cd_check execution time on the time of the day</h1>
     <h2> by dates: <h2>
<% 
          try
          {
            File images_dir = new File(images_path);
            File[] date_dirs=new File[0];
            if(images_dir.isDirectory())
            {
               date_dirs=images_dir.listFiles();
               
            }
            Arrays.sort(date_dirs);
  %>
    <table border="1" cellpadding="3" cellspacing="5">
 <%
            for ( int i = (date_dirs.length -1) ; i>=0 ; --i )
            {
              File date_dir = date_dirs[i];
              String date_dir_name =date_dir.getName();
              if(date_dir_name.equals(""))
              {
                continue;
              }
              
              StringTokenizer st = new StringTokenizer(date_dir_name,"-");
              if(!st.hasMoreTokens())
              {
                continue;
              }
              String year = st.nextToken();
              if(!st.hasMoreTokens())
              {
                continue;
              }
              String month= st.nextToken();
              if(!st.hasMoreTokens())
              {
                continue;
              }
              String date = st.nextToken();

              String bgcolor="#efefef";
              if(i%2 == 0)
              {
                 bgcolor="#bebebe";
              }

              
   %>
     
          <tr   border=1 cellpadding=4 cellspacing=0 width="90%">
             <td bgcolor="<%=bgcolor%>" align=center>
               <h3> <a href="<%=thisPath%>?the_path=images/<%=date_dir_name%>"> 
                     dc_check execution time plots for 
                     <%=month%>/<%=date%>/<%=year%> </a> </h3>
             </td>
           </tr>
       


    <% 
            }
     %>
      </table>
     <%
          }
          catch(Exception e)
          {
            PrintWriter pw = new PrintWriter(response.getWriter());
            pw.println("<h1> exception while reading the directory info </h1>");
            pw.println("<pre>");
            e.printStackTrace(pw);
            pw.println("</pre>");
          }
     %>
     <%
       }
     %>

  </body>
</html>

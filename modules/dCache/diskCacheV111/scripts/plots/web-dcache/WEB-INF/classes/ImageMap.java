import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.awt.*;
import javax.imageio.*;
/** Simple servlet used to generate image map for the plot
 *  <P>
 */

public class ImageMap extends HttpServlet {
    public void doGet(HttpServletRequest request, HttpServletResponse response)
	throws ServletException, IOException
    {
	String filename = request.getParameter("filename");

	if (filename == null) {
	    throw new ServletException("Parameter 'filename' must be supplied");
	}
	int width;
	int height;
	File file = new File(filename);
	if (file.exists()) {
	    Image image = null;
	    try {
		// Read from a file
		image = ImageIO.read(file);
		width = image.getWidth(null);
		height = image.getHeight(null);

	    } catch (IOException e) {
		throw new ServletException("Can't read "+file.getAbsolutePath());
	    }
	} else {
	    throw new FileNotFoundException(file.getAbsolutePath());
	}

 	response.setContentType("text/html");
	PrintWriter out = response.getWriter();

 	//out.println("width="+width+" height="+height+"\n");

 	out.println("<map name=\"sample\">\n" +
		    "<area shape=\"rect\" alt=\"Y Log/Linear\" title=\"Y Log/Linear\" coords=\"" +
		           0 +","+ 0 +","+ width/10 +","+ height +
		    "\" href=\"hotspot1.html\">\n" +

 		    "<area shape=\"rect\" alt=\"Pan Left\" title=\"Pan Left\" coords=\"" +
		    width/10 +","+ (height/10)*9 +","+ (width/10)*4 +","+ height +
		    "\"   href=\"hotspot2.html\">\n" +

		    "<area shape=\"rect\" alt=\"Zoom\" title=\"Zoom\" coords=\"" +
		    (width/10)*4 +","+ (height/10)*9 +","+ (width/10)*7 +","+ height +
		    "\"   href=\"hotspot3.html\">\n" +

		    "<area shape=\"rect\" alt=\"Pan Right\" title=\"Pan Right\" coords=\"" +
		    (width/10)*7 +","+ (height/10)*9 +","+ width +","+ height +
		    "\"   href=\"hotspot4.html\">\n" +
		    "<area shape=\"default\" nohref>\n" +
		    "</map>");
	return;
    }
}

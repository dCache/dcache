package gov.fnal.isd;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;

/**
 * Utility class used for servlet related operations
 */
public class Viewer0 extends HttpServlet {

    public Viewer0() {
	super();
    }

    public void init() throws ServletException {
	return;
    }

    public void service(HttpServletRequest request,
			HttpServletResponse response)
	throws ServletException, IOException, FileNotFoundException {

	String imageDir = null;
	try {
	    imageDir = getServletContext().getInitParameter("image.home");
	} catch (Exception e) {
	    e.printStackTrace();
	}

	HttpSession session = request.getSession();
	String filename = request.getParameter("filename");
	if (filename == null) {
	    throw new ServletException("Parameter 'filename' must be supplied");
	}
	String realPath = getServletContext().getRealPath("/");

	String day = request.getParameter("day");
	if (day != null) {
	    String fmt = request.getParameter("fmt");
	    if (fmt.startsWith("log")) {
		String query = request.getQueryString();
		getServletContext().getRequestDispatcher("/servlet/GBilling?"+query).forward(request, response);
	    }
	}
	filename = realPath + imageDir + "/" + filename;
	File file = new File(filename);
	if (file.exists()) {
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
	    bos.flush();
	    bis.close();
	    bos.close();
	} else {
	    String query = request.getQueryString();
	    getServletContext().getRequestDispatcher("/servlet/GBilling?"+query).forward(request, response);
	    //throw new FileNotFoundException(file.getAbsolutePath());
	}
	return;
    }
}

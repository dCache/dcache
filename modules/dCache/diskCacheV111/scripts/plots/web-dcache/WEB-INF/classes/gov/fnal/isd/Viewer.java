/*
 * Viewer.java
 */
package gov.fnal.isd;

import java.awt.*;
import java.awt.event.*;
import java.io.File;

public class Viewer extends Frame {
    private Image image;

    public Viewer(String fileName) {
	/*
	ExampleFileFilter filter = new ExampleFileFilter(new String[] {"gif", "jpg", "pre"}, "JPEG & GIF Images");
	File dir = new File(fileName);
	//String[] flist = dir.list();
	File[] flist = dir.listFiles(filter);
	int i;
	for (i = 0; i < flist.length; i++) {
	    File fi = flist[i];
	    if (fi.isDirectory())
		continue;
	    System.out.println("Found picture file: "+fi+" length="+fi.length());
	}
	fileName = flist[i-1].getName();
	*/
	Toolkit toolkit = Toolkit.getDefaultToolkit();
	image = toolkit.getImage(fileName);
	MediaTracker mediaTracker = new MediaTracker(this);
	mediaTracker.addImage(image, 0);
	try
	    {
		mediaTracker.waitForID(0);
	    }
	catch (InterruptedException ie)
	    {
		System.err.println(ie);
		System.exit(1);
	    }
	addWindowListener(new WindowAdapter() {
      		public void windowClosing(WindowEvent e) {
		    System.exit(0);
      		}
	    });
	setSize(image.getWidth(null), image.getHeight(null));
	setTitle(fileName);
	show();
    }

    public void paint(Graphics graphics) {
	graphics.drawImage(image, 0, 0, null);
    }

    public static void main(String[] args) {
	new Viewer(args[0]);
    }
}

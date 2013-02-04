// $Id: TreeFrame.java,v 1.1 2002-04-03 15:00:51 cvs Exp $

package dmg.cells.services.gui.realm ;

import javax.swing.*;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;


public class TreeFrame {

    protected JFrame            frame;

    public TreeFrame( String [] args ) {

	try {
//         UIManager.setLookAndFeel(UIManager.getCrossPlatformLookAndFeelClassName());
	   UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
	} catch (Exception exc) {
	    System.err.println("Error loading L&F: " + exc);
	}


	JMenuBar         menuBar = new JMenuBar();
	JPanel           panel = new JPanel(true);

	frame = new JFrame("TreeFrame");
        
	frame.getContentPane().add("Center", panel);
	frame.setJMenuBar(menuBar);
	frame.setBackground(Color.lightGray);


	/* And show it. */
	panel.setLayout(new BorderLayout());
	panel.add("Center", new JRealm(null) );
	panel.add("South", new JLabel("South"));

	frame.addWindowListener( new WindowAdapter() {
	    @Override
            public void windowClosing(WindowEvent e) {System.exit(0);}}
        );

	frame.pack();
	frame.show();
    }



    static public void main(String args[]) {
	new TreeFrame( args );
    }

}

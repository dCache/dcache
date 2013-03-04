// $Id: LmSetupHelp.java,v 1.2 2001-04-17 07:10:21 cvs Exp $
//
package dmg.cells.services.gui ;

import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.text.Document;

import java.awt.Dimension;
import java.awt.Toolkit;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.StringReader;



/**
 */
public class LmSetupHelp extends JFrame {
    private static final String _theHelpText =
    "<html><head><title>LocationManager help</title></head><body bgcolor=yellow>\n"+
    "<center><h1>The LocationManager Configuration Tool</h1></center>"+
    "<blockquote>\n"+
    "<strong>Description</strong>"+
    "<blockquote>\n"+
    "The LocationManager Tool is intended to simplify the creation of\n"+
    "the LocationManager configuration file. It is able to read existing\n"+
    "files and to store newly created or modified setups.\n"+
    "It supports <em>Domain</em>, <em>Connection</em>, <em>DefaultRoute</em>,\n"+
    "<em>Links</em> and <em>Listen</em>\n"+
    "</blockquote>\n"+
    "<strong>The File Menu</strong>"+
    "<blockquote>"+
    "The file menu allows to open existing *.lm (Open) files which are\n"+
    "interpreted and displayed, or existing drawings can be stored\n"+
    "to disk (Save as). As soon as a file is assigned either through\n"+
    "<em>Save as</em> or <em>Open</em> the <em>Save</em> item stores\n"+
    "the display into the assigned file.\n"+
    "</blockquote>"+
    "<strong>The Drawing Window</strong>"+
    "<blockquote>\n"+
    "The Drawing is manipulated by popup menus only.\n"+
    "<br><br><strong>Background Popup</strong>\n"+
    "<blockquote>"+
    "The background popup is actived by popup clicking the background.\n"+
    "Beside color management, the background popup menu creates new\n"+
    "Domain entries. The name of the new Domain is requested from the\n"+
    "user by a dialog box. The new Domain is presented by a squared\n"+
    "box. This box follows the mouse motion if its selected first.\n"+
    "</blockquote>"+
    "<br><strong>The Domain Box Popup</strong>\n"+
    "<blockquote>"+
    "The selection menu poping up above a Domain Box allows to\n"+
    "<ul><li>create a link to another domain box<li>removes an existing\n"+
    "link<li>deletes the box with all its links<li>declares the Domain\n"+
    "as a listener</ul>\n"+
    "</blockquote>"+
    "<br><strong>The Connection Line Popup</strong>\n"+
    "<blockquote>"+
    "The connection line popup is actived if the link line\n"+
    "is clicked. The menu allow to <ul><li>delete the link<li>defines\n"+
    "the link to be the default route of the nearest Domain<li>\n"+
    "deletes the default route</ul>\n"+
    "</blockquote>"+
    "</blockquote>"+
    "<strong>Supported Config commands</strong>"+
    "<blockquote>\n"+
    "<pre>\n"+
    "define &lt;DomainName&gt;\n"+
    "connect &lt;sourceDomainName&gt; &lt;destinationDomainName&gt;\n"+
    "defaultroute &lt;sourceDomainName&gt; &lt;destinationDomainName&gt;\n"+
    "listen &lt;DomainName&gt;\n"+
    "# graphical commands\n"+
    "# move &lt;DomainName&gt; &lt;newRelativePos X&gt; &lt;newRelativePos Y&gt;\n"+
    "# color &lt;DomainName&gt;|* background|text|link|item &lt;r g b&gt;\n"+
    "</pre>\n"+
    "</blockquote>"+
    "<strong>Missing Features</strong>"+
    "<ul>\n"+
    "<li>Overwriting existing files or exiting the application with storing\n"+
    "the current setup is not check or ack. by the user.\n"+
    "<li>The tool needs to talk to the LocationManager directly</ul>\n"+
    "<strong>Known Bugs</strong>"+
    "<ul>\n"+
    "<li>The <em>Default Route Dots</em> are not calculated carefully. They sometimes\n"+
    "disappear behind the Domain Boxes.\n"+
    "<li>The font size is not stored in the setup file\n"+
    "</ul>\n"+
    "</blockquote>"+
    "<hr><br><blockquote>\n"+
    "<address>Author : patrick.fuhrmann@desy.de , Last Updated : $Date: 2001-04-17 07:10:21 $</address>\n"+
    "</blockquote><br><br></body></html>" ;
    private static final long serialVersionUID = 393022461389494206L;

    public LmSetupHelp( String title ){
       super(title);
       JEditorPane htmlDoc = new JEditorPane() ;
       htmlDoc.setContentType( "text/html" ) ;
       Document doc = htmlDoc.getEditorKit().createDefaultDocument() ;
       htmlDoc.setDocument(doc);
       htmlDoc.setEditable(false);
       StringReader sr = new StringReader(_theHelpText);
       try{
          htmlDoc.read( sr , doc ) ;
       }catch(Exception ee ){

       }
       getContentPane().add( new JScrollPane(htmlDoc));
    }
    public static void main(String argv[]) {
        WindowListener l = new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {System.exit(0);}
        };
        LmSetupHelp f = new LmSetupHelp("LocationManager Help Tool");
        f.addWindowListener(l);

        f.pack();
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        int w = 200;
        int h = 200;
        f.setLocation(100,100);
        f.setSize(600,400);
        f.setVisible(true);
    }
}

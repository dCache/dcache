// $Id: JLogin.java,v 1.6 2004-04-26 05:42:28 patrick Exp $
//
package dmg.cells.services.gui ;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.Timer;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.GridBagLayout;
import java.awt.Toolkit;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.util.StringTokenizer;

import dmg.cells.applets.login.DomainConnection;
import dmg.cells.applets.login.DomainEventListener;
import dmg.cells.services.gui.realm.JRealm;

import org.dcache.util.Args;

//



/**
 */
public class JLogin extends JFrame {

    private static final long serialVersionUID = 1103652404120194380L;
    private Timer _timer;
    private JSshLoginPanel   _login  = new JSshLoginPanel() ;
    private DomainConnection _domain = _login.getDomainConnection() ;
    private JTabbedPane      _tab    = new JTabbedPane() ;

    public JLogin( String title , String [] argvector ) {

        super( title ) ;
        setBackground(Color.blue);
        final Container c = getContentPane() ;

        Args   args = new Args( argvector ) ;
        String tmp  = args.getOpt("host") ;

        _login.setHostname(tmp==null?"localhost":tmp) ;
        tmp = args.getOpt("port") ;
        _login.setPortnumber(tmp==null?"24223":tmp);

        Class  [] classArgs  = { DomainConnection.class } ;
        Object [] objectArgs = { _domain } ;

        if( args.argc() > 0 ){
           for( int i = 0 , j = args.argc() ; i < j ; i++ ){
              try{
                  StringTokenizer st = new StringTokenizer( args.argv(i) , "=") ;
                  String name      = st.nextToken() ;
                  String className = st.nextToken() ;
                  Class<? extends JPanel> cn = Class.forName(className).asSubclass(JPanel.class);
                  Constructor<? extends JPanel> cc = cn.getConstructor(classArgs);

                  JPanel cp = cc.newInstance(objectArgs);

                  _tab.addTab( "   "+name+"   " , cp ) ;


              }catch(Exception e){
                  System.err.println("Can't init "+args.argv(i)+" : "+e);
              }
           }
        }else{
           _tab.addTab( "   Commander   " , new JCommander( _domain ) ) ;
           _tab.addTab( "   Spy   " , new JSpyPanel( _domain ) ) ;
           _tab.addTab( "   Realm   " , new JRealm( _domain ) ) ;
        }
        _tab.setSelectedIndex(0);
        c.setLayout( new GridBagLayout() ) ;

        c.add( _login ) ;

        _domain.addDomainEventListener(

           new DomainEventListener(){
              @Override
              public void connectionOpened( DomainConnection connection ){
                 System.out.println("Connection opened");
                 c.removeAll();
                 c.setLayout( new BorderLayout() ) ;
                 c.add( _tab , "Center" ) ;
                 c.invalidate() ;
                 c.doLayout() ;
                 repaint();
              }
              @Override
              public void connectionClosed( DomainConnection connection ){
                 System.out.println("Connection closed" ) ;
                 c.removeAll() ;
                 c.setLayout( new GridBagLayout() ) ;
                 c.add( _login ) ;
                 c.invalidate() ;
                 c.doLayout() ;
                 repaint();
              }
              @Override
              public void connectionOutOfBand( DomainConnection connection, Object obj ){
                 System.out.println("Connection connectionOutOfBand" ) ;
              }
           }
        );

    }
    public static void main(String argv[]) throws Exception  {

       PrintStream devnull = new PrintStream( new FileOutputStream( "/dev/null" ) ) ;
       System.setErr( devnull ) ;
       System.setOut( devnull ) ;

       JLogin f = new JLogin("Cell Login" , argv );

       f.pack();
       Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
       int w = 200;
       int h = 200;
       f.setLocation(100,100);
       f.setSize(600,400);
       f.setVisible(true);
   }
}

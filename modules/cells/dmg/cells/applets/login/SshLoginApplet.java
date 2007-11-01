package dmg.cells.applets.login ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.lang.reflect.* ;
import java.util.* ;
import dmg.util.* ;
import dmg.protocols.ssh.* ;

public class      SshLoginApplet 
       extends    Applet 
       implements ActionListener  {
       
  //
  // remember to change the following variable to your needs 
  // 
  private SshLoginPanel     _loginPanel     = null ;
   
  private String    _remoteHost     = null ;
  private String    _remotePort     = null ;
  private String    _remoteUser     = null ;
  private String    _remotePassword = null ;
  private String    _userPanel      = null ;
  private String    _title          = null ;
  
  private AppletContext    _context        = null ;
  private CardLayout       _cardsLayout    = null ; 
  private CommanderPanel   _commanderPanel = null ;
  private Panel            _switchPanel    = null ;
  private PanelSwitchBoard _switchBoard    = null ;
  
  private Font      _font = new Font( "TimesRoman" , 0 , 16 ) ;
  
  
  private final static Class [] __userPanelArgs = {
       dmg.cells.applets.login.DomainConnection.class 
  } ;
  private final static Class [] __userPanelListener = {
       java.awt.event.ActionListener.class 
  } ;
  
  public void init(){
      System.out.println( "init ..." ) ;
      Dimension   d  = getSize() ;
      _context = getAppletContext() ;
//
//    get all necessay infos ...
//
      _remoteHost = getParameter( "host" ) ;
      _remotePort = getParameter( "port" ) ;
      _remoteUser = getParameter( "user" ) ;
      _title      = getParameter( "title" ) ;
     
      //
      // we allow to set the "panel" :
      //    only this panel is displayed , or
      // we allow to have panel1, panel2 ... 
      //    in this  case the syntax has to be
      //       param name="panel1" value="<name>:<panelClass>"
      //
      _userPanel  = getParameter( "panel" ) ;
      
      _remoteHost = _remoteHost==null?"":_remoteHost ;
      _remotePort = _remotePort==null?"":_remotePort ;
      _remoteUser = _remoteUser==null?"":_remoteUser ;
      _title      = _title==null?"Login":_title;
      
      _loginPanel = new SshLoginPanel() ;
      _loginPanel.setHost( _remoteHost , true , true ) ;
      _loginPanel.setPort( _remotePort , true , true ) ;
      _loginPanel.setUser( _remoteUser , true , true ) ;
      _loginPanel.setTitle( _title ) ;
      
      _loginPanel.addActionListener( this ) ;
   
      setLayout( new CenterLayout() ) ;
      _loginPanel.ok() ;

      _switchPanel = new Panel( _cardsLayout = new CardLayout() ) ;
      _switchPanel.add( _loginPanel  , "login" ) ;
      
      int panelCounter = 0 ;
      if( _userPanel == null  ){
         _switchBoard = new PanelSwitchBoard(true) ;
         for( int i = 0 ; true ; i++ ){
            String tmp = getParameter( "panel"+i ) ;
            if( ( tmp == null ) || ( tmp.equals("") ) )break ;
            StringTokenizer st = new StringTokenizer(tmp,":");
            String name = null ;
            try{
                           name    = st.nextToken() ; 
               Class       pc      = Class.forName( st.nextToken() ) ;
               Constructor pcon    = pc.getConstructor(__userPanelArgs) ;
               Object []   args    = new Object[1] ;
                           args[0] = _loginPanel ; // is the domain connection
               Component   p       = (Component)pcon.newInstance(args) ;
               _switchBoard.add( p , name ) ;
               panelCounter ++ ;
            }catch( Exception e ){
               System.err.println( "Skipping "+name+" ("+e+")") ;
            }
         }
         if( panelCounter == 0 ){
              _userPanel = "dmg.cells.applets.login.CommanderPanel" ;
         }else{
              _switchBoard.addActionListener(this);
              _switchPanel.add( _switchBoard , "commander" ) ;
              _cardsLayout.show( _switchPanel , "commander" ) ;
         }
      
      }
      if( _userPanel != null ){

         try{
            Class       pc      = Class.forName( _userPanel ) ;
            Constructor pcon    = pc.getConstructor(__userPanelArgs) ;
            Object []   args    = new Object[1] ;
                        args[0] = _loginPanel ;
            Component   p       = (Component)pcon.newInstance(args) ;
            Method      m       = pc.getMethod( "addActionListener" ,
                                                __userPanelListener ) ;
            args    = new Object[1] ;
            args[0] = this ; 
            m.invoke( p , args ) ;
            _switchPanel.add( p , "commander" ) ;
         }catch( Exception e ){
            System.out.println( "Problem while new commander instance : "+e ) ;
            System.exit(1) ;
         }
      }
      
      _cardsLayout.show( _switchPanel , "login" ) ;      
      add( _switchPanel ) ;   
  }
  public void start(){
      System.out.println("start ..." ) ;
      validate() ;
      repaint() ;
      //
      // get the parameter from the html file
      //
  }
  public void stop(){
      System.out.println("stop ..." ) ;
  }
  public void destroy(){
      System.out.println("destroy ..." ) ;
//      try{
//      }catch( Exception e ){}
  }      
  public synchronized void actionPerformed( ActionEvent event ){
     String command = event.getActionCommand() ;
     System.out.println( "Action Applet : "+command ) ;
     Object obj = event.getSource() ;
     if( command.equals( "connected" ) ){
         _cardsLayout.show( _switchPanel , "commander" ) ;
     }else if( command.equals( "disconnected" ) ){
         _cardsLayout.show( _switchPanel , "login" ) ;
     }else if( command.equals( "exit" ) ){
         _loginPanel.logout() ;
         _cardsLayout.show( _switchPanel , "login" ) ;
     }
  }
       
}

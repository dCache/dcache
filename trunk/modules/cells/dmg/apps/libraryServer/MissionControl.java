package dmg.apps.libraryServer ;

import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.text.* ;
import dmg.util.* ;

public class      MissionControl 
       extends    Frame 
       implements WindowListener, 
                  ActionListener {
       
  private LSProtocolHandler _control  = null ;
  private ButtonPanel  _buttonPanel   = new ButtonPanel() ;
  private InfoPanel    _infoPanel     = null ;
  private MountPanel   _mountPanel    = null ;
  private MountPanel   _dismountPanel = null ;
  private Component    _currentPanel  = null ;
  private Button       _logButton     = null ;
  private LogPanel     _logPanel      = null ;  
  private Font         _headerFont    = new Font( "SansSerif" , 0 , 18 ) ;
  private Font         _desyFont      = new Font( "SansSerif" , Font.BOLD , 25 ) ;
  private DesyLogo     _desyLogo      = new DesyLogo() ;
  private int          _activeButtons = 0 ;
  private boolean      _silentDismount = true ;
  private class DesyLogo extends Canvas {
     private double base        = 7.0 ;
     private double largeDot    = 1.4 / base ;
     private double mediumDot   = 1.0 / base ;
     private double smallDot    = 0.6 / base ;
     private double smallCircle = 3.9 / base ;
     private double mediumCircle = 6.3 / base ;
     private double largeCircle  = 7.0 / base ;
     private Color  _color = Color.blue ;
     private DesyLogo(){
     
     
     }
     public void setColor( Color color ){ 
        _color = color ; 
        repaint() ;
     }
     public void paint( Graphics g ){
        g.setColor(_color) ;
        Dimension d = getSize() ;
        int cx = d.width  / 2 ;
        int cy = d.height / 2 ;
        int scal = Math.min( d.width -10 , d.height - 10 ) ;
        
//        g.setFont(_desyFont);
//        g.drawString( "DESY" , 100 , 100 ) ;
        int R = (int)(largeCircle / 2. * scal) ;
        int r = (int)(smallCircle / 2. * scal) ;
        int f = (int)(smallCircle / 2. * Math.sqrt(0.5) * scal ) ;
        int q = (int)(mediumCircle / 2. * scal) ;
        int p = (int)(mediumCircle / 2. * Math.sqrt(0.5) * scal) ;
        
        int rsd = (int)( smallDot / 2. * scal ) ;
        int rmd = (int)( mediumDot / 2. * scal ) ;
        int rld = (int)( largeDot / 2. * scal ) ;
//        g.drawOval( cx - R , cy - R   , 2 * R , 2 * R ) ;
//        g.drawOval( cx - r , cy - r   , 2 * r , 2 * r ) ;
        
        g.fillOval( cx + f - rsd , cy - f - rsd , 2 * rsd , 2 * rsd ) ;
        g.fillOval( cx - f - rsd , cy + f - rsd , 2 * rsd , 2 * rsd ) ;
        
        g.fillOval( cx - f - rmd , cy - f - rmd , 2 * rmd , 2 * rmd ) ;
        g.fillOval( cx + f - rmd , cy + f - rmd , 2 * rmd , 2 * rmd ) ;
        
        g.fillOval( cx - rld , cy - r - rld , 2 * rld , 2 * rld ) ;
        g.fillOval( cx - rld , cy + r - rld , 2 * rld , 2 * rld ) ;
        
        g.drawLine( cx - p , cy - p , cx + p , cy + p ) ;
        g.drawLine( cx + p , cy - p , cx - p , cy + p ) ;
        g.drawLine( cx  , cy - q , cx  , cy + q ) ;
     }
  
  }
  private class LogPanel extends MulticastPanel implements ActionListener {
     private String _filename = null ;
     private TextArea  _text  = new TextArea() ;
     private Button    _clearButton = new Button("Clear Screen") ;
     private SimpleDateFormat _formatter
               = new SimpleDateFormat ("MM/dd hh:mm:ss ");
     
     private LogPanel( String filename ){
         BorderLayout bl = new BorderLayout() ;
         bl.setHgap(10);
         bl.setVgap(10);
         setLayout( bl ) ;
         setBorderSize(10);
         setBorderColor( Color.red ) ;
         _filename = filename ;
         
         Panel buttonPanel = new Panel( new FlowLayout(FlowLayout.CENTER) ) ;
         
         buttonPanel.add( _clearButton )  ;
         
         add( _clearButton , "North" ) ;
         add( _text , "Center" ) ;
     }
     public void actionPerformed( ActionEvent event ){
        _text.setText("") ;
     }
     public void say(String text){ 
       String log = _formatter.format(new Date())+text ;
       _text.append(log+"\n") ; 
       
       if( ( _filename == null ) || ( _filename.equals("") ) )return ;
       try{
          PrintWriter pw = new PrintWriter(
                             new OutputStreamWriter(
                               new FileOutputStream( _filename , true ) ) ) ;
                               
          try{
             pw.println( log ) ;
          }catch(Exception eee){}
       
          try{
             pw.close() ;
          }catch(Exception eee){}
       }catch(Exception ee ){
       
       }
       
       
       
     } 
  
  }
  //
  // bug fix for java1.3 on irix 5.3
  //
  private class SectionButton extends Button implements Runnable {
     private class OnButtonAction implements ActionListener {
        public void actionPerformed( ActionEvent event ){
           if( _hsmEvent != null ){
              _processActionEvent( _hsmEvent )   ;      
           }
        } 
     }
     private LSProtocolHandler.Section _section ;
     private Thread         _worker         = null ;
     private ActionListener _actionListener = null ;
     private HsmEvent       _hsmEvent       = null ;
     private long           _time           = 0 ;
     private SectionButton(  LSProtocolHandler.Section section ){
        super( "???" ) ;
        setLabel( section.getName() ) ;
        _section = section ;
        super.addActionListener(new OnButtonAction() ) ;
        _worker  = new Thread(this) ;
        _worker.start() ;
     }
     public void reply( HsmEvent event )throws IOException {
        String reply = event.getCommand() ;
        long now = System.currentTimeMillis() ;
        say( "<-- "+reply+" -time="+((now-_time)/60/1000) );
        _section.setCommandString(reply) ;
        _section.releaseSemaphore() ;
        _worker  = new Thread(this) ;
        _worker.start() ;
     }
     public synchronized void addActionListener( ActionListener actionListener ){
         _actionListener = AWTEventMulticaster.add(
                              _actionListener, actionListener ) ;
     }
     private synchronized void _processActionEvent( ActionEvent e ){
          if( _actionListener != null )
             _actionListener.actionPerformed(e) ;
     }
     private void say(String text ){
        _logPanel.say( "["+_section.getSectionId()+"] ["+_section.getName()+"] "+text ) ;
     }
     private void storeEvent( HsmEvent event ){
        _time     = System.currentTimeMillis() ;
        _hsmEvent = event ;
     }
     public void setEnabled( boolean enabled ){
         setForeground( enabled ? Color.red : Color.black ) ;
         super.setEnabled(enabled);
     }
     public void run(){
        while( ! Thread.interrupted() ){
           try{
              setEnabled(false) ;
//              say( "Waiting for semaphore" ) ;
              _section.getSemaphore() ;
//              say( "Got semaphore" ) ;
              Args args = new Args( _section.getCommandString() ) ;
              say("--> "+args.toString() ) ;
              if( args.argc() < 1 ){
                 //  dummy
                 _section.setCommandString("0 ping-ok") ;
              }else if( ( args.argc() > 3 ) && args.argv(0).equals("200") ){
                 storeEvent( new MountEvent(this,args.argv(2),args.argv(3)) ) ;
                 setEnabled(true) ;
                 _processActionEvent( new ActionEvent( this , 0 , "activated" ) ) ;
                 break ;
              }else if( ( args.argc() > 2 ) && args.argv(0).equals("300") ){
                
                 if( _silentDismount ){
                    reply( new HsmEvent( this , 301 ) ) ;
                 }else{
                    if( args.argc() > 3 ){
                       storeEvent(  new DismountEvent(this,args.argv(2),args.argv(3)) );
                    }else{
                       storeEvent(  new DismountEvent(this,args.argv(2),"Unknown") );
                    }
                    _processActionEvent( new ActionEvent( this , 0 , "activated" ) ) ;
                    setEnabled(true) ;
                 }
                 break ;
              }else if( ( args.argc() > 3 ) && args.argv(0).equals("400") ){
                 String mode   = args.argv(2) ;
                 String detail = args.argv(3) ;
                 if( mode.equals( "panic" ) ){
                    storeEvent(  new InfoEvent(this,args.argv(3)) );
                    setEnabled(true) ;
                    _processActionEvent( new ActionEvent( this , 0 , "activated" ) ) ;
                 }else{  
                    String reply =  "401 say-ok" ;      
                    say( "<-- "+reply);      
                    _section.setCommandString( reply ) ;
                    _section.releaseSemaphore();
                 }
                 break ;
              }else{
                 String reply = "111 command-not-found" ;
                 say( "<-- "+reply);      
                 _section.setCommandString( reply ) ;
              }
              
              _section.releaseSemaphore() ;
           }catch(IOException ioe ){
              say( "!!! Got io Exception : "+ioe ) ;
              break ;
           }catch(InterruptedException ie ){
              say( "!!! Was interrupted : "+ie ) ;
              break ;
           }
        }
     }
  }
  private class ButtonPanel extends SimpleBorderPanel {
      private ButtonPanel(){
         super( new GridLayout(1,0) , 15 , Color.red ) ;
         GridLayout gl = new GridLayout(1,0) ;
         gl.setVgap(10) ;
         gl.setHgap(10) ;
         setLayout(gl);
      }
  
  }
  public MissionControl( Args args ){
      super( "Mission Control" ) ;
      addWindowListener( this ) ;
      setLocation( 60 , 60) ;
      setBackground(new Color(230,240,200));
      
      boolean readWrite = args.getOpt("console") != null ;
      String  logFile   = args.getOpt("log") ;
      if( logFile == null )logFile = "/tmp/MissionControl.log" ;
      
      Panel cbp = new Panel( new GridLayout(0,1) ) ;
      
      _logButton = new Button( "Show Log" ) ;
      _logButton.addActionListener(this) ;
      
      cbp.add( _logButton ) ;
      cbp.add( new Button("Overview") ) ;
      
      _buttonPanel.add( cbp ) ;
      _logPanel = new LogPanel( readWrite ? logFile : null ) ;
       say( "Mission Control started" ) ;
     
      try{
         _control = new LSProtocolHandler( args.argv(0) ,
                                           LSProtocolHandler.SERVER ) ;
         
         int count = _control.getSectionCount() ;
         for( int i = 0 ; i < count ; i++ ){
            Button button = new SectionButton( _control.getSection(i) ) ;
            button.setFont( _headerFont ) ;
            button.addActionListener(this);
            _buttonPanel.add( button ) ;
            _buttonPanel.setBackground( getBackground().darker() ) ;
            
         }
      }catch(Exception ee ){
         ee.printStackTrace() ;
         System.exit(4);
      }
      setLayout( new BorderLayout() ) ;
      add( _buttonPanel , "North" ) ; 
      
      _infoPanel     = new InfoPanel( readWrite ) ;
      _mountPanel    = new MountPanel( readWrite , true);
      _dismountPanel = new MountPanel( readWrite , false) ;
      _currentPanel  = _desyLogo ;

      if( _currentPanel != null )add( _currentPanel , "Center" ) ;        
      _infoPanel.addActionListener(this) ;
      _mountPanel.addActionListener(this);
      _dismountPanel.addActionListener(this);
      
      setSize( 750 , 500 ) ;
      pack() ;
      setSize( 750 , 500 ) ;
      setVisible( true ) ;
      
      
  }
  public void say( String text ){ _logPanel.say(text) ; }
  public synchronized void actionPerformed( ActionEvent event ){
     String command = event.getActionCommand() ;
     Object source  = event.getSource() ;
//     say( "Action : "+event.toString() ) ;
     if( source == _logButton ){
       if( _currentPanel != _logPanel ){
          if( _currentPanel != null )remove( _currentPanel ) ;
          add( _currentPanel = _logPanel , "Center" ) ; 
       }else{
          remove( _currentPanel ) ;
          add( _currentPanel = _desyLogo ) ;
       }
       validate() ;
     }else if(  event instanceof MountEvent ){
       _mountPanel.setEvent( (HsmEvent)event ) ;
       if( _currentPanel != _mountPanel ){
          if( _currentPanel != null )remove( _currentPanel ) ;
          add( _currentPanel = _mountPanel , "Center" ) ; 
       }
       validate() ;
     }else if(  event instanceof DismountEvent ){
       _dismountPanel.setEvent( (HsmEvent)event ) ;
       if( _currentPanel != _dismountPanel ){
          if( _currentPanel != null )remove( _currentPanel ) ;
          add( _currentPanel = _dismountPanel , "Center" ) ; 
       }
       validate() ;
     }else if(  event instanceof InfoEvent ){
       _infoPanel.setEvent( (InfoEvent)event ) ;
       if( _currentPanel != _infoPanel ){
          if( _currentPanel != null )remove( _currentPanel ) ;
          add( _currentPanel = _infoPanel , "Center" ) ; 
       }
       validate() ;
     }else if( source instanceof SectionButton ){
       if( command.equals( "activated" ) ){
         _activeButtons ++ ;
         if( _activeButtons == 1 )_desyLogo.setColor(Color.red);
         return ;
       }
       try{
          SectionButton sectionButton = (SectionButton)source ;
//          say( "<-- "+event ) ;
          sectionButton.reply( (HsmEvent)event ) ;
          sectionButton.setEnabled(false);
          if( _currentPanel != null ){
              remove( _currentPanel ) ;
          }
          _activeButtons-- ;
          if( _activeButtons == 0 )_desyLogo.setColor(Color.blue);
          add( _currentPanel = _desyLogo , "Center" ) ;
          validate() ;
       }catch(IOException ioe ){
          say( "Exception : "+ioe ) ;
       }
     }
  }
  //
  // window interface
  //
  public void windowOpened( WindowEvent event ){}
  public void windowClosed( WindowEvent event ){
      System.exit(0);
  }
  public void windowClosing( WindowEvent event ){
      System.exit(0);
  }
  public void windowActivated( WindowEvent event ){}
  public void windowDeactivated( WindowEvent event ){}
  public void windowIconified( WindowEvent event ){}
  public void windowDeiconified( WindowEvent event ){}
  public static void main( String [] args ){
      if( args.length < 1 ){
         System.err.println( "Usage : ... <controlFile>" ) ;
         System.exit(4) ;
      }
     try{
            
         new MissionControl( new Args( args ) ) ;
      
      }catch( Exception e ){
         e.printStackTrace() ;
         System.exit(4);
      }
      
   }
       
}

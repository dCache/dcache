// $Id: JDCap.java,v 1.1 2001-09-11 04:32:11 cvs Exp $
//
package diskCacheV111.clients.jdcap  ;
//
import java.awt.*;
import java.awt.event.*;
import java.awt.font.*;
import javax.swing.*;
import javax.swing.border.* ;
import java.util.*;
import java.io.* ;

import diskCacheV111.util.* ;
import diskCacheV111.clients.vsp.* ;

public class JDCap extends JFrame {
   private SetupPanel _setupPanel    = null ;
   private Random     _random        = new Random() ;
   private ArrayList  _buttonList    = new ArrayList() ;
   private JPanel     _currentActive = null ;

   private class WindowShutdown extends WindowAdapter {
      public void windowClosing( WindowEvent event ){
          System.exit(0);
      }
   }
   private class ButtonAction implements ActionListener {
      private JPanel _panel = null ;
      private ButtonAction( JPanel panel ){
         _panel = panel ;
      }
      public void actionPerformed( ActionEvent event ){
         if( _currentActive != null )_private.remove(_currentActive) ;
         _private.add( _currentActive = _panel ) ;
         _private.invalidate() ;
         _private.validate() ;
         _private.repaint();
      }
   }
   private class InfoPanel extends JPanel implements Runnable {
      private class InfoBar extends JPanel {
         private JButton _button   = null ;
         private JLabel  _progress = new JLabel("",JLabel.CENTER) ;
         private JProgressBar  _speed    = new JProgressBar() ;
         private JProgressBar _bar = new JProgressBar() ;
         public InfoBar( String title ){
            setLayout( new GridLayout(1,0,4,4));
            _button = new JButton( title ) ;
            _button.addActionListener( new ButtonAction(InfoPanel.this) ) ;
            add(_button) ;
//            add(_progress);
            add(_speed);
            add(_bar) ;
         }
      }
      private String  _title   = null ;
      private Color   _color   = Color.red ;
      private InfoBar _infoBar = null ;
      private Thread  _thread  = null ;
      private JTextField _message = new JTextField();

      private InfoPanel( String title ){
        setLayout( new BorderLayout() ) ;
        _infoBar = new InfoBar(_title=title) ;

        add( new JLabel(_title,JLabel.CENTER) , "North" ) ;
        add( _message , "South" ) ;
      }
      public JPanel getInfoBar(){
        return _infoBar ;
      }
      public void interrupt(){ if( _thread != null )_thread.interrupt() ; }
      public void go(){
         System.out.println( "Starting : "+_title ) ;
         _thread = new Thread(this) ;
         _thread.setPriority(_thread.getPriority()-2);
         _thread.start() ;
      }
      public void run(){
         System.out.println( "Starting : "+_title ) ;
         _infoBar._button.setBackground(Color.white) ;
         try{
            PnfsFile pnfsFile =
                 new PnfsFile( _setupPanel.getFile().getCanonicalPath() ) ;

            _infoBar._speed.setMaximum(_setupPanel.getTransferSpeed());
            _infoBar._speed.setValue(0);


            _infoBar._bar.setMaximum((int)pnfsFile.length());
            _infoBar._bar.setValue(0);

            _infoBar._button.setBackground(Color.yellow);
            VspDevice vsp = new VspDevice( _setupPanel.getHostname() ,
                                           _setupPanel.getPort() ,
                                           _setupPanel.getReplyHost() ) ;
            _message.setText("Waiting for open to be completed");
            vsp.setDebugOutput(false);
            _infoBar._button.setBackground(Color.orange);
            VspConnection c = vsp.open( pnfsFile.getPnfsId().toString() , "r" ) ;
            c.sync() ;
            c.setSynchronous(true);
            _message.setText("Open completed" ) ;
            _infoBar._button.setBackground(Color.green);
            byte [] data = new byte[1*1024] ;
            long size  ;
            long total = 0 ;
            long start = System.currentTimeMillis() ;
            long waitCycles = _setupPanel.getWaitCycles() ;
            try{
               while( ! Thread.currentThread().interrupted() ){
                  if( ( size = c.read( data , 0 , data.length ) ) <= 0 )break ;
                  total += size ;
                  _infoBar._progress.setText(""+total);
                  _infoBar._bar.setValue((int)total);
                  long  diff  = System.currentTimeMillis() - start ;
                  float speed = (float)(((double)total)/((double)diff)*1000.) ;
                  _infoBar._speed.setValue((int)speed);
                  _message.setText( " Total : "+total+"  "+speed) ;
                  try{
                     Thread.currentThread().sleep((_random.nextInt()&0xfffffff)%waitCycles) ;
                  }catch(Exception ee){
                     _message.setText("Interrupted");
                     break ;
                  }
               }
            }catch(Exception ioe ){
               _message.setText("IOException : "+ioe.getMessage() ) ;
            }
            try{ c.close() ; }catch(IOException eee){}
            _infoBar._button.setBackground(Color.magenta);
            vsp.close();
            _infoBar._button.setBackground(Color.white);
           long diff = System.currentTimeMillis() - start ;
            _message.setText( "closed with total : "+total+" and "+
                          (((double)total)/((double)diff)*1000./1024./1024.)+" MB/sec" ) ;
         }catch(Exception iee){
            _message.setText(iee.getMessage());
            _infoBar._button.setBackground(Color.red);
         }
         _setupPanel.substractOne() ;
      }
      public Insets getInsets(){
         return new Insets(10,10,10,10);
      }
   }
   private class SetupPanel extends JPanel implements ActionListener {
       private JPanel _basicPanel = null ;

       private JTextField _filename   = null ;
       private JTextField _serverHost = null ;
       private JTextField _serverPort = null ;
       private JTextField _replyHost  = null ;
       private JTextField _transferSpeed  = null ;
       private JTextField _waitCycles  = null ;

       private File       _file     = null ;
       private int        _active   = 0 ;
       private JButton    _goButton = new JButton("Go") ;
       private JButton    _exitButton = new JButton("Exit") ;
       private JLabel     _message  = new JLabel("");
       public Insets getInsets(){
          System.out.println("Insets requested from SetupPanel");
          return new Insets(10,10,10,10) ;
       }
       public String getHostname(){ return _serverHost.getText() ; }
       public String getReplyHost(){ return _replyHost.getText() ; }
       public int    getPort(){
         try{
            return Integer.parseInt( _serverPort.getText() ) ;
         }catch( Exception e){ return 0 ; }
       }
       public int    getTransferSpeed(){
         try{
            return Integer.parseInt( _transferSpeed.getText() ) ;
         }catch( Exception e){ return 200 ; }
       }
       public int    getWaitCycles(){
         try{
            return Integer.parseInt( _waitCycles.getText() ) ;
         }catch( Exception e){ return 200 ; }
       }
       private class MyLineBorder extends LineBorder {
          public MyLineBorder( Color color , int line ){
            super( color , line ) ;
          }
          public Insets getBorderInsets(Component com){
             System.out.println("BorderInsets requested");
             return new Insets(10,10,10,10);
          }
       }
       private SetupPanel( String [] args ){
          setLayout( new GridBagLayout() ) ;

          add( _basicPanel = new JPanel( new BorderLayout(10,10) ) ) ;

          _basicPanel.add( new JLabel( "JDCap Setup" , JLabel.CENTER ) , "North" ) ;

          _basicPanel.setBorder( new MyLineBorder( Color.red , 2 ) ) ;

          _goButton.addActionListener( this ) ;
          _exitButton.addActionListener(this) ;

          JPanel buttom = new JPanel( new GridLayout(0,2,10,10) ) ;

          _serverHost = new JTextField(30) ;
          if( args.length > 1 )_serverHost.setText(args[1]) ;
          _serverPort = new JTextField(30) ;
          if( args.length > 2 )_serverPort.setText(args[2]);
          _replyHost = new JTextField(30) ;
          if( args.length > 3 )_replyHost.setText(args[3]);
          _filename = new JTextField(30) ;
          if( args.length > 4 )_filename.setText(args[4]);
          _filename.addActionListener( this ) ;

          _transferSpeed = new JTextField("500");
          _waitCycles    = new JTextField("1000");

          buttom.add( new JLabel("Wait Cycles [msec]") ) ;
          buttom.add( _waitCycles ) ;
          buttom.add( new JLabel("Max Transfer Speed [bytes/sec]") ) ;
          buttom.add( _transferSpeed ) ;
          buttom.add( new JLabel("ServerHost") ) ;
          buttom.add( _serverHost ) ;
          buttom.add( new JLabel("ServerPort") ) ;
          buttom.add( _serverPort ) ;
          buttom.add( new JLabel("ReplyHost" ) ) ;
          buttom.add( _replyHost ) ;
          buttom.add( new JLabel("Filename" ) ) ;
          buttom.add( _filename ) ;

          buttom.add( _goButton ) ;
          buttom.add( _exitButton ) ;
          buttom.add( new JLabel("Message") ) ;
          buttom.add( _message ) ;

          _basicPanel.add( buttom , "South" ) ;

       }
       public synchronized void substractOne(){
          _active -- ;
          if( _active <= 0 ){
             _goButton.setText("Go") ;
             _filename.setEnabled(true);
          }
       }
       public synchronized void addOne(){
          _active ++ ;
       }
       public File getFile(){ return _file ; }
       public void actionPerformed( ActionEvent event ){
          if( event.getSource() == _exitButton )System.exit(0);
          _file = new File(_filename.getText()) ;
          if( ! _file.exists() ){
             _message.setText( "File not found : "+_file ) ;
             return ;
          }
          _message.setText("");
          if( event.getSource() == _filename ){
          }else if( event.getSource() == _goButton ){
             if( _active == 0 ){
                _goButton.setText("Cancel");
                _filename.setEnabled(false);
                new Thread(
                 new Runnable(){
                   public void run(){
                      Iterator iter = _buttonList.iterator() ;
                      while( iter.hasNext() ){
                          InfoPanel info = (InfoPanel)iter.next() ;
                          info.go() ;
                          addOne() ;
                          try{
                             Thread.currentThread().sleep(500) ;
                          }catch(InterruptedException ie){}
                      }
                   }
                 }
                ).start() ;
                _tab.setSelectedIndex(1);
             }else{
                new Thread(
                 new Runnable(){
                   public void run(){
                      Iterator iter = _buttonList.iterator() ;
                      while( iter.hasNext() ){
                          InfoPanel info = (InfoPanel)iter.next() ;
                          info.interrupt() ;
                      }
                   }
                 }
                ).start() ;
             }
          }
      }
   }
   private JPanel       _buttons = null ;
   private JScrollPane  _scroll  = null ;
   private JTabbedPane  _tab     = new JTabbedPane() ;
   private JPanel       _private = new JPanel( new BorderLayout() ) ;

   public JDCap( String title , String [] args ){
      super(title);

      addWindowListener( new WindowShutdown() ) ;
      Container c = getContentPane() ;

      _buttons    = new JPanel( new GridLayout( 0,1 ) ) ;
      _scroll     = new JScrollPane( _buttons ) ;
      _setupPanel = new SetupPanel( args ) ;

      _tab.addTab(" Setup "      , _setupPanel) ;
      _tab.addTab(" WorkCanvas " , _scroll ) ;
      _tab.addTab(" Private "    , _private ) ;

      _tab.setSelectedIndex(0);

      c.add( _tab ) ;
      int count = 20 ;
      if( args.length > 0 )count = Integer.parseInt( args[0] ) ;
      for( int i = 0 ; i < count ; i++ ){
         InfoPanel info = new InfoPanel( "Link "+i ) ;
         _buttonList.add(info) ;
         _buttons.add( info.getInfoBar() ) ;
         System.out.println("Done "+i);
      }



   }
   public static void main( String [] args ){
       JDCap f = new JDCap("dCap [Version II]" , args );

       f.pack();
       Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
       f.setLocation(100,100);
       f.setSize(1000,400);
       f.setVisible(true);
   }
}

package  dmg.cells.examples ;

import   dmg.cells.nucleus.* ;
import   dmg.util.* ;

import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
   
import java.io.* ;
   
/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
 public class      GuiDomain 
        extends    Frame 
        implements ActionListener {
                               
    MenuBar         _menuBar ;
    Menu            _editMenu , _fileMenu , _widthMenu ;
    MenuItem        _editEditItem ;
    MenuItem        _fileExitItem ;
    jWorksheet      _worksheetCell ;
    LogoCell        _logoCell1 , _logoCell2 ;
    LogoCell        _logoCell3 , _logoCell4 ;
    ShellCell       _cellShell ;
    
    public GuiDomain(  String [] args ){
    
      String domainName = args.length < 1 ? "GuiDomain" : args[0] ;
      Cell system = new SystemCell( domainName  ) ;
      setTitle( domainName ) ;

      setLayout( new BorderLayout() ) ;      
          
      _worksheetCell = new WorksheetCell("Worksheet") ;
      _cellShell     = new ShellCell( "Shell" , 80 , 15 ) ;
//      _logoCell1     = new LogoCell( "Logo1" , 150 , 150 ) ;
//      _logoCell2     = new LogoCell( "Logo2" , 150 , 150 ) ;
//      _logoCell3     = new LogoCell( "Logo3" , 150 , 150 ) ;
//      _logoCell4     = new LogoCell( "Logo4" , 150 , 150 ) ;
//      Panel logos    = new Panel( new GridLayout(0,2) ) ;
      Panel logos    = new Panel( new GridLayout(0,1) ) ;
//      logos.add( _logoCell1 ) ;
      logos.add( _cellShell ) ;
//      logos.add( _logoCell2 ) ;
//      logos.add( _logoCell3 ) ;
//      logos.add( _logoCell4 ) ;
      
      add( logos           , "North" ) ;
      add( _worksheetCell  , "Center" ) ;
     
//      new DelayEchoCell( "echo" ) ;
      
      if( args.length > 1 ){
         new dmg.cells.network.GNLCell( "tunnel" ,
              "dmg.cells.network.SimpleTunnel "+args[1] ) ;
      }
      installMenu();
      
      
      pack();
      show();
       
 }
 private void installMenu(){
      _menuBar      = new MenuBar() ;
      
      _fileMenu     = new Menu( "File" ) ;
      
      _fileMenu.add( _fileExitItem = new MenuItem( "Exit" ) );
      _fileExitItem.addActionListener( this ) ;
      _fileExitItem.setActionCommand( "exit" ) ;
      
      _editMenu     = new Menu( "Edit" ) ;
      
      _editMenu.add( _editEditItem = new MenuItem( "Edit Topology" ) );
      _editEditItem.addActionListener( this ) ;
      _editEditItem.setActionCommand( "edit" ) ;
      
      _widthMenu     = new Menu( "Options" ) ;
      
      MenuItem item ;
            
      _widthMenu.add( item = new MenuItem( "AnimationUp" ) );
      item.addActionListener( this ) ;
      item.setActionCommand( "animationUp" ) ;
      _widthMenu.add( item = new MenuItem( "AnimationDown" ) );
      item.addActionListener( this ) ;
      item.setActionCommand( "animationDown" ) ;
      _widthMenu.add( item = new MenuItem( "AnimationInfinit" ) );
      item.addActionListener( this ) ;
      item.setActionCommand( "animationInfinit" ) ;
      _widthMenu.add( item = new MenuItem( "Worksheet" ) );
      item.addActionListener( this ) ;
      item.setActionCommand( "worksheet" ) ;
      _widthMenu.add( item = new MenuItem( "Host Menu" ) );
      item.addActionListener( this ) ;
      item.setActionCommand( "hosts" ) ;
      
      _menuBar.add( _fileMenu ) ;
//      _menuBar.add( _editMenu ) ;
      _menuBar.add( _widthMenu ) ;
      setMenuBar( _menuBar ) ;
    
 }
 public void actionPerformed( ActionEvent event ){
      Object source = event.getSource() ;
      String s      = event.getActionCommand() ;
      
      return ;
 }
 private void say( String text ){
       System.out.println( text ) ;
 }
 public static void main( String [] args ){
 
      for( int i = 0 ; i < args.length ; i++ )
         System.out.println( " GuiDomain main "+args[i] ) ;
      new GuiDomain( args  ) ;
    
 }
 
 
}

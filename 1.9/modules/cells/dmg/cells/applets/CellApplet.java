package dmg.cells.applets ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;

import dmg.util.* ;
import dmg.cells.examples.* ;
import dmg.cells.nucleus.* ;

public class      CellApplet 
       extends    Applet 
       implements ActionListener, Runnable       {
       
  private static boolean __systemCreated = false ;
  
  public void actionPerformed( ActionEvent event ){
     String command = event.getActionCommand() ;
     System.out.println( " Action : " + command ) ;
     System.out.println( " Ready "  ) ;
     
  }
  public void run(){
  }
  public void init(){
      System.out.println( "init ..." ) ;
      Dimension   d  = getSize() ;
      setLayout( new FlowLayout() ) ;
      
//      setSize(200,200);
      System.out.println( "Starting Cell Environment" ) ;
      if( ! __systemCreated ){
         new SystemCell( "CellAppletDomain" ) ;
         __systemCreated = true ;
      }
      add( new WorksheetCell( "WorksheetCell*" ) ) ;
      System.out.println( "CellAppletDomain started" ) ;
      setVisible( true ) ;
  }
  public void start(){
      System.out.println("start ..." ) ;
  }
  public void stop(){
  
  }
  public void destroy(){
  
  }      
       
}

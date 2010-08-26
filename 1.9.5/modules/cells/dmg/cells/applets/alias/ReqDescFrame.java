package dmg.cells.applets.alias ;

import java.lang.reflect.* ;
import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;

public class      ReqDescFrame 
       extends    Frame 
       implements WindowListener, ActionListener   {

   static final long serialVersionUID = 8056489938476137979L;

   private Font   _bigFont = 
             new Font( "SansSerif" , Font.BOLD , 18 )  ; 
   private Font   _bigFont2 = 
             new Font( "SansSerif" , Font.BOLD , 24 )  ; 

   private RequestDesc _request = new RequestDesc() ;
   private Button      _button  = new Button( "Update" ) ;
  public ReqDescFrame(  ){
      super( "ReqDescFrame" ) ;
      setLayout( new BorderLayout() ) ;
      addWindowListener( this ) ;
      setLocation( 60 , 60) ;
      add( _request , "North" ) ;
      add( _button , "South" ) ;
//      Object o = new WasteRequest() ;

//      Object o = new eurogate.vehicles.PvrRequestImpl(
//               "mount" , "stk" , "R0010" , "drive0" , "0:3:9:3" ) ;
               
//      _request.setRequest( o ) ;
      _button.addActionListener( this ) ;
      setSize( 750 , 500 ) ;
      pack() ;
      setSize( 750 , 500 ) ;
      setVisible( true ) ;
  
  }
  private int _counter = 1000 ;
  public void actionPerformed( ActionEvent event ){
      _counter ++ ;
//      Object o = new eurogate.vehicles.PvrRequestImpl(
//               "mount" , "stk" , "R"+_counter , "drive0" , "0:3:9:3" ) ;
      
//      if( _counter % 2 == 0 )       
//         _request.setRequest( o ) ;
//      else
//         _request.reset() ;
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
      try{
            
         new ReqDescFrame() ;
      
      }catch( Exception e ){
         e.printStackTrace() ;
         System.err.println( "Connection failed : "+e.toString() ) ;
         System.exit(4);
      }
      
   }

}

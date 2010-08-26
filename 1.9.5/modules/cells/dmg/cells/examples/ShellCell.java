package dmg.cells.examples ;
import java.awt.* ;
import java.awt.event.*;
import java.util.Random ;
import dmg.cells.nucleus.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      ShellCell 
       extends    Panel      
       implements Cell,
                  CellEventListener,
                  ActionListener           {

   private static final int  CELLS   =  1 ;
   private static final int  ROUTES  =  2 ;
   
   private TextArea    _textArea ;
   private Button      _cellButton , _routeButton ;
   private CellNucleus _nucleus ;
   private Object      _statLock = new Object() ;
   private int         _status   = CELLS ;
   private CellShell   _cellShell  ;
   
   public ShellCell( String name , int width , int height ){
   
      setLayout( new BorderLayout() ) ;
      
      _textArea     = new TextArea( height , width ) ;
      _textArea.setBackground( Color.orange ) ;
      _textArea.setForeground( Color.red ) ;
      _textArea.setFont( new Font( "Monospaced" , Font.BOLD, 10 ) );
      _cellButton   = new Button( "Update Cells" ) ;
      _cellButton.addActionListener( this ) ;
      _routeButton  = new Button( "Update Routes" ) ;
      _routeButton.addActionListener( this ) ;
      _nucleus      = new CellNucleus( this , name ) ;
      _cellShell    = new CellShell( _nucleus ) ;
      Panel buttons = new Panel( new FlowLayout() ) ;
      buttons.add( _cellButton ) ;
      buttons.add( _routeButton ) ;
      
      add( _textArea , "Center" ) ;
      add( buttons   , "South"  ) ;

      _status = CELLS ;
      try{
         _textArea.setText( _cellShell.command( "ps-f" ) ) ;
      }catch( Exception eee ){}

      _nucleus.addCellEventListener( this ) ;
      _nucleus.setPrintoutLevel(0);
      
   }
   public void  actionPerformed( ActionEvent event ){
      Object source = event.getSource() ;
      if( source == _cellButton ){
         synchronized( _statLock ){ 
            _status = CELLS ; 
            try{
            _textArea.setText( _cellShell.command( "ps-f" ) ) ;
            }catch(Exception eee){}
         }
      }else if( source == _routeButton ){
         synchronized( _statLock ){ 
            _status = ROUTES ; 
            try{
            _textArea.setText( _cellShell.command( "route" ) ) ;
            }catch(Exception eee){}
         }
      }
   }
   public String   toString(){
     return "Graphical Shell Cell";
   }
   public String   getInfo(){
      return toString()+"\n" ;
   }
   public void   messageArrived( MessageEvent me ){
   }
   public void   prepareRemoval( KillEvent ce ){
     _nucleus.say( " prepareRemoval "+ce ) ;
   }
   public void   exceptionArrived( ExceptionEvent ce ){
     _nucleus.say( " exceptionArrived "+ce ) ;
   }
   public void cellCreated( CellEvent ce ){
      synchronized( _statLock ){ 
        try{
         if( _status == CELLS ) {
           if( _textArea == null ){
               _nucleus.say( " textArea is null " ) ;
            }else if( _cellShell == null ){
               _nucleus.say( " textArea is null " ) ;
            }else{
               String str = _cellShell.command( "ps-f" ) ;
               if( str == null ){
                   _nucleus.say( " _cellShell.command is null " ) ;
               }else{
                  _textArea.setText( str ) ;
               }
            }
         }
        }catch( Exception eee ){}
      }
   }
   public void cellDied( CellEvent ce ){
      synchronized( _statLock ){ 
        try{
         if( _status == CELLS ){ 
            if( _textArea == null ){
               _nucleus.say( " textArea is null " ) ;
            }else if( _cellShell == null ){
               _nucleus.say( " textArea is null " ) ;
            }else{
               
               String str = _cellShell.command( "ps-f" ) ;
               if( str == null ){
                   _nucleus.say( " _cellShell.command is null " ) ;
               }else{
                  _textArea.setText( str ) ;
               }
            }
         }
        }catch( Exception eee ){}
      }
   }

   public void cellExported( CellEvent ce ){ }
   public void routeAdded( CellEvent ce ){ }
   public void routeDeleted( CellEvent ce ){ }



} 

package dmg.cells.applets.login ;

import java.awt.BorderLayout;
import java.awt.Button;
import java.awt.Checkbox;
import java.awt.CheckboxGroup;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Label;
import java.awt.Panel;
import java.awt.TextArea;
import java.awt.TextField;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.Serializable;

import dmg.cells.applets.spy.HistoryTextField;

import org.dcache.util.Args;


public class      CommanderPanel
       extends    SshActionPanel
       implements ActionListener ,
                  DomainConnectionListener  {

    private static final long serialVersionUID = 2843684664717959215L;
    private DomainConnection _dc;
    private TextArea   _display;
    private TextField  _input;
    private Button     _backButton;
    private Button     _clearButton;
    private Panel      _optionPanel;
    private int        _b = 20 ;
    private Font       _font  = new Font( "TimesRoman" , 0 , 24 ) ;
    private Font       _mFont = new Font( "Courier" , 0 , 14 ) ;
    @Override
    public Insets getInsets(){ return new Insets( _b , _b ,_b , _b ) ; }
    @Override
    public void paint( Graphics g ){
       Dimension d = getSize() ;
       int h = _b / 2 ;
       g.setColor( new Color(0,0,0) ) ;
       int [] xs = new int[4] ;
       int [] ys = new int[4] ;
       xs[0] = h           ; ys[0] = h ;
       xs[1] = d.width-h-1 ; ys[1] = h ;
       xs[2] = d.width-h-1 ; ys[2] = d.height - h - 1 ;
       xs[3] = h           ; ys[3] = d.height - h - 1 ;
       g.drawPolygon( xs , ys , xs.length  ) ;
    }
    CheckboxGroup _checkGroup;
    Checkbox _checkString;
    Checkbox _checkArgs;
    Checkbox _checkArray;
    public CommanderPanel( DomainConnection dc ){
       BorderLayout bl = new BorderLayout() ;
       bl.setHgap(15) ;
       bl.setVgap(15) ;
        setLayout( bl ) ;

        setBackground( new Color(210,210,210) ) ;
       _dc = dc ;
       _display = new TextArea() ;
       _display.setFont( _mFont ) ;
       _input   = new HistoryTextField() ;

       _optionPanel = new Panel( new FlowLayout( FlowLayout.CENTER ) ) ;

       Panel topPanel  = new Panel( new GridLayout(0,1) ) ;

       Label header = new Label( "Commander" , Label.CENTER ) ;
       header.setFont( _font ) ;

       topPanel.add( header ) ;

       Panel buttonPanel = new Panel( new GridLayout(1,0) ) ;

       buttonPanel.add( _backButton = new  Button("Back" ) ) ;
       buttonPanel.add( _clearButton = new Button("Clear Screen" ) ) ;

       _optionPanel.add( buttonPanel ) ;
       _optionPanel.add( new Label("Send Command as : " ) ) ;

       _checkGroup  = new CheckboxGroup() ;
       _checkString = new Checkbox( "String" , _checkGroup , true ) ;
       _checkArgs   = new Checkbox( "Args" , _checkGroup , false ) ;
       _checkArray  = new Checkbox( "Array" , _checkGroup , false ) ;

       Panel checkPanel = new Panel( new GridLayout(1,0) ) ;
       checkPanel.add( _checkString ) ;
       checkPanel.add( _checkArgs ) ;
       checkPanel.add( _checkArray ) ;

       _optionPanel.add( checkPanel ) ;
       _backButton.addActionListener( this ) ;
       _clearButton.addActionListener( this ) ;

       _input.addActionListener( this ) ;

       topPanel.add( _optionPanel ) ;

       add( topPanel , "North" ) ;
       add( _display , "Center" ) ;
       add( _input   , "South" ) ;
    }
    @Override
    public synchronized void actionPerformed( ActionEvent event ){
       String command = event.getActionCommand() ;
       System.out.println( "Action CommanderPanel : "+command ) ;
       Object obj = event.getSource() ;
       if( obj == _backButton ){
          informActionListeners( "exit" ) ;
       }else if( obj == _clearButton ){
          _display.setText("");
       }else if( obj == _input ){
          String in = _input.getText() ;
          if( in.length() == 0 ) {
              return;
          }
          _input.setText("") ;
          System.out.println( "Got : "+in ) ;
          sendCommand( in ) ;
       }
    }
    private void sendCommand( String in ){

       Checkbox box = _checkGroup.getSelectedCheckbox() ;
       Serializable toBeSent;
       if( box == _checkString ){
          toBeSent = in ;
       }else if( box == _checkArgs ){
          toBeSent = new Args( in ) ;
       }else if( box == _checkArray ){
          Args args = new Args( in ) ;
          Object [] array = new Object[args.argc()] ;
          for( int i= 0 ; i < args.argc() ; i++ ) {
              array[i] = args.argv(i);
          }
          toBeSent = array ;
       }else {
           return;
       }
       try{
          _dc.sendObject( toBeSent , this , 0 ) ;
       }catch( Exception e ){
          _display.append( "Problem in sendObject : "+e+"\n" ) ;
       }

    }
    @Override
    public void domainAnswerArrived( Object obj , int id ){
      /*
       _display.append( "---------------------------\n" ) ;
       _display.append( "   Message arrived : "+obj.getClass()+"\n" ) ;
       if( obj instanceof Object [] ){
          Object [] a = (Object [])obj  ;
          for( int i = 0 ; i < a.length ; i++ )
            _display.append( "array["+i+"] = "+a[i]+"\n" ) ;
       }else{
          _display.append( obj.toString() +"\n" ) ;
       }
       */
       pp( 0 , obj ) ;
    }
    private void pp( int p , Object obj ){
       if( obj instanceof Object [] ){
           Object [] ar = (Object []) obj ;
           for( int i = 0 ; i < ar.length ; i++ ){
              for( int j = 0 ; j < p ; j++ ) {
                  _display.append("     ");
              }
              _display.append( "["+i+"]=" ) ;
              pp( p+1 , ar[i] ) ;
           }
       }else{
//           _display.append( "("+obj.getClass().getName()+") "+obj.toString() + "\n" ) ;
           _display.append( obj.toString() + "\n" ) ;
       }

    }


}

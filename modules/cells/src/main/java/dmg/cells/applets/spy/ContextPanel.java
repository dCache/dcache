package dmg.cells.applets.spy ;

import java.awt.BorderLayout;
import java.awt.Button;
import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.ItemSelectable;
import java.awt.Label;
import java.awt.Panel;
import java.awt.TextArea;
import java.awt.TextField;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

import dmg.cells.network.CellDomainNode;
import dmg.cells.services.MessageObjectFrame;

import org.dcache.util.Args;


class ContextPanel
      extends Panel
      implements ActionListener, FrameArrivable, ItemListener {

   private static final long serialVersionUID = -5025275864041931726L;
   private DomainConnection _connection ;
   private Button   _updateButton ;
   private Button   _writeButton ;
   private Button   _newContextButton ;
   private Button   _removeContextButton ;
   private Label    _topLabel  ;
   private TextArea _contextText ;
   private TextField _contextNameField ;
   private SpyList   _list ;
   private Font   _bigFont   = new Font( "SansSerif" , Font.ITALIC , 18 )  ;
   private Font   _smallFont = new Font( "SansSerif" , Font.ITALIC|Font.BOLD , 14 )  ;
   private Font   _textFont  = new Font( "Monospaced" , Font.ITALIC   , 14 )  ;
   private CellDomainNode _domainNode;
   private String [] _contextList = new String[0] ;
   private String    _contextName;
   private boolean   _useColor;

   ContextPanel( DomainConnection connection ){
      _connection = connection ;
      _useColor   = System.getProperty( "bw" ) == null ;
      if( _useColor ) {
          setBackground(Color.orange);
      }
      setLayout( new BorderLayout() ) ;

      _topLabel = new Label( "Context" , Label.CENTER )  ;
      _topLabel.setFont( _bigFont ) ;
      add( _topLabel , "North" ) ;

      _updateButton = new Button( "Update List" ) ;
      _updateButton.addActionListener( this ) ;
      _writeButton = new Button( "Rewrite" ) ;
      _writeButton.addActionListener( this ) ;
      _writeButton.setBackground( Color.red ) ;

      Panel buttonPanel = new Panel( new FlowLayout(FlowLayout.CENTER) ) ;
//      buttonPanel.add( _updateButton ) ;
//      buttonPanel.add( _writeButton ) ;

      add( buttonPanel , "South" ) ;

      _contextText = new TextArea() ;
      _contextText.setFont( _textFont ) ;
//      _contextText.setBackground( back ) ;
//      _contextText.setBackground( Color.blue ) ;
      add( new BorderPanel( _contextText ) , "Center" ) ;

      Panel leftPanel = new Panel( new BorderLayout() ) ;
      _list = new SpyList() ;
      _list.addItemListener( this ) ;
//      _list.setBackground( back ) ;
      leftPanel.add( _list , "Center" ) ;
      Panel downPanel = new Panel( new GridLayout( 0 , 1 ) ) ;

      downPanel.add( _updateButton ) ;
      downPanel.add( _writeButton ) ;

      leftPanel.add( new BorderPanel( downPanel ) , "South" ) ;

      add( new BorderPanel( leftPanel ) , "West" ) ;

      _contextNameField = new TextField() ;

      _newContextButton = new Button( "New Context" ) ;
      _newContextButton.addActionListener( this ) ;
      _newContextButton.setBackground( Color.red ) ;
      _removeContextButton = new Button( "Remove Context" ) ;
      _removeContextButton.addActionListener( this ) ;
      _removeContextButton.setBackground( Color.red ) ;

      Panel bottomPanel = new Panel( new BorderLayout() ) ;
      bottomPanel.add( _removeContextButton , "East" ) ;
      bottomPanel.add( _newContextButton    , "West" ) ;
      bottomPanel.add( _contextNameField    , "Center" ) ;

      add( bottomPanel , "South" ) ;
   }
   @Override
   public Insets getInsets(){ return new Insets( 20 , 20 ,20 , 20 ) ; }
   @Override
   public void actionPerformed( ActionEvent event ){
       Object o = event.getSource() ;
       if( o == _updateButton ){
          updateDomain() ;
          _contextText.setText("");
       }else if( o == _newContextButton ){
          if( _domainNode == null ) {
              return;
          }
          final String newContextName = _contextNameField.getText() ;
          if( newContextName.equals("") ) {
              return;
          }
          String req = "set -c context "+newContextName+" "+newContextName ;
          _connection.send(
              _domainNode.getAddress() , req ,
              new FrameArrivable(){
                  @Override
                  public void frameArrived( MessageObjectFrame frame ){
                      Object result = frame.getObject() ;
                      _contextText.setText( result.toString() ) ;
                      _contextNameField.setText("") ;
                      if( ! ( result instanceof Exception ) ){
                          updateDomain(
                             new FrameArrivable(){
                                 @Override
                                 public void frameArrived( MessageObjectFrame frame ){
                                     ContextPanel.this.frameArrived(frame) ;
                                     Object res = frame.getObject() ;
                                     if( res instanceof Exception ){
                                         _contextText.setText( res.toString() ) ;
                                     }else{
                                         updateDomain(newContextName) ;
                                         _list.select(newContextName) ;
                                     }
                                 }
                             }
                          ) ;
                      }
                  }
              }
          ) ;
       }else if( o == _removeContextButton ){
          if( ( _domainNode == null ) || ( _contextName == null ) ) {
              return;
          }
          String req = "unset context "+_contextName ;
          _connection.send(
              _domainNode.getAddress() ,
              req ,
              new FrameArrivable(){
                  @Override
                  public void frameArrived( MessageObjectFrame frame ){
                      _contextText.setText( frame.getObject().toString() ) ;
                      updateDomain() ;
                  }
              }
          ) ;
       }else if( o == _writeButton ){

          if( ( _domainNode == null ) || ( _contextName == null ) ) {
              return;
          }

          String command = "set context " + _contextName + " " +
                  Args.quote(_contextText.getText());
          _connection.send(_domainNode.getAddress(), command, this);
       }
   }
   @Override
   public void frameArrived( MessageObjectFrame frame ){
       Object obj = frame.getObject() ;
       if( obj instanceof String [] ){
          //
          // an array of strings is always the context list  :-)
          _contextList = (String [])obj ;
          _list.removeAll() ;
           for (String contextElement : _contextList) {
               _list.add(contextElement);
           }
          _contextText.setText("");
       }else if( obj instanceof Object [] ){
          //
          // this is the return of an write context
          //
          _list.select( ((Object[])obj)[0].toString() ) ;
          _contextText.setText( ((Object[])obj)[1].toString() ) ;
       }else{
          //
          // and this is whatever arrives
          //
          _contextText.setText( obj.toString() ) ;
       }
   }
   private void updateDomain(){
      updateDomain( this ) ;
   }
   private void updateDomain( FrameArrivable listener ){
      if( _domainNode == null ) {
          return;
      }
      _contextText.setText("") ;
      _contextName = null ;
      _updateButton.setEnabled(true) ;
      _newContextButton.setEnabled(true) ;
      _writeButton.setEnabled(false) ;
      _removeContextButton.setEnabled(false) ;
      _connection.send( _domainNode.getAddress() , "getcontext" ,listener ) ;
   }
   private void updateDomain( String contextName ){
      if( _domainNode == null ) {
          return;
      }
      _connection.send( _domainNode.getAddress() ,
                        "getcontext " + contextName  , this ) ;
      _contextName = contextName ;
      _writeButton.setEnabled(true) ;
      _removeContextButton.setEnabled(true) ;
   }
   public void clear(){
      _topLabel.setText( "<Context>" ) ;
      _list.removeAll() ;
      _contextText.setText("");
      _contextName = null ;
      setEnabled(false) ;
   }
   @Override
   public void setEnabled( boolean enable ){
      _updateButton.setEnabled(enable) ;
      _writeButton.setEnabled(enable) ;
      _newContextButton.setEnabled(enable) ;
      _removeContextButton.setEnabled(enable) ;
   }
   public void showDomain( CellDomainNode domainNode ){
      _topLabel.setText( " Context of "+domainNode.getName()) ;
      _domainNode = domainNode ;
      updateDomain() ;
   }
   @Override
   public void itemStateChanged( ItemEvent event ){
      ItemSelectable sel = event.getItemSelectable() ;
      Object [] obj = sel.getSelectedObjects() ;
      if( ( obj == null ) || ( obj.length == 0 ) ) {
          return;
      }

      String contextName = obj[0].toString() ;
      updateDomain(contextName);
   }

}

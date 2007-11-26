package dmg.cells.applets ;

import java.applet.*;
import java.awt.* ;


public class XFlowLayout extends FlowLayout {

 public XFlowLayout(){ 
    super() ; 
    System.out.println( "XFlow called" ) ; 
 }
 public void addLayoutComponent(String name, Component comp){
    System.out.println( "addLayoutComponent "+name+";comp="+comp) ;
    super.addLayoutComponent( name , comp ) ;
 }


 public void removeLayoutComponent(Component comp){
    System.out.println( "removeLayoutComponent ;comp="+comp) ;
    super.removeLayoutComponent( comp ) ;
 }
 public Dimension preferredLayoutSize(Container parent){
    System.out.println( "preferredLayoutSize ;Container="+parent) ;
    return super.preferredLayoutSize( parent ) ;
 } 
 public Dimension minimumLayoutSize(Container parent){
    System.out.println( "minimumLayoutSize ;Container="+parent) ;
    return super.minimumLayoutSize( parent ) ;
 }
 public void layoutContainer(Container parent){
    System.out.println( "layoutContainer ;Container="+parent) ;
    super.layoutContainer( parent ) ;
 }
 
}

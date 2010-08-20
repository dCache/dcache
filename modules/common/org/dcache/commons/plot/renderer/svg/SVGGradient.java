/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.commons.plot.renderer.svg;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
/**
 *
 * @author taolong
 */
public class SVGGradient implements SVGColor{
    protected Element element;
    protected String ref;
    protected int x1 = 0, y1 = 0, x2 = 100, y2 = 100;

    public SVGGradient(Document doc, String reference){
        element = doc.createElement("linearGradient");
        element.setAttribute("id", reference);
        ref = reference;
    }

    /**
     *
     * @return dom element representing this gradient
     */
    public Element getElement(){
        return element;
    }

   /**
    * set current gradient as simple linear gradient
    * @param x1 percentage of x offset (1-100)
    * @param y1 percentage of y offset (1-100)
    * @param color1 color of first offset
    * @param opacity1 opacity of first offset
    * @param x2 percentage of x end offset (1-100)
    * @param y2 percentage of y end offset (1-100)
    * @param color2 color of end offset
    * @param opacity2 opacity of end offset
    */
    public void setLinerGradient(float x1, float y1, RGBColor color1, float opacity1,
            float x2, float y2, RGBColor color2, float opacity2){
       element.setAttribute("x1", x1 + "%");
       element.setAttribute("y1", y1 + "%");
       element.setAttribute("x2", x2 + "%");
       element.setAttribute("y2", y2 + "%");

       Element stop1 = element.getOwnerDocument().createElement("stop");
       stop1.setAttribute("offset", "0%");
       stop1.setAttribute("style", "stop-color:" + color1 +";stop-opacity:"
               + opacity1);
       element.appendChild(stop1);

       Element stop2 = element.getOwnerDocument().createElement("stop");
       stop2.setAttribute("offset", "100%");
       stop2.setAttribute("style", "stop-color:" + color2 +";stop-opacity:"
               + opacity2);

       element.appendChild(stop2);
    }

    /**
     *
     * @return reference to this element as String
     */
    @Override
    public String toString() {
        return "url(#" + ref + ")";
    }
}

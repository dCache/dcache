/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.commons.plot.renderer.svg;
import java.awt.Color;
/**
 *
 * @author timur and tao
 */
public class RGBColor extends Color implements SVGColor{

    /**
     * default constructor, black
     */
    public RGBColor(){
        super(0, 0, 0);
    }
    /**
     * constructor a rgb color
     * @param r red 0-255
     * @param g green 0-255
     * @param b blue 0-255
     */
    public RGBColor(int r, int g, int b){
        super(r, g, b);
    }

    public RGBColor(Color color){
        super(color.getRed(), color.getGreen(), color.getBlue());
    }
    /**
     * reformat as svg
     * @return
     */
    @Override
    public String toString(){
        return "rgb(" + getRed() + "," + getGreen() + ","
                + getBlue() + ")";
    }
}
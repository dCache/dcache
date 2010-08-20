/*
 * This class wraps commonly used SVG entities, (e.g. lines, texts, colors)
 * around a XML document, it does not require awt or swing
 * and open the template in the editor.
 */
package org.dcache.commons.plot.renderer.svg;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import org.apache.xerces.dom.DocumentImpl;
import org.apache.xml.serialize.OutputFormat;

import java.io.FileOutputStream;
import java.util.List;
import org.apache.xml.serialize.XMLSerializer;
import org.w3c.dom.NodeList;

/**
 *
 * @author taolong
 */
public class SVGDocument {

    protected Document document = new DocumentImpl();
    protected Element svg, defs;
    //state variables
    protected float opacity = 1.0f, strokeWidth = 1.0f;
    protected SVGColor fill = new RGBColor(255, 255, 255),
            stroke = new RGBColor();
    protected float textSize = 12;
    String fontFamily = "arial";

    public String getFontFamily() {
        return fontFamily;
    }

    public void setFontFamily(String fontFamily) {
        this.fontFamily = fontFamily;
    }

    public float getTextSize() {
        return textSize;
    }

    public void setTextSize(float textSize) {
        this.textSize = textSize;
    }

    public enum TextAlignment {

        LEFT,
        CENTER,
        RIGHT
    }
    protected TextAlignment textAlignment = TextAlignment.LEFT;

    /**
     * default constructor, initialize svg element and its dimension attributes
     * width and height are initialize from system properties "svg_width" and
     * "svg_height" and default to 640 x 480
     * It also defines a "defs" element for gradient definition and etc
     */
    public SVGDocument() {
        svg = document.createElement("svg");
        document.appendChild(svg);

        //set svg element attribute
        svg.setAttribute("version", "1.1");
        svg.setAttribute("xmlns", "http://www.w3.org/2000/svg");

        //defs
        defs = document.createElement("defs");
        svg.appendChild(defs);
    }

    public void setWidth(float width) {
        svg.setAttribute("width", Float.toString(width));
    }

    public void setHeight(float height) {
        svg.setAttribute("height", Float.toString(height));
    }

    public SVGColor getFillColor() {
        return fill;
    }

    public SVGColor getStroke() {
        return stroke;
    }

    public void setStroke(SVGColor stroke) {
        this.stroke = stroke;
    }

    public TextAlignment getTextAlignment() {
        return textAlignment;
    }

    public void setTextAlignment(TextAlignment textAlignment) {
        this.textAlignment = textAlignment;
    }

    /**
     *
     * @return defs element for adding gradient definition etc
     */
    public Element getDefs() {
        return defs;
    }

    /**
     *
     * @return svg element for adding gradient definition etc
     */
    public Element getRoot() {
        return svg;
    }

    /**
     * set fill color
     * @param color
     */
    public void setFillColor(SVGColor color) {
        this.fill = color;

        //if current color is gradient, check to see if need to be added to
        //defs
        if (color.getClass().equals(SVGGradient.class)) {
            NodeList nodeList = defs.getChildNodes();
            for (int i = 0; i < nodeList.getLength(); i++) {
                if (nodeList.item(i) == color) {
                    return;
                }
            }

            SVGGradient gradient = (SVGGradient) color;
            defs.appendChild(gradient.getElement());
        }
    }

    /**
     * set stroke color, affecting entities like rectangle, lines, etc
     * @param color
     */
    public void setStrokeColor(SVGColor color) {
        this.stroke = color;
    }

    /**
     * set opacity of svg identities
     * @param opacity
     */
    public void setOpacity(float opacity) {
        this.opacity = opacity;
    }

    /**
     * set stroke width, affecting entities like rectangle, lines, etc
     * @param strokeWidth
     */
    public void setStrokeWidth(float strokeWidth) {
        this.strokeWidth = strokeWidth;
    }

    /**
     * create a rectangle element
     * starting at location (x, y) with dimension (width, height).
     * and add this element to svg element
     * @param x x
     * @param y y
     * @param width width
     * @param height height
     * @return dom element representing the rectangle
     */
    public Element createRectangle(float x, float y, float width, float height) {
        Element rect = document.createElement("rect");
        rect.setAttribute("x", Float.toString(x));
        rect.setAttribute("y", Float.toString(y));
        rect.setAttribute("width", Float.toString(width));
        rect.setAttribute("height", Float.toString(height));
        String style = "fill:" + fill
                + ";stroke:" + stroke
                + ";stroke-width:" + strokeWidth
                + ";opacity:" + opacity;
        rect.setAttribute("style", style);
        return rect;
    }

    /**
     * create a text element
     * @param x x coordinate
     * @param y y coordinate
     * @param t text string
     * @return
     */
    public Element createText(float x, float y, String t) {
        Element text = document.createElement("text");
        text.setAttribute("x", Float.toString(x));
        text.setAttribute("y", Float.toString(y));
        text.setTextContent(t);

        String style = "font-family:" + fontFamily
                + ";font-size:" + textSize
                + ";fill:" + fill;

        text.setAttribute("text-anchor", ((textAlignment == TextAlignment.LEFT) ? "start"
                : (textAlignment == TextAlignment.CENTER) ? "middle" : "end"));

        text.setAttribute("style", style);

        return text;
    }

    /**
     *  create a solid line (dash == null) or a dashed line (dash != null)
     * @param x1 starting x
     * @param y1 starting y
     * @param x2 ending x
     * @param y2 ending y
     * @param dash array to specify "stroke-dasharray", e.g. (2 3)
     * @return element representing the line
     */
    public Element createLine(float x1, float y1, float x2, float y2,
            float[] dash) {
        Element line = document.createElement("line");
        line.setAttribute("x1", Float.toString(x1));
        line.setAttribute("y1", Float.toString(y1));
        line.setAttribute("x2", Float.toString(x2));
        line.setAttribute("y2", Float.toString(y2));
        String style = "stroke:" + stroke
                + ";stroke-width:" + strokeWidth
                + ";opacity:" + opacity;

        //if dash specification exists, use dash
        if (dash != null && dash.length >= 1) {
            style += ";stroke-dasharray:";
            for (int i = 0; i < dash.length; i++) {
                style += dash[i];
                if (i != dash.length - 1) {
                    style += ",";
                }
            }
        }
        line.setAttribute("style", style);
        return line;
    }

    public Element createCircle(float x, float y, float r) {
        Element circle = document.createElement("circle");
        circle.setAttribute("cx", Float.toString(x));
        circle.setAttribute("cy", Float.toString(y));
        circle.setAttribute("r", Float.toString(r));

        circle.setAttribute("stroke", stroke.toString());
        circle.setAttribute("fill", fill.toString());
        circle.setAttribute("stroke-width", Float.toString(strokeWidth));

        return circle;
    }

    public Element createPolygon(List<SVGPoint> vertices) {
        Element poly = document.createElement("polygon");

        StringBuilder builder = new StringBuilder();
        for (SVGPoint vertex : vertices) {
            builder.append(vertex.x);
            builder.append(",");
            builder.append(vertex.y);
            builder.append(" ");
        }

        poly.setAttribute("points", builder.toString());

        String style = "fill:" + fill
                + ";stroke:" + stroke
                + ";stroke-width:" + strokeWidth
                + ";opacity:" + opacity;
        poly.setAttribute("style", style);
        return poly;
    }

    /**
     * set rotation of a <g> element
     * @param g must be a <g> element to have effect
     * @param angle rotation angle in g
     * @param x pivot x
     * @param y pivot y
     */
    public void svgSetRotate(Element g, float angle, float x, float y) {
        if (g.getNodeName().compareTo("g") != 0) {
            return;
        }
        String r = "rotate(" + angle + "," + x + "," + y + ")";
        g.setAttribute("transform", r);
    }

    /**
     * create group transform
     * @param id
     * @return
     */
    public Element createGroup(String id) {
        Element g = document.createElement("g");
        g.setAttribute("id", id);
        return g;
    }

    /**
     * @return svg document
     */
    public Document getDocument() {
        return document;
    }

    /**
     * serialize current svg document to a file
     * @param filename output file name
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void serialize(String filename) throws FileNotFoundException, IOException {
        FileOutputStream fos = new FileOutputStream(filename);

        OutputFormat of = new OutputFormat("XML", "ISO-8859-1", true);
        of.setIndent(1);
        of.setIndenting(true);
        XMLSerializer serializer = new XMLSerializer(fos, of);

        serializer.asDOMSerializer();
        serializer.serialize(document.getDocumentElement());
        fos.close();
    }
}

package org.dcache.commons.plot.renderer.svg;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.dcache.commons.plot.PlotException;
import org.dcache.commons.plot.PlotParameter;
import org.dcache.commons.plot.dao.Tuple;
import org.dcache.commons.plot.dao.TupleDateNumber;
import org.dcache.commons.plot.dao.TupleList;
import org.dcache.commons.plot.renderer.AxisInfo;
import org.dcache.commons.plot.renderer.ParamColumnRenderDescriptor;
import org.dcache.commons.plot.renderer.Range;
import org.dcache.commons.plot.renderer.Scale;
import org.dcache.commons.plot.renderer.svg.SVGDocument.TextAlignment;
import org.w3c.dom.Element;

/**
 * This class renders data into a rectangular area bounded by 4 (top, left
 * right, bottom axis). The input (TupleList) must contain TupleDateNumber
 *
 * @author timur and tao
 */
public class RectRenderer extends SVGRenderer<TupleList<TupleDateNumber> > {

    private AxisInfo bottomAxis = null, leftAxis = null,
            topAxis = null, rightAxis = null;
    private Set<AxisInfo> axis;

    public Set<AxisInfo> getAxis() {
        return axis;
    }

    public void setAxis(Set<AxisInfo> axis) {
        this.axis = axis;
    }

    @Override
    protected void renderFrame() throws PlotException {
        super.renderFrame();

        for (AxisInfo curAxis : getAxis()) {
            switch (curAxis.getLocation()) {
                case BOTTOM:
                    bottomAxis = curAxis;
                    break;
                case RIGHT:
                    rightAxis = curAxis;
                    break;
                case LEFT:
                    leftAxis = curAxis;
                    break;
                case TOP:
                    topAxis = curAxis;
                    break;
            }
        }

        this.renderAxis(bottomAxis);
        this.renderAxis(leftAxis);

        if (topAxis != null) {
            this.renderAxis(topAxis);
        }

        if (rightAxis != null) {
            this.renderAxis(rightAxis);
        }

    }

    private void renderAxis(AxisInfo axis) throws PlotException {
        float x1 = 0, y1 = 0, x2 = 0, y2 = 0;
        float xOffset = axis.getTickLength();
        float yOffset = axis.getTickLength();
        svg.setFillColor(SVGColor.BLACK);
        svg.setStrokeColor(SVGColor.BLACK);
        svg.setOpacity(1);
        svg.setTextAlignment(TextAlignment.LEFT);
        switch (axis.getLocation()) {
            case BOTTOM:
                x1 = margin * 2;
                x2 = width - x1;
                y2 = y1 = height - x1;
                yOffset *= -1;
                xOffset = 0;
                if (axis.getRange() == null) {
                    Collection c = new HashSet();
                    for (Object obj : tupleList.getTuples()) {
                        Tuple tuple = (Tuple) obj;
                        c.add(tuple.getXValue());
                    }
                    Range xRange = new Range();
                    xRange.findRange(c);
                    axis.setRange(xRange);
                }
                break;
            case LEFT:
                y2 = x2 = x1 = margin * 2;
                y1 = height - y2;
                yOffset = 0;
                svg.setTextAlignment(TextAlignment.RIGHT);
                if (axis.getRange() == null) {
                    Collection c = new HashSet();
                    Range<BigDecimal> range = new Range<BigDecimal>();
                    for (Object obj : tupleList.getTuples()) {
                        Tuple tuple = (Tuple) obj;
                        if (tuple.getYValue() instanceof Number) {
                            c.add(tuple.getYValue());
                        }
                        if (tuple.getYValue() instanceof Collection) {
                            Collection c2 = (Collection) tuple.getYValue();
                            Iterator iterator = c2.iterator();

                            while (iterator.hasNext()) {
                                c.add(iterator.next());
                            }
                        }
                    }
                    range.findRange(c);

                    //indication of empty bins
                    if (bottomAxis.getRange().getNumBins(bottomAxis.getBinSize()) > tupleList.size()
                            && range.getMinimum().compareTo(new BigDecimal(0)) > 0) {
                        range.setMinimum(new BigDecimal(0));
                    }
                    axis.setRange(range);
                }
                break;
            case TOP:
                x1 = y1 = y2 = margin * 2;
                x2 = width - x1;
                xOffset = 0;
                break;
            case RIGHT:
                y2 = margin * 2;
                x1 = x2 = width - y2;
                y1 = height - y2;
                yOffset = 0;
                xOffset *= -1;
                break;
        }

        elemFrame.appendChild(svg.createLine(x1, y1, x2, y2, null));

        if (axis.getScale() == Scale.LINEAR) {
            for (int i = 0; i < axis.getMaxNumTicks(); i++) {
                Object label = axis.getRange().getItemAt(1.0f * i / (axis.getMaxNumTicks() - 1));
                float x = x1 + i * (x2 - x1) / (axis.getMaxNumTicks() - 1);
                float y = y1 + i * (y2 - y1) / (axis.getMaxNumTicks() - 1);
                //draw ticks
                elemFrame.appendChild(svg.createLine(x, y, x + xOffset, y + yOffset, null));
                //draw text
                Element text = svg.createText(x - xOffset * 2, y - yOffset * 2, axis.getFormat().format(label));
                text.setAttribute("transform", "rotate(" + axis.getTextAngle() + ", " + (x - xOffset)
                        + ", " + (y - yOffset) + ")");
                elemFrame.appendChild(text);
            }
        }

        if (axis.getScale() == Scale.LOG) {
            if (!(axis.getRange().getMaximum() instanceof Number)) {
                return;
            }
            double maxValue = ((Number) axis.getRange().getMaximum()).doubleValue();
            for (double curValue = 1.0; curValue < maxValue; curValue *= 10) {
                float x = (float) (x1 + Math.log(curValue) / Math.log(maxValue) * (x2 - x1));
                float y = (float) (y1 + Math.log(curValue) / Math.log(maxValue) * (y2 - y1));

                //draw ticks
                elemFrame.appendChild(svg.createLine(x, y, x + xOffset, y + yOffset, null));
                //draw text
                Element text = svg.createText(x - xOffset * 2, y - yOffset * 2, axis.getFormat().format(curValue));
                text.setAttribute("transform", "rotate(" + axis.getTextAngle() + ", " + (x - xOffset)
                        + ", " + (y - yOffset) + ")");
                elemFrame.appendChild(text);
            }
        }
    }

    /**
     * concrete implementation of rendering data specified by ParamColumnRenderDescriptor
     * @throws PlotException
     */
    @Override
    protected void renderData() throws PlotException {
        this.elemData = svg.createGroup("data");
        svg.getRoot().appendChild(elemData);
        for (PlotParameter param : request.getParameters()) {
            if (!(param instanceof ParamColumnRenderDescriptor)) {
                continue;
            }
            ParamColumnRenderDescriptor render = (ParamColumnRenderDescriptor) param;

            int index = render.getIndex();

            Range rangeObj = null;
            Scale scale = Scale.LINEAR;
            switch (render.getRangeReference()) {
                case LEFT:
                    rangeObj = this.leftAxis.getRange();
                    scale = leftAxis.getScale();
                    break;
                case RIGHT:
                    rangeObj = this.rightAxis.getRange();
                    scale = rightAxis.getScale();
                    break;
                case TOP:
                case BOTTOM:
                    throw new PlotException("data render range reference must be LEFT or RIGHT");
            }

            if (!(rangeObj.getMaximum() instanceof Number)) {
                throw new PlotException("LEFT or RIGHT axis range must be instance of Number");
            }

            double maxValue = ((Number) (rangeObj.getMaximum())).doubleValue();
            double minValue = ((Number) (rangeObj.getMinimum())).doubleValue();
            double range = maxValue - minValue;

            float[] yCoords = null;
            int numBins = bottomAxis.getRange().getNumBins(bottomAxis.getBinSize());
            yCoords = new float[numBins];

            //populate bins
            for (TupleDateNumber tuple : tupleList.getTuples()) {
                List<Number> numArray = tuple.getYValue();
                double yValue = numArray.get(index).doubleValue();
                int xCoord = (int) (bottomAxis.getRange().getPosition((Comparable) tuple.getXValue()) * numBins);
                if (xCoord >= numBins) {
                    xCoord = numBins - 1;
                }
                switch (scale) {
                    case LINEAR:
                        yCoords[xCoord] = (float) (yValue / range);
                        break;
                    case LOG:
                        yCoords[xCoord] = (float) (Math.log(yValue + 1) / Math.log(range + 1));
                        break;
                }

                //render
                svg.setFillColor(render.getColor());
                float w = (width - 4 * margin) / yCoords.length;
                float px = 0, py = 0;
                for (int i = 0; i < yCoords.length; i++) {
                    float x = i * (width - 4 * margin) / yCoords.length + margin * 2;
                    float y = height - 2 * margin - yCoords[i] * (height - 4 * margin);

                    switch (render.getRenderStyle()) {

                        case BARS:
                            svg.setFillColor(render.getColor());
                            svg.setStrokeWidth(1);
                            svg.setStrokeColor(SVGColor.BLACK);
                            Element bar = svg.createRectangle(x, y, w, height - 2 * margin - y);
                            elemData.appendChild(bar);
                            break;
                        case DOTS:
                            svg.setFillColor(SVGColor.WHITE);
                            svg.setStroke(render.getColor());
                            if (y == height - 2 * margin) {
                                break;
                            }
                            Element dot = svg.createCircle(x, y, 3);
                            elemData.appendChild(dot);
                            break;
                        case LINES:
                            if (i == 0) {
                                break;
                            }
                            if (y == height - 2 * margin
                                    && py == height - 2 * margin) {
                                break;
                            }
                            svg.setFillColor(render.getColor());
                            svg.setStroke(render.getColor());
                            Element line = svg.createLine(px, py, x, y, null);
                            elemData.appendChild(line);
                    }
                    px = x;
                    py = y;
                }
            }
        }
    }
}

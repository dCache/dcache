package org.dcache.commons.plot.renderer.svg;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.dcache.commons.plot.ParamDaoID;
import org.dcache.commons.plot.PlotException;
import org.dcache.commons.plot.dao.TupleDateNumber;
import org.dcache.commons.plot.dao.TupleList;
import org.dcache.commons.plot.renderer.Range;
import org.dcache.commons.plot.renderer.RenderStyle;
import org.dcache.commons.plot.renderer.Scale;
import org.w3c.dom.Element;

/**
 * This class renders data into a rectangular area bounded by 4 (top, left
 * right, bottom axis). The input (TupleList) must contain TupleDateNumber
 *
 * @author timur and tao
 */
public class RectRenderer extends SVGRenderer<TupleList<TupleDateNumber>> {

    private static final SVGColor[] colors = {
        new RGBColor(10, 10, 255),
        new RGBColor(10, 255, 15),
        new RGBColor(255, 10, 15),
        new RGBColor(0, 255, 155),
        new RGBColor(155, 255, 15),
        new RGBColor(255, 255, 15),};
    private static final RenderStyle renderStyle = RenderStyle.BARS;
    private Range<Date> xRange;
    private Range<BigDecimal> yRange;
    private DateFormat xFormat = new SimpleDateFormat("MMM,dd yyyy");
    private NumberFormat yFormat = new DecimalFormat("#0.00E0");
    private int numXLabels = 12;
    private int numYLabels = 10;
    private int xLabelAngle = 20;
    private float[] gridDash = {2.0f, 1.0f};
    private Scale scale = Scale.LOG;
    private String[] names = null;

    public RectRenderer() {
        super();
        svg.setWidth(width);
        svg.setHeight(height);
    }

    public float[] getGridDash() {
        return gridDash;
    }

    public void setGridDash(float[] gridDash) {
        this.gridDash = gridDash;
    }

    public String[] getNames() {
        return names;
    }

    public void setNames(String[] names) {
        this.names = names;
    }

    public int getNumXLabels() {
        return numXLabels;
    }

    public void setNumXLabels(int numXLabels) {
        this.numXLabels = numXLabels;
    }

    public int getNumYLabels() {
        return numYLabels;
    }

    public void setNumYLabels(int numYLabels) {
        this.numYLabels = numYLabels;
    }

    public Scale getScale() {
        return scale;
    }

    public void setScale(Scale scale) {
        this.scale = scale;
    }

    public DateFormat getxFormat() {
        return xFormat;
    }

    public void setxFormat(DateFormat xFormat) {
        this.xFormat = xFormat;
    }

    public int getxLabelAngle() {
        return xLabelAngle;
    }

    public void setxLabelAngle(int xLabelAngle) {
        this.xLabelAngle = xLabelAngle;
    }

    public NumberFormat getyFormat() {
        return yFormat;
    }

    public void setyFormat(NumberFormat yFormat) {
        this.yFormat = yFormat;
    }

    @Override
    protected void renderFrame() throws PlotException {
        super.renderFrame();

        if (tupleLists.isEmpty()) {
            return;
        }

        int rows = tupleLists.get(0).size();

        //collect data stats
        TupleList<TupleDateNumber> firstList = tupleLists.get(0);
        Date startDate = firstList.get(0).getXValue();
        Date endDate = firstList.get(firstList.size() - 1).getXValue();
        xRange = new Range<Date>();
        xRange.setMinimum(startDate);
        xRange.setMaximum(endDate);

        yRange = new Range<BigDecimal>();
        double max = Double.NEGATIVE_INFINITY;
        double min = Double.POSITIVE_INFINITY;

        for (TupleList<TupleDateNumber> tupleList : tupleLists) {
            for (TupleDateNumber tuple : tupleList.getTuples()) {
                if (max < tuple.getYValue().doubleValue()) {
                    max = tuple.getYValue().doubleValue();
                }

                if (min > tuple.getYValue().doubleValue()) {
                    min = tuple.getYValue().doubleValue();
                }
            }
        }

        if (scale == Scale.LINEAR) {
            if (min != 0) {
                int exp = (int) Math.log10(min);
                double newMin = 1;
                for (int i = 0; i < exp; i++) {
                    newMin *= 10;
                }
                if (newMin != min / 10) {
                    min = newMin;
                }
            }

            if (max != 0) {
                int exp = (int) Math.log10(max) + 1;
                double newMax = 1;
                for (int i = 0; i < exp; i++) {
                    newMax *= 10;
                }
                if (max < newMax / 10) {
                    max = newMax / 10;
                } else if (max < newMax / 8) {
                    max = newMax / 8;
                } else if (max < newMax / 5) {
                    max = newMax / 5;
                } else if (max < newMax / 4) {
                    max = newMax / 4;
                } else if (max < newMax / 2) {
                    max = newMax / 2;
                }
            }
        }
        yRange.setMaximum(new BigDecimal(max));
        yRange.setMinimum(new BigDecimal(min));

        float x1 = margin * 2, y1 = margin * 2;
        float x2 = width - margin * 2;
        float y2 = height - margin * 2;
        float w = width - 4 * margin;
        float h = height - 4 * margin;

        //draw frames
        svg.setStrokeColor(SVGColor.BLACK);
        svg.setStrokeWidth(1);
        elemFrame.appendChild(svg.createLine(x1, y1, x1, y2, null));
        elemFrame.appendChild(svg.createLine(x1, y1, x2, y1, null));
        elemFrame.appendChild(svg.createLine(x2, y1, x2, y2, null));
        elemFrame.appendChild(svg.createLine(x1, y2, x2, y2, null));

        //draw x labels
        if (numXLabels > rows) {
            numXLabels = rows;
        }

        if (numXLabels <= 0) {
            return;
        }

        svg.setStrokeColor(SVGColor.BLACK);
        svg.setFillColor(SVGColor.BLACK);
        svg.setTextAlignment(SVGDocument.TextAlignment.LEFT);

        float halfWidth = w / numXLabels / 2;
        for (float i = 0; i < numXLabels; i++) {
            float x = i / numXLabels * w + x1;
            float y = y2;
            elemFrame.appendChild(svg.createLine(x, y2, x, y2 - 5, null));
            elemFrame.appendChild(svg.createLine(x, y1, x, y2, gridDash));
            x += halfWidth;
            y += svg.getTextSize();
            Date date = (Date) xRange.getItemAt(i / numXLabels);
            String label = xFormat.format(date);
            Element element = svg.createText(x, y, label);
            element.setAttribute("transform", "rotate(" + xLabelAngle + " " + x + "," + y + ")");
            elemFrame.appendChild(element);
        }

        //draw y labels
        svg.setTextAlignment(SVGDocument.TextAlignment.RIGHT);
        if (scale == Scale.LOG) {
            double power = 1;

            while (power < max) {
                float y = (float) (Math.log(power) / Math.log(max));
                y = y2 - y * h;
                elemFrame.appendChild(svg.createLine(x1, y, x1 + 5, y, null));
                power *= 10;
                elemFrame.appendChild(svg.createLine(x1, y, x2, y, gridDash));
                String Label = yFormat.format(power);
                elemFrame.appendChild(svg.createText(x1 - svg.getTextSize(), y, Label));
            }
        }

        if (scale == Scale.LINEAR) {
            for (double i = 1; i < numYLabels; i++) {
                double curValue = i / numYLabels * max;
                float y = y2 - (float) (curValue / (max - min)) * h;
                elemFrame.appendChild(svg.createLine(x1, y, x1 + 5, y, null));
                elemFrame.appendChild(svg.createLine(x1, y, x2, y, gridDash));
                String Label = yFormat.format(curValue);
                elemFrame.appendChild(svg.createText(x1 - svg.getTextSize(), y, Label));
            }
        }
    }

    /**
     * concrete implementation of rendering data specified by ParamColumnRenderDescriptor
     * @throws PlotException
     */
    @Override
    protected void renderData() throws PlotException {
        if (tupleLists.isEmpty()) {
            return;
        }

        if (names == null) {
            ParamDaoID daoId = plotRequest.getParameter(ParamDaoID.class);
            if (daoId != null) {
                names = daoId.getDaoID().split(":");
            }
        }

        int numRows = tupleLists.get(0).size();
        int numCols = tupleLists.size();
        int col = 0;
        float y2 = height - margin * 2;
        float barWidth = (width - margin * 4) / numRows / numCols;

        double max = yRange.getMaximum().doubleValue();
        double min = yRange.getMinimum().doubleValue();
        float h = height - margin * 4;

        for (TupleList<TupleDateNumber> tupleList : tupleLists) {
            int row = 0;
            float py = 0;
            double statsMin = Double.POSITIVE_INFINITY;
            double statsMax = Double.NEGATIVE_INFINITY;
            double statsSum = 0;
            double statsSS = 0;
            for (TupleDateNumber tuple : tupleList.getTuples()) {
                float x = (float) (row) / numRows * (width - margin * 4) + margin * 2;
                double curValue = tuple.getYValue().doubleValue();

                statsSum += curValue;
                statsSS += curValue * curValue;

                if (curValue > statsMax) {
                    statsMax = curValue;
                }

                if (curValue < statsMin) {
                    statsMin = curValue;
                }

                float y = 0;
                if (scale == Scale.LOG) {
                    y = (float) (Math.log(curValue + 1) / Math.log(max + 1) * h);
                    y = y2 - y;
                } else {
                    y = (float) ((curValue - min) / (max - min) * h);
                    y = y2 - y;
                }

                switch (renderStyle) {
                    case BARS:
                        svg.setFillColor(colors[col]);
                        svg.setOpacity(0.8f);
                        if (barWidth < 2) {
                            svg.setStroke(colors[col]);
                        } else {
                            svg.setStroke(SVGColor.BLACK);
                        }
                        elemFrame.appendChild(
                                svg.createRectangle(x + col * barWidth, y, barWidth, y2 - y));
                        break;
                    case LINES:
                        svg.setStrokeColor(colors[col]);
                        svg.setStrokeWidth(1);
                        if (row > 0) {
                            elemFrame.appendChild(
                                    svg.createLine(x, py, x, y, null));
                        }
                        elemFrame.appendChild(svg.createLine(x, y,
                                x + barWidth * numCols, y, null));
                        break;
                }
                row++;
                py = y;
                //System.out.println(curValue +" " + max);
            }

            //plot stats
            svg.setFillColor(colors[col]);
            svg.setStroke(SVGColor.BLACK);
            svg.setTextAlignment(SVGDocument.TextAlignment.LEFT);
            float y = margin * 2 - svg.getTextSize() * 1.5f * col - 10;
            elemFrame.appendChild(svg.createRectangle(margin * 2, y - 10, 20, svg.getTextSize()));
            svg.setFillColor(SVGColor.BLACK);
            if (names != null && names.length > col) {
                elemFrame.appendChild(svg.createText(margin * 2 + 30, y, names[col]));
            }

            elemFrame.appendChild(svg.createText(margin * 2 + 30 + svg.getTextSize() * 10,
                    y, "min: " + yFormat.format(statsMin)));

            double mean = statsSum / numRows;
            elemFrame.appendChild(svg.createText(margin * 2 + 30 + svg.getTextSize() * 20,
                    y, "avg: " + yFormat.format(mean)));

            elemFrame.appendChild(svg.createText(margin * 2 + 30 + svg.getTextSize() * 30,
                    y, "max: " + yFormat.format(statsMax)));

            double std = Math.sqrt(statsSS / numRows - mean * mean);
            elemFrame.appendChild(svg.createText(margin * 2 + 30 + svg.getTextSize() * 40,
                    y, "std: " + yFormat.format(Math.sqrt(std))));
            col++;
        }
    }
}

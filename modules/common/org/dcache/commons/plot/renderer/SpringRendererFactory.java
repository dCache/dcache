package org.dcache.commons.plot.renderer;

import org.dcache.commons.plot.PlotException;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;

/**
 *
 * @author timur and tao
 */
public class SpringRendererFactory extends PlotRendererFactory {

    private String xmlBeanFile = System.getProperty(
            "org.dcache.commons.plot.renderer.SpringRendererFactory",
            "org/dcache/commons/plot/PlotConfiguration.xml");
    private XmlBeanFactory beanFactory = new XmlBeanFactory(new ClassPathResource(xmlBeanFile));;

    public String getXmlBeanFile() {
        return xmlBeanFile;
    }

    public void setXmlBeanFile(String xmlBeanFile) throws PlotException {
        this.xmlBeanFile = xmlBeanFile;
        try {
            beanFactory = new XmlBeanFactory(new ClassPathResource(xmlBeanFile));
        } catch (Exception e) {
            throw new PlotException("failed in setting xml file: " + e, e);
        }
    }

    @Override
    public Renderer getPlotRenderer(PlotOutputType plotOutputType) {
        return (Renderer) beanFactory.getBean(plotOutputType.toString(),
                Renderer.class);
    }

}

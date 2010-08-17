package org.dcache.commons.plot.renderer;

import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;

/**
 *
 * @author timur and tao
 */
public class SpringRendererFactory extends PlotRendererFactory {

    private String xmlBeanFile = System.getProperty(
            "org.dcache.commons.plot.renderer.SpringRendererFactory",
            System.getProperty("dcache.home", "/opt/d-cache") + "/etc/PlotConfiguration.xml");
    private XmlBeanFactory beanFactory = null;

    public String getXmlBeanFile() {
        return xmlBeanFile;
    }

    public void setXmlBeanFile(String xmlBeanFile){
        this.xmlBeanFile = xmlBeanFile;
    }

    @Override
    public Renderer getPlotRenderer(PlotOutputType plotOutputType) {
        if (beanFactory == null) {
            beanFactory = new XmlBeanFactory(new FileSystemResource(xmlBeanFile));
        }
        return (Renderer) beanFactory.getBean(plotOutputType.toString(),
                Renderer.class);
    }

}

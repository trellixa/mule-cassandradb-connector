
package org.mule.modules.cassandradb.generated.config;

import javax.annotation.Generated;
import org.mule.common.MuleVersion;
import org.mule.config.MuleManifest;
import org.mule.modules.cassandradb.generated.processors.AddNewColumnMessageProcessor;
import org.mule.modules.cassandradb.generated.processors.AddNewColumnMessageProcessorDebuggable;
import org.mule.security.oauth.config.AbstractDevkitBasedDefinitionParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.parsing.BeanDefinitionParsingException;
import org.springframework.beans.factory.parsing.Location;
import org.springframework.beans.factory.parsing.Problem;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.9.1", date = "2017-05-30T02:13:49-03:00", comments = "Build UNNAMED.2797.33dc1d7")
public class AddNewColumnDefinitionParser
    extends AbstractDevkitBasedDefinitionParser
{

    private static Logger logger = LoggerFactory.getLogger(AddNewColumnDefinitionParser.class);

    private BeanDefinitionBuilder getBeanDefinitionBuilder(ParserContext parserContext) {
        try {
            MuleVersion muleVersion = new MuleVersion(MuleManifest.getProductVersion());
            BeanDefinitionBuilder beanDefinitionBuilder;
            if (muleVersion.atLeastBase("3.8.0")) {
                beanDefinitionBuilder = beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(AddNewColumnMessageProcessorDebuggable.class.getName());
            } else {
                beanDefinitionBuilder = beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(AddNewColumnMessageProcessor.class.getName());
            }
            return beanDefinitionBuilder;
        } catch (NoClassDefFoundError noClassDefFoundError) {
            String muleVersion = "";
            try {
                muleVersion = MuleManifest.getProductVersion();
            } catch (Exception _x) {
                logger.error("Problem while reading mule version");
            }
            logger.error(("Cannot launch the mule app, the @Processor [add-new-column] within the connector [cassandradb] is not supported in mule "+ muleVersion));
            throw new BeanDefinitionParsingException(new Problem(("Cannot launch the mule app, the @Processor [add-new-column] within the connector [cassandradb] is not supported in mule "+ muleVersion), new Location(parserContext.getReaderContext().getResource()), null, noClassDefFoundError));
        }
    }

    public BeanDefinition parse(Element element, ParserContext parserContext) {
        BeanDefinitionBuilder builder = getBeanDefinitionBuilder(parserContext);
        builder.addConstructorArgValue("addNewColumn");
        builder.setScope(BeanDefinition.SCOPE_PROTOTYPE);
        if (!hasAttribute(element, "config-ref")) {
            throw new BeanDefinitionParsingException(new Problem("It seems that the config-ref for @Processor [add-new-column] within the connector [cassandradb] is null or missing. Please, fill the value with the correct global element.", new Location(parserContext.getReaderContext().getResource()), null));
        }
        parseConfigRef(element, builder);
        parseProperty(builder, element, "table", "table");
        parseProperty(builder, element, "keyspaceName", "keyspaceName");
        if (hasAttribute(element, "input-ref")) {
            if (element.getAttribute("input-ref").startsWith("#")) {
                builder.addPropertyValue("input", element.getAttribute("input-ref"));
            } else {
                builder.addPropertyValue("input", (("#[registry:"+ element.getAttribute("input-ref"))+"]"));
            }
        }
        BeanDefinition definition = builder.getBeanDefinition();
        setNoRecurseOnDefinition(definition);
        attachProcessorDefinition(parserContext, definition);
        return definition;
    }

}


package org.mule.modules.cassandradb.generated.config;

import javax.annotation.Generated;
import org.mule.config.MuleManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.factory.xml.NamespaceHandlerSupport;


/**
 * Registers bean definitions parsers for handling elements in <code>http://www.mulesoft.org/schema/mule/cassandradb</code>.
 * 
 */
@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.9.1", date = "2017-05-30T02:13:49-03:00", comments = "Build UNNAMED.2797.33dc1d7")
public class CassandradbNamespaceHandler
    extends NamespaceHandlerSupport
{

    private static Logger logger = LoggerFactory.getLogger(CassandradbNamespaceHandler.class);

    private void handleException(String beanName, String beanScope, NoClassDefFoundError noClassDefFoundError) {
        String muleVersion = "";
        try {
            muleVersion = MuleManifest.getProductVersion();
        } catch (Exception _x) {
            logger.error("Problem while reading mule version");
        }
        logger.error(((((("Cannot launch the mule app, the  "+ beanScope)+" [")+ beanName)+"] within the connector [cassandradb] is not supported in mule ")+ muleVersion));
        throw new FatalBeanException(((((("Cannot launch the mule app, the  "+ beanScope)+" [")+ beanName)+"] within the connector [cassandradb] is not supported in mule ")+ muleVersion), noClassDefFoundError);
    }

    /**
     * Invoked by the {@link DefaultBeanDefinitionDocumentReader} after construction but before any custom elements are parsed. 
     * @see NamespaceHandlerSupport#registerBeanDefinitionParser(String, BeanDefinitionParser)
     * 
     */
    public void init() {
        try {
            this.registerBeanDefinitionParser("config", new CassandraDBConnectorBasicAuthConnectionStrategyConfigDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("config", "@Config", ex);
        }
        try {
            this.registerBeanDefinitionParser("create-keyspace", new CreateKeyspaceDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("create-keyspace", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("drop-keyspace", new DropKeyspaceDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("drop-keyspace", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("create-table", new CreateTableDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("create-table", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("drop-table", new DropTableDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("drop-table", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("execute-c-q-l-query", new ExecuteCQLQueryDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("execute-c-q-l-query", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("insert", new InsertDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("insert", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("update", new UpdateDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("update", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("delete-columns-value", new DeleteColumnsValueDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("delete-columns-value", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("delete-rows", new DeleteRowsDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("delete-rows", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("select", new SelectDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("select", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("get-table-names-from-keyspace", new GetTableNamesFromKeyspaceDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("get-table-names-from-keyspace", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("change-column-type", new ChangeColumnTypeDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("change-column-type", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("add-new-column", new AddNewColumnDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("add-new-column", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("drop-column", new DropColumnDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("drop-column", "@Processor", ex);
        }
        try {
            this.registerBeanDefinitionParser("rename-column", new RenameColumnDefinitionParser());
        } catch (NoClassDefFoundError ex) {
            handleException("rename-column", "@Processor", ex);
        }
    }

}


package org.mule.modules.cassandradb.generated.processors;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Generated;
import org.mule.api.MuleContext;
import org.mule.api.MuleEvent;
import org.mule.api.debug.DebugInfoProvider;
import org.mule.api.debug.FieldDebugInfo;
import org.mule.api.debug.FieldDebugInfoFactory;
import org.mule.api.debug.ObjectFieldDebugInfo;
import org.mule.api.transformer.TransformerException;
import org.mule.api.transformer.TransformerMessagingException;
import org.mule.common.DefaultResult;
import org.mule.common.Result;
import org.mule.common.metadata.NativeQueryMetadataTranslator;
import org.mule.common.query.dsql.parser.MuleDsqlParser;
import org.mule.devkit.internal.dsql.DsqlMelParserUtils;
import org.mule.modules.cassandradb.CassandraDBConnector;
import org.mule.util.ClassUtils;
import org.mule.util.TemplateParser;

@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.9.1", date = "2017-05-30T02:13:49-03:00", comments = "Build UNNAMED.2797.33dc1d7")
public class SelectMessageProcessorDebuggable
    extends SelectMessageProcessor
    implements DebugInfoProvider
{


    public SelectMessageProcessorDebuggable(String operationName) {
        super(operationName);
    }

    private boolean isConsumable(Object evaluate) {
        return (ClassUtils.isConsumable(evaluate.getClass())||Iterator.class.isAssignableFrom(evaluate.getClass()));
    }

    private Object getEvaluatedValue(MuleContext muleContext, MuleEvent muleEvent, String fieldName, Object field)
        throws NoSuchFieldException, TransformerException, TransformerMessagingException
    {
        Object evaluate = null;
        if (!(field == null)) {
            evaluate = this.evaluate(TemplateParser.createMuleStyleParser().getStyle(), muleContext.getExpressionManager(), muleEvent.getMessage(), field);
            Type genericType = this.getClass().getSuperclass().getDeclaredField(fieldName).getGenericType();
            if (!isConsumable(evaluate)) {
                evaluate = this.evaluateAndTransform(muleContext, muleEvent, genericType, null, field);
            }
        }
        return evaluate;
    }

    private FieldDebugInfo createDevKitFieldDebugInfo(String name, String friendlyName, Class type, Object value, MuleEvent muleEvent) {
        try {
            return FieldDebugInfoFactory.createFieldDebugInfo(friendlyName, type, getEvaluatedValue(muleContext, muleEvent, ("_"+name+"Type"), value));
        } catch (NoSuchFieldException e) {
            return FieldDebugInfoFactory.createFieldDebugInfo(friendlyName, type, e);
        } catch (TransformerMessagingException e) {
            return FieldDebugInfoFactory.createFieldDebugInfo(friendlyName, type, e);
        } catch (TransformerException e) {
            return FieldDebugInfoFactory.createFieldDebugInfo(friendlyName, type, e);
        }
    }

    @Override
    public List<FieldDebugInfo<?>> getDebugInfo(MuleEvent muleEvent) {
        List<FieldDebugInfo<?>> fieldDebugInfoList = new ArrayList<FieldDebugInfo<?>>();
        fieldDebugInfoList.add(getQueryDebugInfo(muleEvent, "Query", "query", (java.lang.String.class), query));
        fieldDebugInfoList.add(createDevKitFieldDebugInfo("parameters", "Parameters", (java.util.List.class), parameters, muleEvent));
        return fieldDebugInfoList;
    }

    private ObjectFieldDebugInfo getQueryDebugInfo(MuleEvent muleEvent, String name, String friendlyName, Class clazz, Object value) {
        List<FieldDebugInfo<?>> fieldDebugInfoList = new ArrayList<FieldDebugInfo<?>>();
        DsqlMelParserUtils dsqlQueryParser = new DsqlMelParserUtils();
        if (value.toString().startsWith("dsql:")) {
            String parsedDsql = ((String) dsqlQueryParser.parseDsql((getMuleContext()), muleEvent, value));
            String queryString = parsedDsql.substring(5);
            fieldDebugInfoList.add(FieldDebugInfoFactory.createFieldDebugInfo("DSQL", String.class, queryString));
            try {
                Result<String> translatedQuery = null;
                Object connector = findOrCreate(null, true, muleEvent);
                if (connector instanceof NativeQueryMetadataTranslator) {
                    NativeQueryMetadataTranslator translator = ((NativeQueryMetadataTranslator) connector);
                    translatedQuery = translator.toNativeQuery(new MuleDsqlParser().parse(queryString));
                } else {
                    CassandraDBConnector translator = ((CassandraDBConnector) connector);
                    translatedQuery = new DefaultResult(translator.toNativeQuery(new MuleDsqlParser().parse(queryString)));
                }
                fieldDebugInfoList.add(FieldDebugInfoFactory.createFieldDebugInfo("Native Query", String.class, translatedQuery.get()));
            } catch (Exception e) {
                fieldDebugInfoList.add(FieldDebugInfoFactory.createFieldDebugInfo("Native Query", String.class, e));
            }
        } else {
            try {
                fieldDebugInfoList.add(FieldDebugInfoFactory.createFieldDebugInfo("Native Query", String.class, getEvaluatedValue(muleContext, muleEvent, friendlyName, value)));
            } catch (Exception e) {
                fieldDebugInfoList.add(FieldDebugInfoFactory.createFieldDebugInfo("Native Query", String.class, e));
            }
        }
        return FieldDebugInfoFactory.createFieldDebugInfo(name, String.class, fieldDebugInfoList);
    }

}

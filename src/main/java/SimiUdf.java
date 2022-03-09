import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.xm.Similarity;
import org.xm.similarity.text.CosineSimilarity;
import org.xm.similarity.text.TextSimilarity;
import org.apache.log4j.Logger;

/**
 * Description:
 *
 * @author zz
 * @date 2022/2/18
 */
@Description(name = "simi_udf",
        value = "_FUNC_(text1,text2) - Returns two words similarity value",
        extended = "Example:\n"
                + " > SELECT _FUNC_('aa','bb'); \n")
public class SimiUdf extends GenericUDF {
    private static Logger logger = Logger.getLogger(SimiUdf.class);
    private static TextSimilarity similarity = new CosineSimilarity();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentException("The operator 'simi_udf' accepts 2 arguments.");
        }
        ObjectInspector returnType = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
        return returnType;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object text1 = arguments[0].get();
        Object text2 = arguments[1].get();
        if (text1== null | text2 == null) {
            return 0;
        }
        try {
            return similarity.getSimilarity(String.valueOf(text1), String.valueOf(text2));
        } catch (Exception e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("返回 " + children[0] + "," + children[1] + "相似度分值")
                .append("\n")
                .append("Usage: simi_udf(text1,text2)")
                .append("\n")
                .append("相似度范围[0,1]");
        return sb.toString();
    }
}

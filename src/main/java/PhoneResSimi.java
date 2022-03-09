import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;

/**
 * Description:
 *
 * @author zz
 * @date 2022/2/21
 */

@Description(name = "phone_res_simi",
        value = "_FUNC_(text1,text2) - Returns two phone number similarity value",
        extended = "Example:\n"
                + " > SELECT _FUNC_('123','234'); \n")
public class PhoneResSimi extends GenericUDF {

    private static Logger logger = Logger.getLogger(SimiUdf.class);

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentException("The operator 'phone_res_simi' accepts 2 arguments.");
        }
        ObjectInspector returnType = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
        return returnType;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String text1 = String.valueOf(arguments[0].get());
        String text2 = String.valueOf(arguments[1].get());
        if (text1 == null | text2 == null) {
            return 0;
        }
        try {
            int lenMin = Math.min(text1.length(), text2.length());
            return getSimilarityRatio(new StringBuilder(text1).reverse().toString(), new StringBuilder(text2).reverse().toString().substring(0,lenMin));
        } catch (Exception e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("返回 " + children[0] + "," + children[1] + "反转后相似度分值")
                .append("\n")
                .append("Usage: phone_res_simi(text1,text2)")
                .append("\n")
                .append("相似度范围[0,1]");
        return sb.toString();
    }

    /**
     * 比较两个字符串的相识度
     * <p>
     * 核心算法：用一个二维数组记录每个字符串是否相同，如果相同记为0，不相同记为1，每行每列相同个数累加
     * <p>
     * 则数组最后一个数为不相同的总数，从而判断这两个字符的相识度
     *
     * @param str
     * @param target
     * @return
     */

    private static int compare(String str, String target) {

        int d[][]; // 矩阵

        int n = str.length();

        int m = target.length();

        int i; // 遍历str的

        int j; // 遍历target的

        char ch1; // str的

        char ch2; // target的

        int temp; // 记录相同字符,在某个矩阵位置值的增量,不是0就是1
        if (n == 0) {
            return m;
        }
        if (m == 0) {
            return n;
        }
        d = new int[n + 1][m + 1];
        // 初始化第一列
        for (i = 0; i <= n; i++) {
            d[i][0] = i;
        }
        // 初始化第一行

        for (j = 0; j <= m; j++) {
            d[0][j] = j;

        }
        for (i = 1; i <= n; i++) {

            // 遍历str
            ch1 = str.charAt(i - 1);
            // 去匹配target

            for (j = 1; j <= m; j++) {
                ch2 = target.charAt(j - 1);
                if (ch1 == ch2 || ch1 == ch2 + 32 || ch1 + 32 == ch2) {
                    temp = 0;

                } else {
                    temp = 1;
                }
                // 左边+1,上边+1, 左上角+temp取最小
                d[i][j] = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + temp);
            }
        }

        return d[n][m];
    }

    /**
     * 获取最小的值
     */

    private static int min(int one, int two, int three) {
        return (one = one < two ? one : two) < three ? one : three;

    }

    /**
     * 获取两字符串的相似度
     */

    public static float getSimilarityRatio(String str, String target) {
        int max = Math.max(str.length(), target.length());
        return 1 - (float) compare(str, target) / max;

    }
}

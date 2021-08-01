package com.atguigu.hive.udtf;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.Collections;
import java.util.List;

public class ExplodeJSONArray extends GenericUDTF {


    /**
     * 输入输出检查, 输入参数个数为1, 类型为String, 输出结果个数为1
     *
     * @param argOIs 输入参数的类型检查器
     * @return 输出参数的类型检查器
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        //1. 检查参数个数
        if (argOIs.length != 1) {
            throw new UDFArgumentLengthException("参数个数只能为1个!");
        }

        //2. 检查参数类型
        if (!argOIs[0].getTypeName().equals("string")) {
            throw new UDFArgumentTypeException(0, "参数类型必须为string!");
        }


        //3. 确定输出结果类型

        List<String> fieldsNames = Collections.singletonList("Json");
        List<ObjectInspector> types = Collections.singletonList(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector
        );
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldsNames, types);


    }

    /**
     * 实际处理逻辑
     *
     * @param args 输入的Json字符串
     * @throws HiveException
     */
    @Override
    public void process(Object[] args) throws HiveException {
        //把字符串包装为JsonArray
        String line = args[0].toString();
        JSONArray array = new JSONArray(line);

        //遍历这个JsonArray
        for (int i = 0; i < array.length(); i++) {
            String output = array.getString(i);
            Object[] row = new Object[1];
            row[0] = output;
            forward(row);
        }

    }

    @Override
    public void close() throws HiveException {

    }
}

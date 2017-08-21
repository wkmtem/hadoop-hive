package com.compass.hive.udf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 
 * @Class Name: AreaUDF
 * @Description: 自定义函数，打成jar包，注册这个udf
 * @author: wkm
 * @Company: www.compass.com
 * @Create date: 2017年7月19日下午8:53:22
 * @version: 2.0
 */
public class AreaUDF extends UDF{

	private static Map<String, String> areaMap = new HashMap<String, String>();
	
	// 转换字段
	static {
		areaMap.put("china", "中国");
		areaMap.put("japan", "日本");
		areaMap.put("usa", "美国");
	}
	
	// 自定义函数必须叫eavluate, 底层跑的是mapreduce，返回值Text，需序列化后在网络中传输
	public Text evaluate(Text in){ // 形参：map的key
		String result = areaMap.get(in.toString());// 不存在的值toString，则返回null
		if(result == null){
			result = "无对应的国家";
		}
		return new Text(result);
	}
}

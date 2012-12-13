package org.ochilab.osimpleaw;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.aw20.commons.amazon.SimpleDB;

/**
 * net.aw20.commons.amazon.SimpleDBをラップしたSimpleDBのラッパー。
 * @author Y.OCHI, Y.Takubo (Ochi Lab)
 *
 */
public class OSimpleDB {

	private SimpleDB sdb;
	private String prefix;

	public OSimpleDB(String awsId, String secretKey, String prefix) {// コンストラクター
		String url = "sdb.amazonaws.com";
		sdb = new SimpleDB(url, awsId, secretKey);
		this.prefix = prefix;
	}
	public OSimpleDB(String awsId, String secretKey) {// コンストラクター
		String url = "sdb.amazonaws.com";
		sdb = new SimpleDB(url, awsId, secretKey);
		this.prefix = "";
	}

	public OSimpleDB() {

	}

	public void put(Object o) {

		HashMap<String, String> map = this.mapping(o);
		try {
			String itemName = map.get("itemName");
			map.remove("itemName");
			sdb.putAttributes(prefix + o.getClass().getSimpleName(), itemName,
					map);
			System.out.println(o.getClass().getSimpleName());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public <T> void persist(T object) {//入力されたオブジェクトを永続化する
		@SuppressWarnings("unchecked")
		Class<T> clazz=(Class<T>) object.getClass();
		String domainName = prefix+clazz.getSimpleName();
		Map<String, Method> fieldMethodMap = getFieldMethodMap(clazz);//フィールド名、getterMethodのマップを取得
		Map<String, String>    fieldFieldvalueMap = new HashMap<String, String>();// フィールド,フィールド値Map		ココで配列を格納することを諦めている
		Map<String, String[]> fieldFieldvalueArrayMap = new HashMap<String, String[]>();// フィールド,フィールド値Map		ココで配列を格納することを諦めている
		
		for (Map.Entry<String, Method> pare : fieldMethodMap.entrySet()) {//フィールド名、getterMethodのマップを１ペアずつ処理してく
			try {
				Method method = pare.getValue();//getterMethod取得
	
				if(method.invoke(object).getClass().toString().equals("class [Ljava.lang.String;")){//配列やったら、ココに入れて
					fieldFieldvalueArrayMap.put(pare.getKey(),(String[]) method.invoke(object));// フィールド,フィールド値(配列)Map	
				}else{
					fieldFieldvalueMap.put(pare.getKey(), method.invoke(object).toString());// フィールド,フィールド値Map	作成
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}// fieldFieldvalueMap(フィールドフィールド値マップ)が完成
		///////////////////////////////////////////////////////////////////////////////////////////////////////////
		List<Map<String,String>> mapList=new ArrayList<Map<String,String>>();
		for (Map.Entry<String, String[]> fieldname_valueS_entry : fieldFieldvalueArrayMap.entrySet()) {
			String key=fieldname_valueS_entry.getKey();
			String[] values=fieldname_valueS_entry.getValue();
			
			for(int i=0;i<values.length;i++){
				Map<String, String> map=new HashMap<String, String>();
				map.put(key, values[i]);
				mapList.add(map);
			}
		}
		///////////////////////////////////////////////////////////////////////////////////////////////////////////
		try {
			sdb.putAttributes(domainName, fieldFieldvalueMap.get("itemName"), fieldFieldvalueMap);// フィールド,フィールド値Map格納
			for(int i=0;i<mapList.size();i++){
				sdb.putAttributes(domainName, fieldFieldvalueMap.get("itemName"), mapList.get(i));
			}
		} catch (Exception er) {
			er.printStackTrace();
		}
	}
	
	
	
	public void delete(Class<?> c, String itemName) {
		try {
			
			sdb.deleteAttributes(c.newInstance().getClass().getSimpleName(), itemName);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	
	/**
	 * 
	 * @param c
	 * @param query
	 * @return
	 */
	/*public List select(Class<?> c, String query) {
		List list = new ArrayList();
		try {
			List<Map> result = sdb.select(query);
			for(Map<String,String[]> map :result ){
				Object obj = this.mapToObject(c.newInstance().getClass(), map);
				list.add(obj);
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}finally{
			return list;
		}

	}*/
	
	/**
	 * オブジェクトのリストが返るように仕様変更
	 * @param clazz
	 * @param sql
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <T> List<T> select(Class<T> clazz, String sql){//overrideしているよ
		List<Map<String, String[]>> selectedList=null;
		try {
			selectedList = sdb.select(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return mapToObject(clazz, selectedList);
	}
	
	
/*	public List select(Object o, String query) {
		List list = new ArrayList();
		try {
			List<Map> result = sdb.select(query);
			for(Map<String,String[]> map :result ){
				Object obj = this.mapToObject(o, map);
				list.add(obj);
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}finally{
			return list;
		}

	}
	*/
	
	public Object get(Class<?> c, String itemName) {
		Map result;
		Object obj=null; ;
		try {
			
			result = sdb.getAttributes(c.newInstance().getClass().getSimpleName(), itemName);
			System.out.println("name="+((String[])result.get("name"))[0]);
			obj = this.mapToObject(c, result);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Error(get):"+e.getMessage());
		}finally{
			return obj;
		}

	}
	/*public Object get(Object o, String itemName) {
		Map result;
		Object obj=null; ;
		try {
			
			result = sdb.getAttributes(o.getClass().getSimpleName(), itemName);
			System.out.println("name="+((String[])result.get("name"))[0]);
			obj = this.mapToObject(o, result);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Error(get):"+e.getMessage());
		}finally{
			return obj;
		}

	}*/


	private HashMap<String, String> mapping(Object o) {

		Object instance = o;
		Class<?> c = instance.getClass();// そのオブジェクト内の調査準備
		Method[] methods = c.getMethods();// そのオブジェクト内のメソッドを全部取得
		HashMap<String, Method> fMethodMap = getFieldMNames(methods);// Method[]から、Map<フィールド名,
																		// Method>を取得

		// HashMap<String, String> fieldNameFieldValueMap =new
		// HashMap<String, String>();//ココに宣言したらバグる
		List<HashMap<String, String>> returnMapList = new ArrayList<HashMap<String, String>>();// 最後に返すマップリスト、Map<フィールド名,
																								// フィールドの値>のList
		HashMap<String, String> ffMap = new HashMap<String, String>();// 一人ずつのデータのために毎回HashMapをニュー
		for (Map.Entry<String, Method> map : fMethodMap.entrySet()) {
			try {
				Method method = map.getValue();
				ffMap.put(map.getKey(), method.invoke(o).toString());
			} catch (Exception er) {
				er.printStackTrace();
			}
		}
		return ffMap;
	}

	/**
	 * 
	 * @param list
	 * @return
	 */
	private List<HashMap<String, String>> mapping(List<Object> list) {

		if (list.size() < 1) {// まず初めにlistの要素数が１以上であるかどうかチェック！
			return null;
		} else {
			Object instance = list.get(0);// とりあえずlistに格納されている一個目のオブジェクトを取得する。
			Class<?> c = instance.getClass();// そのオブジェクト内の調査準備
			Method[] methods = c.getMethods();// そのオブジェクト内のメソッドを全部取得
			HashMap<String, Method> fMethodMap = getFieldMNames(methods);// Method[]から、Map<フィールド名,
																			// Method>を取得

			// HashMap<String, String> fieldNameFieldValueMap =new
			// HashMap<String, String>();//ココに宣言したらバグる
			List<HashMap<String, String>> returnMapList = new ArrayList<HashMap<String, String>>();// 最後に返すマップリスト、Map<フィールド名,
																									// フィールドの値>のList

			for (int i = 0; i < list.size(); i++) {// オブジェクトの数だけ繰り返す。
				Object object = list.get(i);
				HashMap<String, String> ffMap = new HashMap<String, String>();// 一人ずつのデータのために毎回HashMapをニュー
																				// 無駄な気がするけどこう書かなければ。
				for (Map.Entry<String, Method> map : fMethodMap.entrySet()) {
					try {
						Method method = map.getValue();
						ffMap.put(map.getKey(), method.invoke(object)
								.toString());
					} catch (Exception er) {
						er.printStackTrace();
					}
				}
				returnMapList.add(ffMap);
			}
			return returnMapList;
		}
	}

	private HashMap<String, Method> getFieldMNames(Method[] methods) {// Method[]
																		// から、フィールド名とゲッターメソッドをゲットしてマップにまとめて返すよ。
		String methodName, fieldName;
		HashMap<String, Method> map = new HashMap<String, Method>();
		for (int i = 0; i < methods.length; i++) {
			methodName = methods[i].getName();
			if (methodName.startsWith("get") && (methodName != "getClass")) {
				fieldName = methodName.replaceAll("get", "");
				fieldName = fieldName.substring(0, 1).toLowerCase()
						+ fieldName.substring(1);// ゲッターメソッドが用意されているフィールド名を取得
				map.put(fieldName, methods[i]);// フィールド名、メソッド名のマップを取得
			}
		}
		return map;
	}

	/**
	 * リストに格納されたマップをオブジェクトのリストとして返す（データの一括処理）
	 * @param clazz
	 * @param inputList
	 * @return
	 */
	private <T> List<T> mapToObject(Class<T> clazz,List<Map<String, String[]>> inputList) {
		System.out.println("persist(list) is called ");
		int inputListSize = inputList.size();
		if( inputListSize==0){
			return null;
		}
		Map<String, String[]> dataMap = inputList.get(0);//今から一個目を調査していくぞ
		Object[] keys = dataMap.keySet().toArray();// DBから取り出したMapのキーを配列に格納
		int keyNumber = keys.length;//keyの数をGET
		
		String[] keyArray = new String[keyNumber];//keyの配列を作成
		Method[] methodArray = new Method[keyNumber];//Methodの配列を作成
		
		List<T> returnList = new ArrayList<T>();//リターン用のリストを作成
		
		for (int i = 0; i < keyNumber; i++) {// このfor文でキー値の配列に関連つけて、Method型の配列を作成
			keyArray[i] = (String) keys[i];//keyの名前(fieldの名前)をGET!
			String setterMethodName = "set" + keyArray[i].substring(0, 1).toUpperCase() + keyArray[i].substring(1);//setterメソッド名を取得
			try {
				methodArray[i] = clazz.getMethod(setterMethodName, new Class[] { String.class });//上で取得したsetterメソッド名を元にsetterメソッドを取得
			} //setterメソッド(String)を定義する時に使うぞ
			catch (NoSuchMethodException e) {
				try {
					methodArray[i] = clazz.getMethod(setterMethodName, new Class[] { String[].class });
				} //setterメソッド(String[])を定義する時に使うぞ
				catch (NoSuchMethodException e1) {
					e1.printStackTrace();
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		for (int i = 0; i < inputListSize; i++) {// 各オブジェクトごとに繰り返す。
			dataMap = inputList.get(i);// 入力されたListからMap<String, String[]>を取り出す
			try {
				T createdInstance = (T) clazz.newInstance();// とりあえず、型に合ったインスタンスを作成
				for (int j = 0; j < keyNumber; j++) {// 各キーごとに繰り返す。このfor文でオブジェクトが一つ完成だよ♪
					try {
						String[] setterMethodArgument = dataMap.get(keyArray[j]);//setterメソッドの引数をゲットしているぞ
						if(2<=setterMethodArgument.length){//valueの型をチェック配列の要素数が２以上ならば
							methodArray[j].invoke(createdInstance, new Object[] {setterMethodArgument });
						}else{//valueの型をチェック配列の要素数が１ならば
							methodArray[j].invoke(createdInstance, setterMethodArgument[0]);//０個だけ入れる
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				returnList.add(createdInstance);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		return returnList;
	}

	/**
	 * mapをオブジェクトとして返す
	 * @param c
	 * @param map
	 * @return
	 */
	private Object mapToObject(Class<?> c, Map<String, String[]> map) {
		Class<?> clazz=null;
		Object obj=null;
		try {
		 obj=c.newInstance();
			clazz = obj.getClass();
			for (String key : map.keySet()) {// このfor文で一つのオブジェクトが完成！
				//System.out.println("key="+key);
				String setterMethodName = "set" + key.substring(0, 1).toUpperCase()
						+ key.substring(1);// mapのキー値（フィールド名）からセッターメソッド名を推測）
				String[] setterMethodArgument = (String[])map.get(key);// mapの中身から、セッターメソッドの引数を入手
				//System.out.println("setterMethodArgument="+setterMethodArgument.toString());
				try {
					Method method = clazz.getMethod(setterMethodName,
							new Class[] { String.class });// ↑の２値からセッターメソッド作成
					method.invoke(obj, setterMethodArgument);// セッターメソッド実行
				} catch (Exception e) {
					System.out.println("Error(mapToObject):"+e.getMessage());
				}
			}
			//System.out.println(clazz.toString());
		} catch (InstantiationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}finally{
			return obj;
		}
		
	}
	// ------------------------------------------privateメソッド
		private <T> Map<String, Method> getFieldMethodMap(Class<T> clazz) {
			Method[] allMethods = clazz.getMethods();//全メソッド取得
			String methodName, fieldName;
			HashMap<String, Method> fieldMethodMap = new HashMap<String, Method>();// フィールド名、getterMethodのマップをnew
			for (Method method : allMethods) {
				methodName = method.getName();
				if (methodName.startsWith("get") && (methodName != "getClass")) {
					fieldName = methodName.replaceAll("get", "");
					fieldName = fieldName.substring(0, 1).toLowerCase() + fieldName.substring(1);// ゲッターメソッドが用意されているフィールド名を取得
					fieldMethodMap.put(fieldName, method);// フィールド名、getterMethodのマップを取得
				}
			}
			return fieldMethodMap;
		}
}

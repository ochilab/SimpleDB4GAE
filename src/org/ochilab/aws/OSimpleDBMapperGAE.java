/**
* This library is based on simpledb.java of Alanwilliamson's Library. 
* You can use simpledb on GAE!
 * GAE対応のSimpleDB用 ORマッパー
 * @author Youji Ochi, Yuki Takubo
 *         http://www.ochi-lab.org/research/project/osimplemapper
 * 
**/

package org.ochilab.aws;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.eaio.uuid.UUID;

import net.aw20.commons.amazon.SimpleDB;

public class OSimpleDBMapperGAE {
	// ------------------------------------------field
	private SimpleDB _simpleDB = null;
	private  String _host; /*= "sdb.amazonaws.com";**/
	private  String _userName = "";
	private  String _awsId;
	private  String _secretKey;
	private Map<Class<?>,ClassData> _class_classData__Map=new HashMap<Class<?>,ClassData>();

//	// ------------------------------------------コンストラクター
//	public  OSimpleDBMapperGAE(String host,String awsId, String secretKey, String userName) {// 生成メソッド　コンストラクターの代わり
//		_host=host;
//		_awsId = awsId;
//		_secretKey = secretKey;
//		_userName = userName;
//		return SingletonHolderWithUserName.instance;
//	}
//	private  class SingletonHolderWithUserName {// 内部クラス
//		private  final IislabSimpleDBSingleton instance = new IislabSimpleDBSingleton(_host,_awsId, _secretKey, _userName);// 内部クラスのフィールド
	public IislabSimpleDBSingleton(String host, String awsId, String secretKey, String userName) {// コンストラクター
		_simpleDB = new SimpleDB(host, awsId, secretKey);
		_host=host;
		_awsId = awsId;
		_secretKey = secretKey;
		_userName = userName;
	}
	
	// ----------------------------------------------------------------------------------------------------------------------------------publicメソッド
	// ----------------------------------------------------------------------------------------------------------------------------------publicメソッド
	// 修正が容易
	public <T> void createDomain(Class<T> clazz) {// 「class名.class」を入力するとドメインを自動生成する。
		String domainName = _userName + clazz.getSimpleName();
		try {
			_simpleDB.createDomain(domainName);// ドメイン作成！（既に同名のドメインがあれば、そのままそこに書き込むよ）
		} catch (Exception e) {
			System.out.println("テーブルを作成できませんでした。");
			e.printStackTrace();
		}
	}

	// 修正が容易
	public <T> void deleteAttributes(Class<T> clazz, String ItemName) {
		String domainName = _userName + clazz.getSimpleName();
		try {
			_simpleDB.deleteAttributes(domainName, ItemName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 修正しなくてもOK?
	@SuppressWarnings("unchecked")
	// 本来ならMapのListがreturnされるけど、このメソッドのおかげで、objectのlistがreturnされる
	public <T> List<T> select(String sql, Class<T> clazz) {// overrideしているよ
		List<Map<String, String[]>> selectedList = null;
		try {
			selectedList = _simpleDB.select(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}
		//_simpleDB.getLastToken();
		return mapListToObjectList(selectedList, clazz);
	}
	@SuppressWarnings("unchecked")
	public <T> List<T> selectAll(String sql, Class<T> clazz) {// overrideしているよ
		List<Map<String, String[]>> selectedList = new ArrayList<Map<String,String[]>>();
		String lastToken=null;
		int i=1;
		try {
			while(true){
				System.out.println(i+"回目のselect");
				List<Map<String, String[]>> listGotBySQL=_simpleDB.select(sql,lastToken);
				System.out.println(listGotBySQL.size()+"個取得");
				selectedList.addAll(listGotBySQL);//合体
				if(lastToken!=null && lastToken.equals(_simpleDB.getLastToken())){//selectしても、前回と同じやったら抜ける
					break;
				}
				lastToken=_simpleDB.getLastToken();
				if(lastToken==null){//更新されてもnullなら、それは１００個未満だったんだよ　だから抜ける
					break;
				}
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return mapListToObjectList(selectedList, clazz);
	}

	@SuppressWarnings("unchecked")
	public  <T> T getAttributes(Class<T> clazz, String ItemName) {
		if(!_class_classData__Map.containsKey(clazz)){//オブジェクトのフィールド　メソッドマップが登録されているかどうかチェック
			add__classClassData_Map(clazz);//されていなかったら、このメソッドを使用して登録する。
		}else{
			System.out.println("_class_classData__Mapに登録済みだからgetも簡単！");
		}
		ClassData classData = _class_classData__Map.get(clazz);
		T createdInstance = null;
		Map<String, String[]> map = null;//simpleDBの標準フォーマットはMap<String, String[]>
		try {
			createdInstance = clazz.newInstance();// とりあえず、型に合ったインスタンスを作成
			map = _simpleDB.getAttributes(classData.getDomainName(), ItemName);
			// getAttributesメソッドで取得したmapには、itemNameキーが存在しない。そのため↓でputする。
			map.put("ItemName",new String[]{ ItemName });

			Map<String,Method> fieldName_setterMethodMap=classData.getFieldNameSetterMethod_Map();//createFieldName_setterMethodMap(clazz);
			
			for(String fieldName : fieldName_setterMethodMap.keySet()){
				Method setterMethod=fieldName_setterMethodMap.get(fieldName);
				Class<?>[] parameterTypes = setterMethod.getParameterTypes();
				if(parameterTypes[0].toString().equals("class java.lang.String")){
					String[] value=map.get(fieldName);//simpleDBに格納されているのは配列だからね。こういうめんどい処理がいるんだ。
					setterMethod.invoke(createdInstance, value[0]);
				}else{
					setterMethod.invoke(createdInstance, (Object)map.get(fieldName));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return createdInstance;
	}

	public <T> void put(T object) {// 入力されたオブジェクトを永続化する
		Class<? extends Object> clazz = object.getClass();
		if(!_class_classData__Map.containsKey(clazz)){//オブジェクトのフィールド　メソッドマップが登録されているかどうかチェック
			add__classClassData_Map(clazz);//されていなかったら、このメソッドで登録する。
		}else{
			System.out.println("_class_classData__Mapに登録済みだからputも簡単");
		}
		ClassData classData = _class_classData__Map.get(clazz);
		Map<String, Method> fieldNameGetterMethod_Map = classData.getFieldNameGetterMethod_Map();
		
		Map<String, String>    fieldNameFieldvalue_Map = new HashMap<String, String>();// フィールドname,フィールド値Map
		Map<String, String[]> fieldNameFieldvalueArray_Map = new HashMap<String, String[]>();// フィールドname,フィールド値Map
		
		for(String key:fieldNameGetterMethod_Map.keySet()){
			Method getterMethod=fieldNameGetterMethod_Map.get(key);
			String returnType=getterMethod.getGenericReturnType().toString();
			try{
				if(returnType.equals("class java.lang.String")){//ゲッターの戻り値がStringなら
					fieldNameFieldvalue_Map.put(check_IS_itemName(key), (String) getterMethod.invoke(object));// フィールド,フィールド値(配列)Map
				}else{//ゲッターの戻り値が配列なら
					fieldNameFieldvalueArray_Map.put(key, (String[]) getterMethod.invoke(object));// フィールド,フィールド値(配列)Map
				}
			}catch(Exception e){
				System.out.println("バグの原因になっているmethodの名前=" +getterMethod.getName());
			}
		}
		List<Map<String, String>> mapList = arrayMap_TO_mapList(fieldNameFieldvalueArray_Map);
		registerDataWithSimpaleDB(classData.getDomainName(), fieldNameFieldvalue_Map, mapList);
	}
	private String check_IS_itemName(String word){
		if(word.equals("itemName")){
			word="ItemName";
		}
		return word;
	}
	// ----------------------------------------------------------------------------privateメソッド
	// ----------------------------------------------------------------------------privateメソッド
	// ----------------------------------------------------------------------------privateメソッド

	private void add__classClassData_Map(Class<?> clazz){
		String className=clazz.getSimpleName();
		System.out.println(className+"は_class_classData__Mapにまだ登録されていないので、登録します。");
		ClassData cd=new ClassData();
		String domainName = _userName +className; 
		cd.setDomainName(domainName);
		////////////////////////////////////////////////fieldNameFieldvalueArray_Map,fieldNameFieldvalue_Mapを完成させるブロック
		Method[] declaredMethods = clazz.getDeclaredMethods();
		Map<String,Method> fieldNameGetterMethod_Map = new HashMap<String, Method>();// フィールドname,GetterMethodMap
		Map<String,Method> fieldNameSetterMethod_Map = new HashMap<String, Method>();
		for (Method method : declaredMethods) {// このクラスで宣言された全てのメソッド　getClassは含まない
			String methodName = method.getName();
			if (methodName.matches("^get[\\dA-Z]\\w*")) {// getterメソッドの場合
				String key = methodName.substring(3);//4文字目以降だけを取得
				key = key.substring(0, 1).toLowerCase()+key.substring(1);//先頭文字だけを小文字にする。field名がkeyになる。
				fieldNameGetterMethod_Map.put(key, method);
			}else if(methodName.matches("^set[\\dA-Z]\\w*")){
				String fieldName = methodName.substring(3);//4文字目以降だけを取得
				fieldName = fieldName.substring(0, 1).toLowerCase()+fieldName.substring(1);//先頭文字だけを小文字にする。field名がkeyになる。
				fieldNameSetterMethod_Map.put(check_IS_itemName(fieldName), method);
			}
		}
		cd.setFieldNameGetterMethod_Map(fieldNameGetterMethod_Map);
		cd.setFieldNameSetterMethod_Map(fieldNameSetterMethod_Map);
		_class_classData__Map.put(clazz, cd);
	}
	
	private void add__classClassData_Map2(Class<?> clazz){
		String className=clazz.getSimpleName();
		System.out.println(className+"は_class_classData__Mapにまだ登録されていないので、登録します。");
		ClassData cd=new ClassData();
		String domainName = _userName +className; 
		cd.setDomainName(domainName);
		////////////////////////////////////////////////fieldNameFieldvalueArray_Map,fieldNameFieldvalue_Mapを完成させるブロック
		Method[] declaredMethods = clazz.getDeclaredMethods();
		Map<String,Method> fieldNameGetterMethod_Map = new HashMap<String, Method>();// フィールドname,GetterMethodMap
		Map<String,Method> fieldNameSetterMethod_Map = new HashMap<String, Method>();
		for (Method method : declaredMethods) {// このクラスで宣言された全てのメソッド　getClassは含まない
			String methodName = method.getName();
			if (methodName.matches("^get[\\dA-Z]\\w*")) {// getterメソッドの場合
				String key = methodName.substring(3);//4文字目以降だけを取得
				//key = key.substring(0, 1).toLowerCase()+key.substring(1);//先頭文字だけを小文字にする。field名がkeyになる。
				fieldNameGetterMethod_Map.put(key, method);
			}else if(methodName.matches("^set[\\dA-Z]\\w*")){
				String fieldName = methodName.substring(3);//4文字目以降だけを取得
				//fieldName = fieldName.substring(0, 1).toLowerCase()+fieldName.substring(1);//先頭文字だけを小文字にする。field名がkeyになる。
				fieldNameSetterMethod_Map.put(check_IS_itemName(fieldName), method);
			}
		}
		cd.setFieldNameGetterMethod_Map(fieldNameGetterMethod_Map);
		cd.setFieldNameSetterMethod_Map(fieldNameSetterMethod_Map);
		_class_classData__Map.put(clazz, cd);
	}
	
	
	
	private void registerDataWithSimpaleDB(String domainName, Map<String, String> fieldNameFieldvalue_Map, List<Map<String, String>> mapList) {
		try {
			// itemName定義　格納するオブジェクトにitemNameフィールドがなければ、自動的に乱数を適用
			String registeredItemName = fieldNameFieldvalue_Map.get("ItemName");// 存在するかは不明だが、itemNameフィールドの値をget。存在しない時はnullを代入するよ。
			fieldNameFieldvalue_Map.remove("ItemName");// 存在するかは不明だが、itemNameフィールドを削除
			if (registeredItemName == null) {//ココで例外を投げる様にしましょうかね？
				UUID uuid = new UUID();
				registeredItemName = uuid.toString();
			}
			_simpleDB.putAttributes(domainName, registeredItemName, fieldNameFieldvalue_Map);// フィールド,フィールド値Map格納
			for (int i = 0; i < mapList.size(); i++) {
				_simpleDB.putAttributes(domainName, registeredItemName, mapList.get(i));
			}
		} catch (Exception er) {
			System.out.println("書き込み先のドメインがないかも");
			System.out.println("もしくは登録するクラスのインスタンスのフィールドが多すぎではないですか？最大で２５６個までですよ！");
			System.out.println("もしくは1マスに1024Byte以上入れようとしてません？そんなにいっぱい文章入りませんよ～！");
			er.printStackTrace();
		}
	}

	// 配列のmapを、mapのListに変換する
	private List<Map<String, String>> arrayMap_TO_mapList(Map<String, String[]> fieldNameFieldvalueArray_Map) {
		List<Map<String, String>> mapList = new ArrayList<Map<String, String>>();
		for (Map.Entry<String, String[]> fieldname_valueS_entry : fieldNameFieldvalueArray_Map.entrySet()) {
			String key = fieldname_valueS_entry.getKey();
			String[] values = fieldname_valueS_entry.getValue();

			for (int i = 0; i < values.length; i++) {
				Map<String, String> map = new HashMap<String, String>();
				map.put(key, values[i]);
				mapList.add(map);
			}
		}
		return mapList;
	}
	// //////------------------------------------------------------mapListをinstanceListに変更するぞ。
	@SuppressWarnings("rawtypes")
	private <T> List<T> mapListToObjectList(List<Map<String, String[]>> inputList, Class<T> clazz) {
		if (inputList.size() == 0) {
			return null;
		}
		if(!_class_classData__Map.containsKey(clazz)){//オブジェクトのフィールド　メソッドマップが登録されているかどうかチェック
			add__classClassData_Map(clazz);//されていなかったら、このメソッドで登録する。
		}else{
			System.out.println("_class_classData__Mapに登録済みだからmapListToObjectListメソッドも簡単");
		}
		ClassData classData = _class_classData__Map.get(clazz);
		Map<String, Method> map_fieldName_setterMethod = classData.getFieldNameSetterMethod_Map();
		
		List<T> returnList = new ArrayList<T>();// リターン用のリストを作成
		
		for (int i = 0; i < inputList.size(); i++) {// 各オブジェクトごとに繰り返す。
			Map<String, String[]> dataMap = (Map<String, String[]>)inputList.get(i);// 入力されたListからMap<String, String[]>を取り出す
			
			try {
				T createdInstance = (T) clazz.newInstance();// とりあえず、型に合ったインスタンスを作成

				for (String key : map_fieldName_setterMethod.keySet()) {// instanceを完成させるfor文
					Method setterMethod = map_fieldName_setterMethod.get(key);// setterメソッド取得
					Class[] setterMethodArgumentClasses = setterMethod.getParameterTypes();//全ての引数の型を取得
					String setterMethodArgumentClassName=setterMethodArgumentClasses[0].getName();//セッターメソッドの引数は必ず一つなので０個目の引数のクラス名だけ取得

					if(setterMethodArgumentClassName.equals("java.lang.String")){
						
						String[]  temp= dataMap.get(key);
						if(temp==null){
							key = key.substring(0, 1).toUpperCase()+key.substring(1);
							temp= dataMap.get(key);
						}
						
						
						System.out.println(temp[0]);
						setterMethod.invoke(createdInstance, dataMap.get(key)[0]);// ０個目だけ入れる
						
						
					}
					else{
						setterMethod.invoke(createdInstance,(Object)dataMap.get(key));// 配列ごと入れる
					}
				}
				returnList.add(createdInstance);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		return returnList;
	}
	
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////	
	private class ClassData{
		private String domainName;
		private Map<String, Method> fieldNameGetterMethod_Map;// = new HashMap<String, Method>();// フィールドname,GetterMethodMap
		private Map<String, Method> fieldNameSetterMethod_Map;// = new HashMap<String, Method>();// フィールドname,GetterMethodMap
		
		private String getDomainName() {
			return domainName;
		}
		private void setDomainName(String domainName) {
			this.domainName = domainName;
		}
		private Map<String, Method> getFieldNameGetterMethod_Map() {
			return fieldNameGetterMethod_Map;
		}
		private void setFieldNameGetterMethod_Map(Map<String, Method> fieldNameGetterMethod_Map) {
			this.fieldNameGetterMethod_Map = fieldNameGetterMethod_Map;
		}
		private Map<String, Method> getFieldNameSetterMethod_Map() {
			return fieldNameSetterMethod_Map;
		}
		private void setFieldNameSetterMethod_Map(Map<String, Method> fieldNameSetterMethod_Map) {
			this.fieldNameSetterMethod_Map = fieldNameSetterMethod_Map;
		}
		
	}
}

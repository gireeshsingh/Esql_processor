package dbmsProject;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class QPCreator {
	
	SalesTable salesTable;
	Payload payload;
	Helper helper;
	String varPrefix;
	ArrayList<String> projectionVars;
	private ArrayList<HashMap<String,String>> listMapsAggregates = new ArrayList<HashMap<String,String>>();
	
	public QPCreator(Payload payload) {
		this.varPrefix ="DBvar$";
		this.payload=payload;
		this.projectionVars= new ArrayList<String>();
		this.helper=new Helper();
	}
	
	//CREATES THE INPUT ROW CLASS FILE
	public void createInputRow() throws IOException {
		StringBuilder fileData = new StringBuilder();
		fileData.append("package dbmsProject;\n");
		fileData.append("public class InputRow{\n");
		ArrayList<String> list = new ArrayList<String>( Arrays.asList(new String[]{"cust", "prod", "day", "month", "year", "state", "quant"}));
		for(int i=0;i<list.size();i++ ) {
			String select = list.get(i);
			boolean found=false;
			for(String str: payload.getselect_variables()) {
				if(str.contains(select)) {
					projectionVars.add(select);
					fileData=createrSetterGetter(fileData, helper.columnMapping.get(select), select);
					found=true;
					break;
				}
			}
			if(found) continue;
			for(String str: payload.getgrouping_attributes()) {
				if(str.contains(select)) {
					projectionVars.add(select);
					fileData=createrSetterGetter(fileData, helper.columnMapping.get(select), select);
					found=true;
					break;
				}
			}
			if(found) continue;
			for(String str: payload.getaggregate_functions()) {
				if(str.contains(select)) {
					projectionVars.add(select);
					fileData=createrSetterGetter(fileData, helper.columnMapping.get(select), select);
					found=true;
					break;
				}
			}
			if(found) continue;
			for(String str: payload.getsuch_that_predicates()) {
				if(str.contains(select)) {
					projectionVars.add(select);
					fileData=createrSetterGetter(fileData, helper.columnMapping.get(select), select);
					found=true;
					break;
				}
			}
			if(found) continue;
			if(payload.isHavingClause() && payload.getHavingClause().contains(select)) {
				projectionVars.add(select);
				fileData=createrSetterGetter(fileData, helper.columnMapping.get(select), select);
				found=true;
			}
			if(found) continue;
			if(payload.getIsWhereClause() && payload.getWhereClause().contains(select)) {
				projectionVars.add(select);
				fileData=createrSetterGetter(fileData, helper.columnMapping.get(select), select);
			}
		}
		
		fileData.append("}\n");
		FileOutputStream fos = new FileOutputStream("src/dbmsProject/InputRow.java");
		fos.write(fileData.toString().getBytes());
		fos.flush();
		fos.close();
	}

	//CREATE THE OUTPUTROW class file for storing the results
	public void createOutputRow() throws IOException {
		StringBuilder fileData = new StringBuilder();
		fileData.append("package dbmsProject;\n");
		fileData.append("public class OutputRow{\n");
		
		for(String select: payload.getselect_variables()) {
			if(helper.columnMapping.containsKey(select)) {
				int col = helper.columnMapping.get(select);
				if(col==0 || col==1 || col==5) {
					fileData=createrSetterGetter(fileData, col, select);
				}
				else {
					fileData=createrSetterGetter(fileData, -1, select);
				}
			}
			else {
				if(select.contains("/")) select=select.replaceAll("/", "_divide_");
				if(select.contains("*")) select=select.replaceAll("*", "_multiply_");
				if(select.contains("+")) select=select.replaceAll("+", "_add_");
				if(select.contains("-")) select=select.replaceAll("-", "_minus_");
				if(select.contains(")")) select=select.replaceAll("[)]+", "");
				if(select.contains("(")) select=select.replaceAll("[(]+", "");
				fileData=createrSetterGetter(fileData, -5, select);
			}
		}
		
		fileData.append("}\n");
		FileOutputStream fos = new FileOutputStream("src/dbmsProject/OutputRow.java");
		fos.write(fileData.toString().getBytes());
		fos.flush();
		fos.close();
	}
	
	//GENERATE SETTERS AND GETTERS FOR INPUTROW AND OUTPUTROW classes
	public StringBuilder createrSetterGetter(StringBuilder fileData, int flag, String select) {
		if(flag==0 || flag==1 || flag==5) {
			fileData.append("	String "+varPrefix+select+";\n");
			fileData.append("	public void set"+varPrefix+select+"(String "+varPrefix+select+")"+"{\n");
			fileData.append("		this."+varPrefix+select+"="+varPrefix+select+";\n");
			fileData.append("	}\n");
			fileData.append("	public String get"+varPrefix+select+"()"+"{\n");
			fileData.append("		return "+varPrefix+select+";\n");
			fileData.append("	}\n");
		}
		else if(flag==-5) {
			fileData.append("	Double "+varPrefix+select+";\n");
			fileData.append("	public void set"+varPrefix+select+"(Double "+varPrefix+select+")"+"{\n");
			fileData.append("		this."+varPrefix+select+"="+varPrefix+select+";\n");
			fileData.append("	}\n");
			fileData.append("	public Double get"+varPrefix+select+"()"+"{\n");
			fileData.append("		return "+varPrefix+select+";\n");
			fileData.append("	}\n");
		}
		else {
			fileData.append("	int "+varPrefix+select+";\n");
			fileData.append("	public void set"+varPrefix+select+"(int "+varPrefix+select+")"+"{\n");
			fileData.append("		this."+varPrefix+select+"="+varPrefix+select+";\n");
			fileData.append("	}\n");
			fileData.append("	public int get"+varPrefix+select+"()"+"{\n");
			fileData.append("		return "+varPrefix+select+";\n");
			fileData.append("	}\n");
		}
		
		return fileData;
	}
	
	//CREATE THE QUERY PROCESSOR CLASS
	public void createQueryProcessor() throws IOException {
		for(int i=0; i<=payload.getnumber_of_grouping_variables();i++) {
			listMapsAggregates.add(new HashMap<String, String>());
		}
		for(int i=0;i<payload.getnumber_of_aggregate_functions();i++) {
			String[] temp = payload.getaggregate_functions().get(i).split("_",3);
			listMapsAggregates.get(Integer.parseInt(temp[0])).put(temp[1], temp[2]);//(key,value) -> (aggregate function , column name)
		}
		StringBuilder fileData = new StringBuilder();
		fileData.append("package dbmsProject;\r\n\r\n" + 
				"import java.io.FileOutputStream;\r\n" + 
				"import java.io.IOException;\r\n" + 
				"import java.text.DateFormat;\r\n" + 
				"import java.text.DecimalFormat;\r\n" + 
				"import java.text.SimpleDateFormat;\r\n" + 
				"import java.util.ArrayList;\r\n" + 
				"import java.util.Calendar;\r\n" + 
				"import java.util.HashMap;\r\n");
		fileData.append("\r\npublic class QueryProcessor{\n");
		fileData.append("	private SalesTable salesTable;\n");
		fileData.append("	private ExpTree expTree;\n");
		fileData.append("	private Payload payload;\n");
		fileData.append("	private Helper helper;\n");
		fileData.append("	private ArrayList<HashMap<String,String>> listMapsAggregates;\r\n" + 
						"	private HashMap<String, Double> aggregatesMap;\r\n" + 
						"	private ArrayList<HashMap<String, ArrayList<InputRow>>> allGroups;\r\n" + 
						"	private ArrayList<ArrayList<String>> allGroupKeyStrings;\r\n" + 
						"	private ArrayList<ArrayList<InputRow>> allGroupKeyRows;\n");
		fileData.append("\r\n	public QueryProcessor(SalesTable salesTable, Payload payload){\n");
		fileData.append("		this.aggregatesMap = new HashMap<String, Double>();\n");
		fileData.append("		this.salesTable=salesTable;\n");
		fileData.append("		this.payload=payload;\n");
		fileData.append("		this.expTree=new ExpTree();\n");
		fileData.append("		this.helper=new Helper();\n"
						+ "		this.allGroupKeyRows = new ArrayList<ArrayList<InputRow>>();\r\n" + 
						"		this.allGroupKeyStrings = new ArrayList<ArrayList<String>>();\r\n" + 
						"		this.listMapsAggregates = new ArrayList<HashMap<String,String>>();\r\n"	+
						"		this.allGroups = new ArrayList<HashMap<String, ArrayList<InputRow>>>();\n");
		fileData.append("	}\n");
		fileData.append("\r\n	public ArrayList<InputRow> createInputSet(){\n");
		fileData.append("		ArrayList<InputRow> inputResultSet = new ArrayList<InputRow>();\n");
		fileData.append("		for(SalesTableRow row: salesTable.getResultSet()) {\n");
		fileData.append("			InputRow ir=new InputRow();\n");
		for(String var: this.projectionVars) {
			fileData.append("			ir.set"+varPrefix+var+"(row.get"+var+"());\n");
		}
		fileData.append("			inputResultSet.add(ir);\n");
		fileData.append("		}\n");
		fileData.append("		return inputResultSet;\n");
		fileData.append("	}\n");
		
//OUTPUT ROW CREATION LOGIC
		fileData.append("\r\n	public OutputRow convertInputToOutputRow(InputRow inputRow, String str, ArrayList<String> strList){\n");
		fileData.append("		String temp=\"\";\n");
		fileData.append("		OutputRow outputRow = new OutputRow();\n");
		for(String select: payload.getselect_variables()) {
			if(helper.columnMapping.containsKey(select)) {
				fileData.append("		outputRow.set"+varPrefix+select+"(inputRow.get"+varPrefix+select+"());\n");
			}
			else {
				String temp=select;
				if(select.contains("/")) select=select.replaceAll("/", "_divide_");
				if(select.contains("*")) select=select.replaceAll("*", "_multiply_");
				if(select.contains("+")) select=select.replaceAll("+", "_add_");
				if(select.contains("-")) select=select.replaceAll("-", "_minus_");
				if(select.contains(")")) select=select.replaceAll("[)]+", "");
				if(select.contains("(")) select=select.replaceAll("[(]+", "");
				fileData.append("		temp = prepareClause(inputRow, inputRow, \""+temp+"\", str, strList);\n");
				fileData.append("		if(temp.contains(\"(\")) temp = expTree.execute(temp);\r\n");
				fileData.append("		if(temp.equals(\"discard_invalid_entry\")) return null;\n");
				fileData.append("		outputRow.set"+varPrefix+select+"(Double.parseDouble(temp));\n");
			}
		}
		fileData.append("		return outputRow;\n");
		fileData.append("	}\n");
		
//WHERE CLAUSE EXECUTOR
		fileData.append("\r\n	public ArrayList<InputRow> executeWhereClause(ArrayList<InputRow> inputResultSet) {\r\n" + 
				"		int i=0;\r\n" + 
				"		while(i<inputResultSet.size()) {\r\n" + 
				"			String condition=prepareClause(inputResultSet.get(i), inputResultSet.get(i), payload.getWhereClause(), \"\", new ArrayList<String>());\r\n" + 
				"			if(condition.equals(\"discard_invalid_entry\") || !Boolean.parseBoolean(expTree.execute(condition))){\r\n" + 
				"				inputResultSet.remove(i);\r\n" + 
				"				continue;\r\n" + 
				"			}\r\n" + 
				"			i++;\r\n" + 
				"		}\r\n" + 
				"		return inputResultSet;\r\n" + 
				"	}\n");

//REFINE CLAUSE FOR PROCESSING
		fileData.append("\r\n	public String prepareClause(InputRow row, InputRow rowZero, String condition, String str, ArrayList<String> strList) {\r\n" + 
				"		for(int i=0;i<strList.size();i++) {\r\n" + 
				"			if(condition.contains(i+\"_\")) {\r\n" + 
				"				boolean flag=false;\r\n" + 
				"				for(String ag : payload.getaggregate_functions()) {\r\n" + 
				"					if(!ag.contains(i+\"\")) continue;\r\n" + 
				"					if(condition.contains(ag)) flag=true;\r\n" + 
				"					condition=condition.replaceAll(ag, ag+\"_\"+strList.get(i));\r\n" + 
				"				}\r\n" + 
				"				if(flag) {\r\n" + 
				"					boolean changeFlag=false;\r\n" + 
				"					for(String key : aggregatesMap.keySet()) {\r\n" + 
				"						if(condition.contains(key)) changeFlag=true;\r\n" + 
				"						condition = condition.replaceAll(key, Double.toString(aggregatesMap.get(key)));\r\n" + 
				"					}\r\n" + 
				"					if(!changeFlag) return \"discard_invalid_entry\";\r\n" + 
				"				}\r\n" + 
				"			}\r\n" + 
				"		}\r\n" + 
				"		\r\n" + 
				"		if(condition.contains(\".\")) {\r\n");
		for(String var: projectionVars) {
			if(helper.columnMapping.get(var)==0 || helper.columnMapping.get(var)==1 || helper.columnMapping.get(var)==5)
				fileData.append("			condition=condition.replaceAll(\"[0-9]+\\\\."+var+"\", row.get"+varPrefix+var+"());\r\n");
			else
				fileData.append("			condition=condition.replaceAll(\"[0-9]+\\\\."+var+"\", Integer.toString(row.get"+varPrefix+var+"()));\r\n");
		}
		fileData.append("		}\n");
		
		for(String var: projectionVars) {
			if(helper.columnMapping.get(var)==0 || helper.columnMapping.get(var)==1 || helper.columnMapping.get(var)==5)
				fileData.append("		condition=condition.replaceAll(\""+var+"\", rowZero.get"+varPrefix+var+"());\r\n");
			else
				fileData.append("		condition=condition.replaceAll(\""+var+"\", Integer.toString(rowZero.get"+varPrefix+var+"()));\r\n");
		}
		
		fileData.append(
				"		condition=condition.replaceAll(\"\\\\s+\", \"\");\r\n" + 
				"		condition=condition.replaceAll(\"\\\"\", \"\");\r\n" + 
				"		condition=condition.replaceAll(\"\\'\", \"\");\r\n" + 
				"		\r\n" + 
				"		return condition;\r\n" + 
				"	}\n");

//CREATE GROUPS		
		fileData.append("\r\n	public void createListsBasedOnSuchThatPredicate(ArrayList<InputRow> inputResultSet) {\r\n" + 
				"		\r\n" + 
				"		for(int i=0;i<=payload.getnumber_of_grouping_variables();i++) {\r\n" + 
				"			ArrayList<String> groupKeyStrings = new ArrayList<String>();\r\n" + 
				"			ArrayList<InputRow> groupKeyRows = new ArrayList<InputRow>();\r\n" + 
				"			for(InputRow row : inputResultSet) {\r\n" + 
				"				StringBuilder temp=new StringBuilder();\r\n" + 
				"				InputRow groupRow = new InputRow();\r\n" + 
				"				for(String group: payload.getGroupingAttributesOfAllGroups().get(i)) {\r\n" + 
				"					int col = helper.columnMapping.get(group);\r\n" + 
				"					switch(col) {\r\n");
		for(String var: projectionVars) {
			fileData.append("						case "+helper.columnMapping.get(var)+":"+"{temp.append(row.get"+varPrefix+var+"()+\"_\"); groupRow.set"+varPrefix+ var+ "(row.get"+varPrefix+var+"()); break;}\r\n");
		}
		fileData.append("					}\n"+
				"				}\r\n" + 
				"				String s=temp.toString();\r\n" + 
				"				if(s.charAt(s.length()-1)=='_') s=s.substring(0, s.length()-1);\r\n" + 
				"				if( !groupKeyStrings.contains(s) ) {\r\n" + 
				"					groupKeyStrings.add(s);\r\n" + 
				"					groupKeyRows.add(groupRow);\r\n" + 
				"				}\r\n" + 
				"			}\r\n" + 
				"			allGroupKeyRows.add(groupKeyRows);\r\n" + 
				"			allGroupKeyStrings.add(groupKeyStrings);\r\n" + 
				"		}\r\n" + 
				"		\r\n" + 
				"		for(int i=0;i<=payload.getnumber_of_grouping_variables();i++) {\r\n" + 
				"			HashMap<String, ArrayList<InputRow>> res = new HashMap<String, ArrayList<InputRow>>();\r\n" + 
				"			String suchThat = payload.getsuch_that_predicates().get(i);\r\n" + 
				"			for(int j=0;j<allGroupKeyRows.get(i).size();j++) {\r\n" + 
				"				InputRow zeroRow = allGroupKeyRows.get(i).get(j);\r\n" + 
				"				ArrayList<InputRow> groupMember = new ArrayList<InputRow>();\r\n" + 
				"				for(InputRow salesRow : inputResultSet) {\r\n" + 
				"					String condition = prepareClause(salesRow, zeroRow, suchThat, \"\", new ArrayList<String>());\r\n" + 
				"					if(Boolean.parseBoolean(expTree.execute(condition))) {\r\n" + 
				"						groupMember.add(salesRow);\r\n" + 
				"					}\r\n" + 
				"				}\r\n" + 
				"				res.put(allGroupKeyStrings.get(i).get(j), new ArrayList<InputRow>(groupMember));\r\n" + 
				"			}\r\n" + 
				"			allGroups.add(new HashMap<String, ArrayList<InputRow>>(res));\r\n" + 
				"		}\r\n" + 
				"	}\n");
		
//GETGROUPING VARIABLE METHOD
		fileData.append("\r\n	public String getGroupingVariable(int i, InputRow row) {\r\n" + 
				"		switch(i) {\r\n");
		for(String var: projectionVars) {
			if(helper.columnMapping.get(var)==0||helper.columnMapping.get(var)==1||helper.columnMapping.get(var)==5)
				fileData.append("			case "+helper.columnMapping.get(var)+": return row.get"+varPrefix+var+"();\r\n");
			else if(helper.columnMapping.get(var)==7)
				fileData.append("			case "+helper.columnMapping.get(var)+": return \"all\"");
			else
				fileData.append("			case "+helper.columnMapping.get(var)+": return Integer.toString(row.get"+varPrefix+var+"());\r\n");
		}
		fileData.append("		}\r\n");
		fileData.append("		return \"__Garbage__\";\r\n");
		fileData.append("	}\n");
		
//COMPUTE AGGREGATES METHOD
		fileData.append("\r\n	public void computeAggregates(ArrayList<InputRow> inputResultSet) {	\r\n" + 
				"		double val=0;\r\n"+
				"		for(int i=0; i<=payload.getnumber_of_grouping_variables();i++) {\r\n" + 
				"			listMapsAggregates.add(new HashMap<String, String>());\r\n" + 
				"		}\r\n" + 
				"		for(int i=0;i<payload.getnumber_of_aggregate_functions();i++) {\r\n" + 
				"			String[] temp = payload.getaggregate_functions().get(i).split(\"_\",3);\r\n" + 
				"			listMapsAggregates.get(Integer.parseInt(temp[0])).put(temp[1], temp[2]);//(key,value) -> (aggregate function , column name)\r\n" + 
				"		}\r\n"+
				"		int nGroupingVariables=0;\r\n"+
				"		aggregatesMap = new HashMap<>();\r\n"+
				"		HashMap<String,Double> tempAggregatesMap;\r\n");
		
		for(int nGroupingVariables=0;nGroupingVariables<=payload.getnumber_of_grouping_variables();nGroupingVariables++) {
			fileData.append("\n		nGroupingVariables="+nGroupingVariables+";\r\n");
			fileData.append(
							"		tempAggregatesMap = new HashMap<String,Double>();\r\n" + 
							"		\r\n" + 
							"		for(int i=0;i<allGroupKeyRows.get(nGroupingVariables).size(); i++) {\r\n" + 
							"			InputRow zeroRow = allGroupKeyRows.get(nGroupingVariables).get(i);\r\n" );
			
			//MFvsEMF
			if(isGroupMF(nGroupingVariables)) fileData.append("			for(InputRow row: allGroups.get(nGroupingVariables).get(allGroupKeyStrings.get(nGroupingVariables).get(i)))	{\r\n");
			else fileData.append("			for(InputRow row: inputResultSet) {\r\n");
			
			fileData.append("				String condition = payload.getsuch_that_predicates().get(nGroupingVariables);\r\n" + 
							"				String str = allGroupKeyStrings.get(nGroupingVariables).get(i);\r\n" + 
							"				ArrayList<String> strList = new ArrayList<String>();\r\n" + 
							"				for(int j=0;j<=payload.getnumber_of_grouping_variables();j++) strList.add(str);\r\n" +
							"				condition= prepareClause(row, zeroRow, condition, str, strList);\r\n" + 
							"				if(condition.equals(\"discard_invalid_entry\") || !Boolean.parseBoolean(expTree.execute(condition))) continue;\r\n"
							);
			String key1 = nGroupingVariables+"_sum_"+listMapsAggregates.get(nGroupingVariables).get("sum");
			String key2 = nGroupingVariables+"_avg_"+listMapsAggregates.get(nGroupingVariables).get("avg");
			String key3 = nGroupingVariables+"_min_"+listMapsAggregates.get(nGroupingVariables).get("min");
			String key4 = nGroupingVariables+"_max_"+listMapsAggregates.get(nGroupingVariables).get("max");
			String key5 = nGroupingVariables+"_count_"+listMapsAggregates.get(nGroupingVariables).get("count");
			String key6 = nGroupingVariables+"_count_"+listMapsAggregates.get(nGroupingVariables).get("avg");
			
			if(listMapsAggregates.get(nGroupingVariables).containsKey("sum")) 
				fileData.append("				String key1=\""+key1+"\";\r\n");
			if(listMapsAggregates.get(nGroupingVariables).containsKey("avg")) {
				fileData.append("				String key2=\""+key2+"\";\r\n");
				fileData.append("				String key6=\""+key6+"\";\r\n");
			}
			if(listMapsAggregates.get(nGroupingVariables).containsKey("min"))
				fileData.append("				String key3=\""+key3+"\";\r\n");
			if(listMapsAggregates.get(nGroupingVariables).containsKey("max"))
				fileData.append("				String key4=\""+key4+"\";\r\n");
			if(listMapsAggregates.get(nGroupingVariables).containsKey("count"))
				fileData.append("				String key5=\""+key5+"\";\r\n");
			
			fileData.append("				for(String ga: payload.getGroupingAttributesOfAllGroups().get(nGroupingVariables)) {\r\n");
			if(listMapsAggregates.get(nGroupingVariables).containsKey("sum")) 
				fileData.append("					key1=key1+\"_\"+ getGroupingVariable(helper.columnMapping.get(ga), zeroRow);\r\n");
			if(listMapsAggregates.get(nGroupingVariables).containsKey("avg")) {
				fileData.append("					key2=key2+\"_\"+ getGroupingVariable(helper.columnMapping.get(ga), zeroRow);\r\n");
				fileData.append("					key6=key6+\"_\"+ getGroupingVariable(helper.columnMapping.get(ga), zeroRow);\r\n");
			}
			if(listMapsAggregates.get(nGroupingVariables).containsKey("min"))
				fileData.append("					key3=key3+\"_\"+ getGroupingVariable(helper.columnMapping.get(ga), zeroRow);\r\n");
			if(listMapsAggregates.get(nGroupingVariables).containsKey("max"))
				fileData.append("					key4=key4+\"_\"+ getGroupingVariable(helper.columnMapping.get(ga), zeroRow);\r\n");
			if(listMapsAggregates.get(nGroupingVariables).containsKey("count"))
				fileData.append("					key5=key5+\"_\"+ getGroupingVariable(helper.columnMapping.get(ga), zeroRow);\r\n");
			fileData.append("				}\r\n");
			
			if(listMapsAggregates.get(nGroupingVariables).containsKey("sum")) {
				fileData.append("			val=tempAggregatesMap.getOrDefault(key1, 0.0)+Double.parseDouble(getGroupingVariable(helper.columnMapping.get(listMapsAggregates.get(nGroupingVariables).get(\"sum\")), row));\r\n" + 
				"			tempAggregatesMap.put(key1, val);\r\n");
			}
			if(listMapsAggregates.get(nGroupingVariables).containsKey("avg")) {
				fileData.append("			val=tempAggregatesMap.getOrDefault(key2, 0.0)+Double.parseDouble(getGroupingVariable(helper.columnMapping.get(listMapsAggregates.get(nGroupingVariables).get(\"avg\")), row));\r\n"+
				"			tempAggregatesMap.put(key2, val);\r\n"+
				"			tempAggregatesMap.put(key6, tempAggregatesMap.getOrDefault(key6, 0.0)+1);\r\n");
			}
			if(listMapsAggregates.get(nGroupingVariables).containsKey("min")) {
				fileData.append("			val=Math.min( tempAggregatesMap.getOrDefault(key3, Double.MAX_VALUE) , Double.parseDouble(getGroupingVariable(helper.columnMapping.get(listMapsAggregates.get(nGroupingVariables).get(\"min\")), row)));\r\n"+
				"			tempAggregatesMap.put(key3, val);\r\n");
			}
			if(listMapsAggregates.get(nGroupingVariables).containsKey("max")) {
				fileData.append("			val=Math.max( tempAggregatesMap.getOrDefault(key4, Double.MIN_VALUE) , Double.parseDouble(getGroupingVariable(helper.columnMapping.get(listMapsAggregates.get(nGroupingVariables).get(\"max\")), row)));\r\n"+
				"			tempAggregatesMap.put(key4, val);");
			}
			if(listMapsAggregates.get(nGroupingVariables).containsKey("count")) {
				fileData.append("			tempAggregatesMap.put(key5, tempAggregatesMap.getOrDefault(key5, 0.0)+1);\r\n");
			}
			fileData.append("			}\r\n");
			fileData.append("		}\r\n");
			if(listMapsAggregates.get(nGroupingVariables).containsKey("avg")) {
				fileData.append(
				"		for(String key: tempAggregatesMap.keySet()) {\r\n"+
				"			if(key.contains(\"_avg_\"))\r\n"+
				"				tempAggregatesMap.put(key, tempAggregatesMap.get(key)/tempAggregatesMap.get(key.replace(\"_avg_\", \"_count_\")));\r\n"+
				"		}\r\n");
			}
			fileData.append("		aggregatesMap.putAll(tempAggregatesMap);\r\n");
		}
		
		fileData.append("	}\n");

//PREPARE THE RESULTS AND ADD THEM TO A LIST OF OUTPUTROW
		fileData.append("\r\n	public ArrayList<OutputRow> createOutputResultSet() {\r\n" + 
				"		ArrayList<OutputRow> outputRowList = new ArrayList<OutputRow>();\r\n"+
				"		for(int i=0; i<allGroupKeyRows.get(0).size();i++) {\r\n" + 
				"			String str=allGroupKeyStrings.get(0).get(i);\r\n" + 
				"			String[] tempStr = str.split(\"_\");\r\n" + 
				"			ArrayList<String> strList = new ArrayList<String>();\r\n" + 
				"			for(int j=0; j<=payload.getnumber_of_grouping_variables(); j++) {\r\n" + 
				"				String ss = \"\";\r\n" + 
				"				int k=0;\r\n" + 
				"				for(String gz: payload.getgrouping_attributes()) {\r\n" + 
				"					if(payload.getGroupingAttributesOfAllGroups().get(j).contains(gz)) ss=ss+tempStr[k++]+\"_\";\r\n" + 
				"					else k++;\r\n" + 
				"				}\r\n" + 
				"				strList.add(ss.substring(0, ss.length()-1));\r\n" + 
				"			}\r\n" + 
				"			//having check\r\n" + 
				"			if(payload.isHavingClause()) {\r\n" + 
				"				String condition= prepareClause(allGroupKeyRows.get(0).get(i), allGroupKeyRows.get(0).get(i), payload.getHavingClause(), str, strList);\r\n" + 
				"				if(condition.equals(\"discard_invalid_entry\") || !Boolean.parseBoolean(expTree.execute(condition))) continue;\r\n" + 
				"			}\r\n" + 
				"\r\n" + 
				"			OutputRow outputRow= convertInputToOutputRow(allGroupKeyRows.get(0).get(i), str, strList);\r\n"+
				"			if(outputRow!=null){\r\n" + 
				"				outputRowList.add(outputRow);\r\n"+
				"			}\r\n"+	
				"		}\r\n" + 
				"		return outputRowList;\r\n"+
				"	}\n");
		
//PRINT THE OUTPUT ROW
		fileData.append("\r\n	public void printOutputResultSet(ArrayList<OutputRow> outputResultSet) throws IOException{\r\n");
		fileData.append("		Calendar now = Calendar.getInstance();\r\n" + 
				"		DateFormat dateFormat = new SimpleDateFormat(\"MM/dd/yyyy HH:mm:ss\");\r\n" + 
				"		StringBuilder fileData = new StringBuilder();\r\n" + 
				"		fileData.append(\"TIME (MM/dd/yyyy HH:mm:ss)::::\"+dateFormat.format(now.getTime())+\"\\r\\n\");\r\n" + 
				"		String addDiv = \" -------------- \";\r\n" + 
				"		String divide = \"\";\r\n" + 
				"		String header=\"\";"+
				"		for(String select: payload.getselect_variables()) {\r\n" + 
				"			if(select.contains(\"0_\")) select=select.substring(2);\r\n" + 
				"			header=header+\"  \"+select;\r\n" + 
				"			for(int i=0;i<14-select.length();i++) header=header+\" \";\r\n" + 
				"			divide=divide+addDiv;\r\n"+
				"		}\r\n" + 
				"		System.out.println(divide); fileData.append(divide+\"\\r\\n\");\r\n" + 
				"		System.out.println(header); fileData.append(header+\"\\r\\n\");\r\n" + 
				"		System.out.println(divide); fileData.append(divide+\"\\r\\n\");\r\n" + 
				//"		System.out.println(); fileData.append(\"\\r\\n\");\r\n" + 
				"		String ansString=\"\";\r\n" + 
				"		DecimalFormat df = new DecimalFormat(\"#.####\");\r\n");
		fileData.append("		for(OutputRow outputRow: outputResultSet) {\r\n");
		fileData.append("			String answer=\"\";\r\n");
		
		for(String select: payload.getselect_variables()) {
			if(helper.columnMapping.containsKey(select)) {
				int col = helper.columnMapping.get(select);
				if(col==0|| col==1|| col==5) {
					//string
					fileData.append("			ansString=outputRow.get"+varPrefix+select+"();\r\n");
					fileData.append("			answer=answer+\" \"+ansString;\r\n");
					fileData.append("			for(int k=0;k<14-ansString.length();k++) answer=answer+\" \";\r\n");
				}
				else {
					//int
					fileData.append("			ansString = Integer.toString(outputRow.get"+varPrefix+select+"());\r\n");
					fileData.append("			for(int k=0;k<12-ansString.length();k++) answer=answer+\" \";\r\n");
					fileData.append("			answer=answer+ansString+\"    \";\r\n");
				}
			}
			else {
				//double
				if(select.contains("/")) select=select.replaceAll("/", "_divide_");
				if(select.contains("*")) select=select.replaceAll("*", "_multiply_");
				if(select.contains("+")) select=select.replaceAll("+", "_add_");
				if(select.contains("-")) select=select.replaceAll("-", "_minus_");
				if(select.contains(")")) select=select.replaceAll("[)]+", "");
				if(select.contains("(")) select=select.replaceAll("[(]+", "");
				fileData.append("			ansString = df.format(outputRow.get"+varPrefix+select+"());\r\n");
				fileData.append("			for(int k=0;k<12-ansString.length();k++) answer=answer+\" \";\r\n");
				fileData.append("			answer=answer+ansString+\"    \";\r\n");
			}
		}
		fileData.append("			System.out.println(answer); fileData.append(answer+\"\\r\\n\");\r\n");
		fileData.append("		}\r\n");
		fileData.append("		FileOutputStream fos = new FileOutputStream(\"queryOutput/"+payload.fileName+"\");\r\n" + 
				"		fos.write(fileData.toString().getBytes());\r\n" + 
				"		fos.flush();\r\n" + 
				"		fos.close();\r\n");
		fileData.append("	}\r\n");
		
		
//DRIVER METHOD OF THE QUERY PROCESSOR
		fileData.append("\r\n	public void process() throws IOException{\r\n" + 
				"		ArrayList<InputRow> inputResultSet = createInputSet();\r\n" + 
				"		if(payload.getIsWhereClause()) inputResultSet = executeWhereClause(inputResultSet);\r\n" + 
				"		if(payload.getnumber_of_grouping_variables()>0) createListsBasedOnSuchThatPredicate(inputResultSet);\r\n" + 
				"		computeAggregates(inputResultSet);\r\n" + 
				"		ArrayList<OutputRow> outputResultSet = createOutputResultSet();\r\n" + 
				"		printOutputResultSet(outputResultSet);\r\n"+
				"	}\n");
		
		fileData.append("}");
		FileOutputStream fos = new FileOutputStream("src/dbmsProject/QueryProcessor.java");
		fos.write(fileData.toString().getBytes());
		fos.flush();
		fos.close();
	}
	
	/*
	 * public String createAggregateString(InputRow inputRow) {
	 * 
	 * }
	 */
//CHECK IF QUERY IF MF or EMF
	public boolean isGroupMF( int nGroupingVariables) {
		String str = payload.getsuch_that_predicates().get(0).replaceAll("0", ""+nGroupingVariables);
		if(payload.getsuch_that_predicates().get(nGroupingVariables).contains(str))
			return true;
		return false;
	}
	
	//Provides the sequence to compute multiple processes without conflict
	public ArrayList<ArrayList<Integer>> sequenceOfGroups(){
		ArrayList<ArrayList<Integer>> tempResult = new ArrayList<ArrayList<Integer>>();
		ArrayList<ArrayList<Integer>> finalResult = new ArrayList<ArrayList<Integer>>();
		tempResult.add(new ArrayList<Integer>());
		for(int i=1;i<=payload.getnumber_of_such_that_predicates();i++) {
			ArrayList<Integer> list = new ArrayList<Integer>();
			list.add(0);
			String str=payload.getsuch_that_predicates().get(i);
			for(int j=1;j<=payload.getnumber_of_such_that_predicates(); j++) {
				if(i!=j && (str.contains(j+".") || str.contains(j+"_"))) list.add(j);
			}
			tempResult.add(list);
		}
		
		for(int k=0; k<=payload.getnumber_of_such_that_predicates(); k++) {
			
			ArrayList<Integer> list = new ArrayList<Integer>();
			for(int i=0;i<=payload.getnumber_of_such_that_predicates();i++) {
				if(tempResult.get(i)!=null && tempResult.get(i).size()==0) {
					tempResult.set(i,null);
					list.add(i);
				}
			}
			if(list.size()==0) break;
			finalResult.add(list);
			for(int i=0;i<=payload.getnumber_of_such_that_predicates();i++) {
				if(tempResult.get(i)==null) continue;
				for(int j=0; j <list.size(); j++) {
					tempResult.get(i).remove(Integer.valueOf(list.get(j)));
				}
			}
		}
		
		return finalResult;
	}

	
}

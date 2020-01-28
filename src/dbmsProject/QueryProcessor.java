package dbmsProject;

import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

public class QueryProcessor{
	private SalesTable salesTable;
	private ExpTree expTree;
	private Payload payload;
	private Helper helper;
	private ArrayList<HashMap<String,String>> listMapsAggregates;
	private HashMap<String, Double> aggregatesMap;
	private ArrayList<HashMap<String, ArrayList<InputRow>>> allGroups;
	private ArrayList<ArrayList<String>> allGroupKeyStrings;
	private ArrayList<ArrayList<InputRow>> allGroupKeyRows;

	public QueryProcessor(SalesTable salesTable, Payload payload){
		this.aggregatesMap = new HashMap<String, Double>();
		this.salesTable=salesTable;
		this.payload=payload;
		this.expTree=new ExpTree();
		this.helper=new Helper();
		this.allGroupKeyRows = new ArrayList<ArrayList<InputRow>>();
		this.allGroupKeyStrings = new ArrayList<ArrayList<String>>();
		this.listMapsAggregates = new ArrayList<HashMap<String,String>>();
		this.allGroups = new ArrayList<HashMap<String, ArrayList<InputRow>>>();
	}

	public ArrayList<InputRow> createInputSet(){
		ArrayList<InputRow> inputResultSet = new ArrayList<InputRow>();
		for(SalesTableRow row: salesTable.getResultSet()) {
			InputRow ir=new InputRow();
			ir.setDBvar$prod(row.getprod());
			ir.setDBvar$quant(row.getquant());
			inputResultSet.add(ir);
		}
		return inputResultSet;
	}

	public OutputRow convertInputToOutputRow(InputRow inputRow, String str, ArrayList<String> strList){
		String temp="";
		OutputRow outputRow = new OutputRow();
		outputRow.setDBvar$prod(inputRow.getDBvar$prod());
		outputRow.setDBvar$quant(inputRow.getDBvar$quant());
		return outputRow;
	}

	public ArrayList<InputRow> executeWhereClause(ArrayList<InputRow> inputResultSet) {
		int i=0;
		while(i<inputResultSet.size()) {
			String condition=prepareClause(inputResultSet.get(i), inputResultSet.get(i), payload.getWhereClause(), "", new ArrayList<String>());
			if(condition.equals("discard_invalid_entry") || !Boolean.parseBoolean(expTree.execute(condition))){
				inputResultSet.remove(i);
				continue;
			}
			i++;
		}
		return inputResultSet;
	}

	public String prepareClause(InputRow row, InputRow rowZero, String condition, String str, ArrayList<String> strList) {
		for(int i=0;i<strList.size();i++) {
			if(condition.contains(i+"_")) {
				boolean flag=false;
				for(String ag : payload.getaggregate_functions()) {
					if(!ag.contains(i+"")) continue;
					if(condition.contains(ag)) flag=true;
					condition=condition.replaceAll(ag, ag+"_"+strList.get(i));
				}
				if(flag) {
					boolean changeFlag=false;
					for(String key : aggregatesMap.keySet()) {
						if(condition.contains(key)) changeFlag=true;
						condition = condition.replaceAll(key, Double.toString(aggregatesMap.get(key)));
					}
					if(!changeFlag) return "discard_invalid_entry";
				}
			}
		}
		
		if(condition.contains(".")) {
			condition=condition.replaceAll("[0-9]+\\.prod", row.getDBvar$prod());
			condition=condition.replaceAll("[0-9]+\\.quant", Integer.toString(row.getDBvar$quant()));
		}
		condition=condition.replaceAll("prod", rowZero.getDBvar$prod());
		condition=condition.replaceAll("quant", Integer.toString(rowZero.getDBvar$quant()));
		condition=condition.replaceAll("\\s+", "");
		condition=condition.replaceAll("\"", "");
		condition=condition.replaceAll("\'", "");
		
		return condition;
	}

	public void createListsBasedOnSuchThatPredicate(ArrayList<InputRow> inputResultSet) {
		
		for(int i=0;i<=payload.getnumber_of_grouping_variables();i++) {
			ArrayList<String> groupKeyStrings = new ArrayList<String>();
			ArrayList<InputRow> groupKeyRows = new ArrayList<InputRow>();
			for(InputRow row : inputResultSet) {
				StringBuilder temp=new StringBuilder();
				InputRow groupRow = new InputRow();
				for(String group: payload.getGroupingAttributesOfAllGroups().get(i)) {
					int col = helper.columnMapping.get(group);
					switch(col) {
						case 1:{temp.append(row.getDBvar$prod()+"_"); groupRow.setDBvar$prod(row.getDBvar$prod()); break;}
						case 6:{temp.append(row.getDBvar$quant()+"_"); groupRow.setDBvar$quant(row.getDBvar$quant()); break;}
					}
				}
				String s=temp.toString();
				if(s.charAt(s.length()-1)=='_') s=s.substring(0, s.length()-1);
				if( !groupKeyStrings.contains(s) ) {
					groupKeyStrings.add(s);
					groupKeyRows.add(groupRow);
				}
			}
			allGroupKeyRows.add(groupKeyRows);
			allGroupKeyStrings.add(groupKeyStrings);
		}
		
		for(int i=0;i<=payload.getnumber_of_grouping_variables();i++) {
			HashMap<String, ArrayList<InputRow>> res = new HashMap<String, ArrayList<InputRow>>();
			String suchThat = payload.getsuch_that_predicates().get(i);
			for(int j=0;j<allGroupKeyRows.get(i).size();j++) {
				InputRow zeroRow = allGroupKeyRows.get(i).get(j);
				ArrayList<InputRow> groupMember = new ArrayList<InputRow>();
				for(InputRow salesRow : inputResultSet) {
					String condition = prepareClause(salesRow, zeroRow, suchThat, "", new ArrayList<String>());
					if(Boolean.parseBoolean(expTree.execute(condition))) {
						groupMember.add(salesRow);
					}
				}
				res.put(allGroupKeyStrings.get(i).get(j), new ArrayList<InputRow>(groupMember));
			}
			allGroups.add(new HashMap<String, ArrayList<InputRow>>(res));
		}
	}

	public String getGroupingVariable(int i, InputRow row) {
		switch(i) {
			case 1: return row.getDBvar$prod();
			case 6: return Integer.toString(row.getDBvar$quant());
		}
		return "__Garbage__";
	}

	public void computeAggregates(ArrayList<InputRow> inputResultSet) {	
		double val=0;
		for(int i=0; i<=payload.getnumber_of_grouping_variables();i++) {
			listMapsAggregates.add(new HashMap<String, String>());
		}
		for(int i=0;i<payload.getnumber_of_aggregate_functions();i++) {
			String[] temp = payload.getaggregate_functions().get(i).split("_",3);
			listMapsAggregates.get(Integer.parseInt(temp[0])).put(temp[1], temp[2]);//(key,value) -> (aggregate function , column name)
		}
		int nGroupingVariables=0;
		aggregatesMap = new HashMap<>();
		HashMap<String,Double> tempAggregatesMap;

		nGroupingVariables=0;
		tempAggregatesMap = new HashMap<String,Double>();
		
		for(int i=0;i<allGroupKeyRows.get(nGroupingVariables).size(); i++) {
			InputRow zeroRow = allGroupKeyRows.get(nGroupingVariables).get(i);
			for(InputRow row: allGroups.get(nGroupingVariables).get(allGroupKeyStrings.get(nGroupingVariables).get(i)))	{
				String condition = payload.getsuch_that_predicates().get(nGroupingVariables);
				String str = allGroupKeyStrings.get(nGroupingVariables).get(i);
				ArrayList<String> strList = new ArrayList<String>();
				for(int j=0;j<=payload.getnumber_of_grouping_variables();j++) strList.add(str);
				condition= prepareClause(row, zeroRow, condition, str, strList);
				if(condition.equals("discard_invalid_entry") || !Boolean.parseBoolean(expTree.execute(condition))) continue;
				for(String ga: payload.getGroupingAttributesOfAllGroups().get(nGroupingVariables)) {
				}
			}
		}
		aggregatesMap.putAll(tempAggregatesMap);

		nGroupingVariables=1;
		tempAggregatesMap = new HashMap<String,Double>();
		
		for(int i=0;i<allGroupKeyRows.get(nGroupingVariables).size(); i++) {
			InputRow zeroRow = allGroupKeyRows.get(nGroupingVariables).get(i);
			for(InputRow row: inputResultSet) {
				String condition = payload.getsuch_that_predicates().get(nGroupingVariables);
				String str = allGroupKeyStrings.get(nGroupingVariables).get(i);
				ArrayList<String> strList = new ArrayList<String>();
				for(int j=0;j<=payload.getnumber_of_grouping_variables();j++) strList.add(str);
				condition= prepareClause(row, zeroRow, condition, str, strList);
				if(condition.equals("discard_invalid_entry") || !Boolean.parseBoolean(expTree.execute(condition))) continue;
				String key5="1_count_prod";
				for(String ga: payload.getGroupingAttributesOfAllGroups().get(nGroupingVariables)) {
					key5=key5+"_"+ getGroupingVariable(helper.columnMapping.get(ga), zeroRow);
				}
			tempAggregatesMap.put(key5, tempAggregatesMap.getOrDefault(key5, 0.0)+1);
			}
		}
		aggregatesMap.putAll(tempAggregatesMap);

		nGroupingVariables=2;
		tempAggregatesMap = new HashMap<String,Double>();
		
		for(int i=0;i<allGroupKeyRows.get(nGroupingVariables).size(); i++) {
			InputRow zeroRow = allGroupKeyRows.get(nGroupingVariables).get(i);
			for(InputRow row: inputResultSet) {
				String condition = payload.getsuch_that_predicates().get(nGroupingVariables);
				String str = allGroupKeyStrings.get(nGroupingVariables).get(i);
				ArrayList<String> strList = new ArrayList<String>();
				for(int j=0;j<=payload.getnumber_of_grouping_variables();j++) strList.add(str);
				condition= prepareClause(row, zeroRow, condition, str, strList);
				if(condition.equals("discard_invalid_entry") || !Boolean.parseBoolean(expTree.execute(condition))) continue;
				String key5="2_count_prod";
				for(String ga: payload.getGroupingAttributesOfAllGroups().get(nGroupingVariables)) {
					key5=key5+"_"+ getGroupingVariable(helper.columnMapping.get(ga), zeroRow);
				}
			tempAggregatesMap.put(key5, tempAggregatesMap.getOrDefault(key5, 0.0)+1);
			}
		}
		aggregatesMap.putAll(tempAggregatesMap);
	}

	public ArrayList<OutputRow> createOutputResultSet() {
		ArrayList<OutputRow> outputRowList = new ArrayList<OutputRow>();
		for(int i=0; i<allGroupKeyRows.get(0).size();i++) {
			String str=allGroupKeyStrings.get(0).get(i);
			String[] tempStr = str.split("_");
			ArrayList<String> strList = new ArrayList<String>();
			for(int j=0; j<=payload.getnumber_of_grouping_variables(); j++) {
				String ss = "";
				int k=0;
				for(String gz: payload.getgrouping_attributes()) {
					if(payload.getGroupingAttributesOfAllGroups().get(j).contains(gz)) ss=ss+tempStr[k++]+"_";
					else k++;
				}
				strList.add(ss.substring(0, ss.length()-1));
			}
			//having check
			if(payload.isHavingClause()) {
				String condition= prepareClause(allGroupKeyRows.get(0).get(i), allGroupKeyRows.get(0).get(i), payload.getHavingClause(), str, strList);
				if(condition.equals("discard_invalid_entry") || !Boolean.parseBoolean(expTree.execute(condition))) continue;
			}

			OutputRow outputRow= convertInputToOutputRow(allGroupKeyRows.get(0).get(i), str, strList);
			if(outputRow!=null){
				outputRowList.add(outputRow);
			}
		}
		return outputRowList;
	}

	public void printOutputResultSet(ArrayList<OutputRow> outputResultSet) throws IOException{
		Calendar now = Calendar.getInstance();
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		StringBuilder fileData = new StringBuilder();
		fileData.append("TIME (MM/dd/yyyy HH:mm:ss)::::"+dateFormat.format(now.getTime())+"\r\n");
		String addDiv = " -------------- ";
		String divide = "";
		String header="";		for(String select: payload.getselect_variables()) {
			if(select.contains("0_")) select=select.substring(2);
			header=header+"  "+select;
			for(int i=0;i<14-select.length();i++) header=header+" ";
			divide=divide+addDiv;
		}
		System.out.println(divide); fileData.append(divide+"\r\n");
		System.out.println(header); fileData.append(header+"\r\n");
		System.out.println(divide); fileData.append(divide+"\r\n");
		String ansString="";
		DecimalFormat df = new DecimalFormat("#.####");
		for(OutputRow outputRow: outputResultSet) {
			String answer="";
			ansString=outputRow.getDBvar$prod();
			answer=answer+" "+ansString;
			for(int k=0;k<14-ansString.length();k++) answer=answer+" ";
			ansString = Integer.toString(outputRow.getDBvar$quant());
			for(int k=0;k<12-ansString.length();k++) answer=answer+" ";
			answer=answer+ansString+"    ";
			System.out.println(answer); fileData.append(answer+"\r\n");
		}
		FileOutputStream fos = new FileOutputStream("queryOutput/query7.txt");
		fos.write(fileData.toString().getBytes());
		fos.flush();
		fos.close();
	}

	public void process() throws IOException{
		ArrayList<InputRow> inputResultSet = createInputSet();
		if(payload.getIsWhereClause()) inputResultSet = executeWhereClause(inputResultSet);
		if(payload.getnumber_of_grouping_variables()>0) createListsBasedOnSuchThatPredicate(inputResultSet);
		computeAggregates(inputResultSet);
		ArrayList<OutputRow> outputResultSet = createOutputResultSet();
		printOutputResultSet(outputResultSet);
	}
}
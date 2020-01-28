package dbmsProject;

import java.util.HashMap;

public class Helper {
	
	public HashMap<String, Integer> columnMapping;
	
	public Helper() {
		generateMapping();
	}
	public void generateMapping(){
		columnMapping= new HashMap<String,Integer>();
		columnMapping.put("cust", 0);
		columnMapping.put("prod", 1);
		columnMapping.put("day", 2);
		columnMapping.put("month", 3);
		columnMapping.put("year", 4);
		columnMapping.put("state", 5);
		columnMapping.put("quant", 6);
		columnMapping.put("all", 7);
	}

	public boolean deleteRow(String operator, Integer temp1, Integer temp2) {
		if(operator.equalsIgnoreCase("=") && ((temp1).compareTo(temp2)!=0) ) return true;
		else if(operator.equalsIgnoreCase("<") && ((temp1).compareTo(temp2)>0)) return true;
		else if(operator.equalsIgnoreCase(">") && ((temp1).compareTo(temp2)<0)) return true;
		else if(operator.equalsIgnoreCase("<>")||operator.equalsIgnoreCase("><")||operator.equalsIgnoreCase("!=") && ((temp1).compareTo(temp2)==0)) return true;
		else if(operator.equalsIgnoreCase("<=") && ((temp1).compareTo(temp2)>0)) return true;
		else if(operator.equalsIgnoreCase(">=") && ((temp1).compareTo(temp2)<0)) return true;
		return false;
	}
	
	public boolean deleteRow(String operator, String temp1, String temp2) {
		if(operator.equalsIgnoreCase("=") && ((temp1).compareTo(temp2)!=0) ) return true;
		else if(operator.equalsIgnoreCase("<") && ((temp1).compareTo(temp2)>0)) return true;
		else if(operator.equalsIgnoreCase(">") && ((temp1).compareTo(temp2)<0)) return true;
		else if(operator.equalsIgnoreCase("<>")||operator.equalsIgnoreCase("><")||operator.equalsIgnoreCase("!=") && ((temp1).compareTo(temp2)==0)) return true;
		else if(operator.equalsIgnoreCase("<=") && ((temp1).compareTo(temp2)>0)) return true;
		else if(operator.equalsIgnoreCase(">=") && ((temp1).compareTo(temp2)<0)) return true;
		return false;
	}	

	public String getGroupingVariable(int i, SalesTableRow row) {
		switch(i) {
			case 0: return row.getcust();
			case 1: return row.getprod();
			case 2: return Integer.toString(row.getday());
			case 3: return Integer.toString(row.getmonth());
			case 4: return Integer.toString(row.getyear());
			case 5: return row.getstate();
			case 6: return Integer.toString(row.getquant());
			case 7: return "all";
		}
		return "__Garbage__";
	}
	
	
}

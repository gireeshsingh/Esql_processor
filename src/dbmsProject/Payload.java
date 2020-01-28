package dbmsProject;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class Payload{
	
	String fileName = "query7.txt";
	
	private int number_of_select_variables;
	private int number_of_grouping_variables;
	private int number_of_grouping_attributes;
	private int number_of_aggregate_functions;
	private int number_of_such_that_predicates;
	private int number_of_having_clause_predicates;
	private boolean isHavingClause;
	private boolean isWhereClause;
	private String whereClause;
	private String havingClause;
	
	private ArrayList<String> select_variables;
	private ArrayList<String> grouping_attributes;
	private ArrayList<String> aggregate_functions;
	private ArrayList<String> such_that_predicates;
	
	private Scanner scanner;
	
	private ArrayList<ArrayList<String>> groupingAttributesOfAllGroups;

	public ArrayList<ArrayList<String>> getGroupingAttributesOfAllGroups() {
		return groupingAttributesOfAllGroups;
	}

	public void setGroupingAttributesOfAllGroups(ArrayList<ArrayList<String>> groupingAttributesOfAllGroups) {
		this.groupingAttributesOfAllGroups = groupingAttributesOfAllGroups;
	}

	public String getWhereClause() {
		return whereClause;
	}

	public void setWhereClause(String whereClause) {
		this.whereClause = whereClause;
	}

	public boolean isHavingClause() {
		return isHavingClause;
	}

	public void setHavingClause(boolean isHavingClause) {
		this.isHavingClause = isHavingClause;
	}

	public String getHavingClause() {
		return havingClause;
	}

	public void setHavingClause(String havingClause) {
		this.havingClause = havingClause;
	}

	public void setWhereClause(boolean isWhereClause) {
		this.isWhereClause = isWhereClause;
	}

	public int getnumber_of_select_variables(){
		return number_of_select_variables;
	}
	
	public int getnumber_of_grouping_variables(){
		return number_of_grouping_variables;
	}
	
	public int getnumber_of_grouping_attributes(){
		return number_of_grouping_attributes;
	}
	
	public int getnumber_of_aggregate_functions(){
		return number_of_aggregate_functions;
	}
	
	public int getnumber_of_such_that_predicates(){
		return number_of_such_that_predicates;
	}
	
	public int getnumber_of_having_clause_predicates(){
		return number_of_having_clause_predicates;
	}
	
	public boolean getIsWhereClause(){
		return isWhereClause;
	}
	
	public ArrayList<String> getselect_variables(){
		return select_variables;
	}
	
	public ArrayList<String> getgrouping_attributes(){
		return grouping_attributes;
	}
	
	public ArrayList<String> getaggregate_functions(){
		return aggregate_functions;
	}
	
	public ArrayList<String> getsuch_that_predicates(){
		return such_that_predicates;
	}

	public Payload() throws FileNotFoundException {
		scanner = new Scanner(new File("queryInput/"+this.fileName));
		select_variables = new ArrayList<String>();
		groupingAttributesOfAllGroups = new ArrayList<ArrayList<String>>();
	}
	
	public void create() {
		
		//SELECT CLAUSE : get select attributes from user
		System.out.println("\nEnter the number of Select attributes(SELECT CLAUSE).");
		scanner.nextLine();
		number_of_select_variables=scanner.nextInt();
		select_variables = createStringList(number_of_select_variables);
		for(int i=0;i<number_of_select_variables;i++) {
			if(select_variables.get(i).matches("[a-zA-Z]+_.*")) {
				select_variables.set(i, "0_"+select_variables.get(i));
			}
		}
		
		//GROUP BY CLAUSE : get grouping attributes from user
		System.out.println("\nEnter the number of grouping variables (GROUP BY CLAUSE).");
		scanner.nextLine();scanner.nextLine();
		number_of_grouping_variables=scanner.nextInt();
		System.out.println("\nEnter the number of grouping attributes (GROUP BY CLAUSE).");
		scanner.nextLine();scanner.nextLine();
		number_of_grouping_attributes=scanner.nextInt();
		grouping_attributes = createStringList(number_of_grouping_attributes);
		
		//get aggregate functions from user
		System.out.println("\nEnter the number of aggregate functions/attributes.");
		scanner.nextLine();scanner.nextLine();
		number_of_aggregate_functions=scanner.nextInt();
		aggregate_functions=createStringList(number_of_aggregate_functions);
		for(int i=0;i<number_of_aggregate_functions;i++) {
			if(aggregate_functions.get(i).matches("[a-zA-Z]+.*")) {
				aggregate_functions.set(i, "0_"+aggregate_functions.get(i));
			}
		}
		
		//SUCH THAT CLAUSE : get predicates for grouping variables
		System.out.println("\nEnter the number of grouping variable predicates(SUCH THAT CLAUSE).");
		scanner.nextLine();scanner.nextLine();
		number_of_such_that_predicates=scanner.nextInt();
		such_that_predicates=createStringList(number_of_such_that_predicates);
		String zeroGroupPredicate=createZeroGroupPredicate();//"((0.cust=cust)and(0.state='NY'))";//
		such_that_predicates.add(0, zeroGroupPredicate);
		if(number_of_such_that_predicates>0) {
			for(int i=0;i<getsuch_that_predicates().size();i++) {
				such_that_predicates.set(i, refineString(such_that_predicates.get(i)));
			}
		}
		groupingAttributesOfAllGroups.add(new ArrayList<String>(grouping_attributes));
		for(int i=1;i<=number_of_grouping_variables;i++) {
			ArrayList<String> list=new ArrayList<String>();
			for(String select: grouping_attributes) {
				if(such_that_predicates.get(i).contains(select)) {
					list.add(select);
				}
			}
			groupingAttributesOfAllGroups.add(new ArrayList<String>(list));
		}
		
		//HAVING CLAUSE : get predicates for having clause
		System.out.println("\nDoes query contain HAVING clause? (YES=1; NO=0)");
		scanner.nextLine();scanner.nextLine();
		if(scanner.nextInt()==1) isHavingClause=true;
		else isHavingClause = false;
		if(isHavingClause) {
			System.out.println("Enter HAVING clause.");
			havingClause = refineString(scanner.next());
		}
		
		//WHERE CLAUSE : get predicates for where clause
		System.out.println("\nDoes query contain WHERE clause.(YES=1; NO=0)");
		scanner.nextLine();scanner.nextLine();
		if(scanner.nextInt()==1) isWhereClause=true;
		else isWhereClause = false;
		if(isWhereClause) {
			System.out.println("Enter WHERE clause.");
			whereClause = refineString(scanner.next());
		}
	}
	
	public String createZeroGroupPredicate() {
		String str="";
		for(int i=0;i<grouping_attributes.size();i++) {
			if(i==0) {
				str="(0."+grouping_attributes.get(i)+"="+grouping_attributes.get(i)+")";
			}
			//else str = "(("+0+"."+grouping_attributes.get(i)+"="+grouping_attributes.get(i)+")"+"and"+str+")";
			else str = "("+str+"and(0."+grouping_attributes.get(i)+"="+grouping_attributes.get(i)+"))";
		}
		 return str;
	}
	
	public ArrayList<String> createStringList(int count) {
		int i=0;
		ArrayList<String> stringList=new ArrayList<String>();
		while(i<count){
			System.out.print("Enter attribute number "+(i+1)+": ");
			stringList.add(refineString(scanner.next()));
			i++;
		}
		return stringList;
	}
	
	public String refineString(String input) {
		input = input.replaceAll("\\)or\\(",")|(");
		input = input.replaceAll("\\)and\\(",")&(");
		input = input.replaceAll("!=","!");
		input = input.replaceAll("<=","≤");
		input = input.replaceAll(">=","≥");
		input = input.replaceAll("><","!");
		input = input.replaceAll("<>","!");
		return input;
	}
}
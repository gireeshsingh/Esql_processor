package dbmsProject;

import java.util.Stack;

public class ExpTree {	
	
	public class TreeNode{
		String nodeStr;
		TreeNode left;
		TreeNode right;
		TreeNode(String nodeStr){
			this.nodeStr=nodeStr;
		}
	}
	
	public String execute(String exp) {
		Stack<TreeNode> operand =new Stack<TreeNode>();
		Stack<TreeNode> operator =new Stack<TreeNode>();
		String current="";
		for(String s: exp.split("")) {
			if(s.equals("(")) continue;
			else if(s.equals("*") || s.equals("+") || s.equals("-") || s.equals("/") || s.equals("!")
					|| s.equals("|") || s.equals("&") || s.equals("<") ||s.equals(">") || s.equals("=")  
					|| s.equals("!") || s.equals("≥") || s.equals("≤")){
				if(current.length()>0) {
					TreeNode tn=new TreeNode(current);
 					operand.push(tn);
				}
				TreeNode tn = new TreeNode(s);
				operator.push(tn);
				current="";
			}
			else if(s.equals(")")) {
				if(current.length()>0) {
					TreeNode tn=new TreeNode(current);
					operand.push(tn);
				}
				TreeNode op = operator.pop();
				TreeNode right =  operand.pop();
				TreeNode left = operand.pop();
				op.left=left;
				op.right=right;
				operand.push(op);
				current="";
			}
			else current=current+s;
		}
		TreeNode root = operand.pop();
		return evaluateExpTree(root);
	}
	
	public String evaluateExpTree(TreeNode root) {
		String ans="";
		if(root==null) return "";
		if(root.left==null) return root.nodeStr;
		String operand1=evaluateExpTree(root.left);
		String operand2=evaluateExpTree(root.right);
		ans = solve(operand1, root.nodeStr, operand2);
		return ans;
	}
	
	public String solve(String a, String operator, String b) {
		switch(operator) {
			case("+"): 	return Double.toString(Double.parseDouble(a)+Double.parseDouble(b));
			case("-"):	return Double.toString(Double.parseDouble(a)-Double.parseDouble(b));
			case("*"): 	return Double.toString(Double.parseDouble(a)*Double.parseDouble(b));
			case("/"):	return Double.toString(Double.parseDouble(a)/Double.parseDouble(b));
			case("|"): 	return String.valueOf(Boolean.valueOf(a)|Boolean.valueOf(b));
			case("&"):	return String.valueOf(Boolean.valueOf(a)&Boolean.valueOf(b));
			case("≥"):	{
				try {
					return Boolean.toString(Double.parseDouble(a)>=Double.parseDouble(b));
				} catch(NumberFormatException e) {
					return String.valueOf(a.compareTo(b)>=0);
				}
			}
			case("≤"):	{
				try {
					return Boolean.toString(Double.parseDouble(a)<=Double.parseDouble(b));
				} catch(NumberFormatException e) {
					return String.valueOf(a.compareTo(b)<=0);
				}
			}
			case("="): 	{
				try {
					return Boolean.toString((int)Double.parseDouble(a)==(int)Double.parseDouble(b));
				} catch(NumberFormatException e) {
					return String.valueOf(a.equals(b));
				}
			}
			case(">"): 	{
				try {
					return Boolean.toString(Double.parseDouble(a)>Double.parseDouble(b));
				} catch(NumberFormatException e) {
					return String.valueOf(a.compareTo(b)>0);
				}
			}
			case("<"):	{
				try {
					return Boolean.toString(Double.parseDouble(a)<Double.parseDouble(b));
				} catch(NumberFormatException e) {
					return String.valueOf(a.compareTo(b)<0);
				}
			}
			case("!"):	{
				try {
					return Boolean.toString(Double.parseDouble(a)!=Double.parseDouble(b));
				} catch(NumberFormatException e) {
					return String.valueOf(!(a.equals(b)));
				}
				
			}
		}
		return "_Garbage_";
	}
	
	public void preOrder(TreeNode root) {
		if(root==null) return;
		preOrder(root.left);
		System.out.println(root.nodeStr);
		preOrder(root.right);
	}

}

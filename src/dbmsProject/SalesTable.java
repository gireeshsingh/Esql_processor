package dbmsProject;

import java.sql.*;
import java.util.ArrayList;

public class SalesTable{
	
	private ResultSet rs=null;
	private Connection conn=null;
	private ArrayList<SalesTableRow> resultSet;
	
	public ArrayList<SalesTableRow> getResultSet(){
		return resultSet;
	}
	
	public SalesTable(Connection conn){
		this.conn=conn;
	}
	
	//populates the ResultSet
	public void populateResultSet(){
		try{
			Statement st=conn.createStatement();
			String query="select * from sales";
			rs=st.executeQuery(query);
			resultSet=new ArrayList<SalesTableRow>();

			while(rs.next()) {
				SalesTableRow row = new SalesTableRow();
				row.setcust(rs.getString(1));
				row.setprod(rs.getString(2));
				row.setday(rs.getInt(3));
				row.setmonth(rs.getInt(4));
				row.setyear(rs.getInt(5));
				row.setstate(rs.getString(6));
				row.setquant(rs.getInt(7));
				resultSet.add(row);
			}
			
		}catch(SQLException e) {
			System.out.println("SQLException: "+e.getMessage());
			System.out.println("SQLState: "+e.getSQLState());
			System.out.println("VendorError: "+e.getErrorCode());
			e.printStackTrace();
		}
	}
	
	public void displayResults(){
		System.out.printf("%-8s","Customer  ");             //left aligned
		System.out.printf("%-7s","Product  ");              //left aligned
		System.out.printf("%-5s","Day    " +"");                //left aligned
		System.out.printf("%-10s","Month    ");          //left aligned
		System.out.printf("%-5s","Year   ");                //left aligned
		System.out.printf("%-10s","State    ");          //left aligned
		System.out.printf("%-5s%n","Quant  ");              //left aligned
		System.out.println("========  =======  =====  ========  =====  ========  =====");
		
		for(SalesTableRow row: resultSet) {
			System.out.printf("%-8s  ",row.getcust());            //left aligned
			System.out.printf("%-7s  ",row.getprod());            //left aligned
			System.out.printf("%5s  ",row.getday());             //right aligned
			System.out.printf("%8s  ",row.getmonth());            //right aligned
			System.out.printf("%5s  ",row.getyear());             //right aligned
			System.out.printf("%-8s  ",row.getstate());            //right aligned
			System.out.printf("%5s%n",row.getquant());   //right aligned	
		}
	}

}
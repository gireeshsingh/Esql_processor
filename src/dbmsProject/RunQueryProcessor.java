package dbmsProject;

import java.io.IOException;
import java.sql.Connection;

public class RunQueryProcessor {	
	public static void main(String args[]) throws IOException, ClassNotFoundException {
		//Accept user query
		Payload payload=new Payload();
		payload.create();
		Credential cred=new Credential();
		
		//create connection to Database
		DBConnection dbconnection = new DBConnection(cred);
		dbconnection.createConnection();
		Connection conn=dbconnection.getConnectionToSales();
		
		//read Sales Table from DB
		SalesTable salesTable = new SalesTable(conn);
		salesTable.populateResultSet();
		
		//Run Query
		QueryProcessor qp = new QueryProcessor(salesTable, payload);
		qp.process();
	}
}

package dbmsProject;

import java.sql.*;

public class DBConnection{
	private Connection conn = null;
	private Credential cred;
	
	public Connection getConnectionToSales(){
		return conn;
	}
	
	public DBConnection(Credential cred){
		this.cred=cred;
	}
	
	public void createConnection() throws ClassNotFoundException{
        try {
			Class.forName(cred.getDriver());
			conn=DriverManager.getConnection(cred.getUrl(),cred.getUsername(),cred.getPassword());
			System.out.println("Success loading Driver!");
        } catch(SQLException e) {
			System.out.println("SQLException: "+e.getMessage());
			System.out.println("SQLState: "+e.getSQLState());
			System.out.println("VendorError: "+e.getErrorCode());
			e.printStackTrace();
        }
    }
}
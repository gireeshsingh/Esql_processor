package dbmsProject;

public class Credential{
	
	private String username ="postgres";
    private String password	="okokok";//"oltp!562%OLAP"
    private String url 		="jdbc:postgresql://localhost:5432/postgres";
	private String driver	="org.postgresql.Driver";
	
	public String getUsername(){
		return username;
	}
	
	public String getPassword(){
		return password;
	}
	
	public String getUrl(){
		return url;
	}
	
	public String getDriver(){
		return driver;
	}
	
	public void setDriver(String driver){
		this.driver=driver;
	}
}
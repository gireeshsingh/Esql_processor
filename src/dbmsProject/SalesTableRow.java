package dbmsProject;

public class SalesTableRow{
	private String cust;
	private String prod;
	private int day;
	private int month;
	private int year;
	private String state;
	private int quant;
	public String getcust() {
		return cust;
	}
	public void setcust(String cust) {
		this.cust = cust;
	}
	public String getprod() {
		return prod;
	}
	public void setprod(String prod) {
		this.prod = prod;
	}
	public int getday() {
		return day;
	}
	public void setday(int day) {
		this.day = day;
	}
	public int getmonth() {
		return month;
	}
	public void setmonth(int month) {
		this.month = month;
	}
	public int getyear() {
		return year;
	}
	public void setyear(int year) {
		this.year = year;
	}
	public String getstate() {
		return state;
	}
	public void setstate(String state) {
		this.state = state;
	}
	public int getquant() {
		return quant;
	}
	public void setquant(int quant) {
		this.quant = quant;
	}
}

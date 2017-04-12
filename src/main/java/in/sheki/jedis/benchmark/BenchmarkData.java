package in.sheki.jedis.benchmark;

import java.util.ArrayList;
import java.util.List;

public class BenchmarkData {

	private String key;
	private List<String> fieldList = new ArrayList<String>();
	
	public BenchmarkData(String key) {
		this.key = key;
	}
	
	public void setKey(String key) {
		this.key = key;
	}
	
	public String getKey() {
		return this.key;
	}
	
	public void addField(String field) {
		this.fieldList.add(field);
	}
	
	public List<String> getFiledList() {
		return this.fieldList;
	}
}

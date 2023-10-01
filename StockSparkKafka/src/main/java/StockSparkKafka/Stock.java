package StockSparkKafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;

public class Stock implements Serializable{

	private static final long serialVersionUID = 154265L;

	public List<Tuple2<String,String>> keywordList;
	
	public Stock()
	{
		this.keywordList=new ArrayList<Tuple2<String,String>>(); 
	}
	
	public String GetStatement()
	{
		if(this.keywordList.isEmpty())
			return "";
		
		return this.keywordList.stream().map(t -> t._1()).collect(Collectors.joining(","));
	}
	
	public String GetKeywordsFound()
	{
		if(this.keywordList.isEmpty())
			return "";
		
		return this.keywordList.stream().map(t -> t._2()).collect(Collectors.joining(","));
	}
	
	@Override
	public String toString()
	{
		return  "|" + this.GetStatement()+"|"+this.GetKeywordsFound();
	}
}

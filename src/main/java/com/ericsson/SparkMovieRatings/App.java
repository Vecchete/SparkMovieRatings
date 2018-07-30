package com.ericsson.SparkMovieRatings;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.api.java.*;
import java.util.*;
import org.apache.spark.api.java.function.Function;
import java.lang.*;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.FileUtils;
import java.util.Iterator;

public class App{
	
	public App(){
		
	}
	
	private Map<Integer,String> loadMovieNames(){
		int count; 
		Map<Integer,String> movieNames = new HashMap<Integer,String>();
		movieNames.clear();
		count = 1;
		File file = new File("/home/hadoop/Documents/Spark/u.item");
		try{
			LineIterator it = FileUtils.lineIterator(file, "UTF-8");
			try{
				while (it.hasNext()) {
					String line = it.nextLine();
					//do operation in line
					String[] fields = line.split("\\|");
					movieNames.put(count,fields[1]);
					count = count + 1;
				}
			}finally{
				LineIterator.closeQuietly(it);
			}
		}catch(IOException e){
			e.printStackTrace();
		}
		return movieNames;
	}
		
	public static void main(String[] args){
		App obj = new App();
		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example").getOrCreate();
		
		Map<Integer,String> movieNames = obj.loadMovieNames();
		JavaRDD<String> lines = spark.read().textFile("hdfs:///ml-100k/u.data").javaRDD();
		JavaRDD<Row> movies = lines.map(new Function<String,Row>(){
			public Row call(String line) throws Exception{
				String[] fields = line.split(" ");
				int movieID = Integer.parseInt(fields[1]);
				float rating = Float.parseFloat(fields[2]);
				List<Object> row = new ArrayList<Object>();
				row.add(movieID);
				row.add(rating);
				Row rowrdd = RowFactory.create(row);
				return rowrdd;
			}
		});
		
		StructType schema = new StructType(new StructField[]{
			new StructField("movieID",DataTypes.IntegerType,false,Metadata.empty()),
			new StructField("rating",DataTypes.FloatType,false,Metadata.empty())
		});
		
		
		Dataset<Row> movieDataset = spark.createDataFrame(movies, schema);
		Dataset<Row> averageRatings = movieDataset.groupBy("movieID").avg("rating");
		Dataset<Row> counts = movieDataset.groupBy("movieID").count();
		Dataset<Row> averagesAndCounts = counts.join(averageRatings, "movieID");
		Dataset<Row> relationAVGCount = averagesAndCounts.withColumn("ratio", ((averagesAndCounts.col("count").divide(100000)).	multiply(averagesAndCounts.col("avg(rating)"))).plus(averagesAndCounts.col("avg(rating)")));
		
		Dataset<Row>topTen = relationAVGCount.sort(relationAVGCount.col("ratio").desc()).filter((relationAVGCount.col("count")).geq(27));
		//Dataset<Row>top = topTen.take(10);
		
		//Iterator<Row> rowsAsIterator = topTen.toLocalIterator();
/*		while (rowsAsIterator.hasNext()) {
			Row it = rowsAsIterator.next();
			System.out.println(it.mkString());
		}*/
		
		String result = movieNames.get(1);
		System.out.println("Eu tentei "+ result);
		
		
		//stop the session
		spark.stop();
	}
}

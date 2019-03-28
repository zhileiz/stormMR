package edu.upenn.cis455.mapreduce.job;

import java.util.Iterator;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job {

	/**
	 * This is a method that lets us call map while recording the StormLite source executor ID.
	 * 
	 */
	public void map(String key, String value, Context context, String sourceExecutor)
	{
		// Your map function for WordCount goes here
	}

	/**
	 * This is a method that lets us call map while recording the StormLite source executor ID.
	 * 
	 */
	public void reduce(String key, Iterator<String> values, Context context, String sourceExecutor)
	{
		// Your reduce function goes here
	}

}

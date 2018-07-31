package com.ericsson.SparkMovieRatings;

public class MovieRatings {
	
	private int movieID;
	private int rating;
	
	public MovieRatings() {
	}
	
	public void setMovieID(int MovieID) {
		this.movieID = MovieID;
	}
	
	public void setRating(int rating) {
		this.rating = rating;
	}
	
	
	public int getMovieID() {
		return this.movieID;
	}
	
	public int getRating() {
		return this.rating;
	}

}

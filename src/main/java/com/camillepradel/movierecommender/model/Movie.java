package com.camillepradel.movierecommender.model;

import java.util.List;

public class Movie {
    private static int ID = 0;
    private int id;
    private String title;
    private List<Genre> genres;
    
    public Movie(String title){
        id = ID;
        this.title = title;
        ID = ID + 1;
    }

    public Movie(int id, String title, List<Genre> genres) {
        this.id = id;
        this.title = title;
        this.genres = genres;
    }

    public int getId() {
        return this.id;
    }

    public String getTitle() {
        return this.title;
    }

    public List<Genre> getGenres() {
        return this.genres;
    }
}

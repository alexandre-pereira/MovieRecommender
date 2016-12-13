package com.camillepradel.movierecommender.model;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

public class Rating {

    private Movie movie;
    private int userId;
    private int score;

    public Rating() {
        this.movie = null;
        this.userId = 0;
        this.score = 0;
    }

    public Rating(Movie movie, int userId, int score) {
        this.movie = movie;
        this.userId = userId;
        this.score = score;
    }

    public Rating(Movie movie, int userId) {
        this.movie = movie;
        this.userId = userId;
        this.score = 0;
    }

    public Movie getMovie() {
        return this.movie;
    }

    public void setMovie(Movie movie) {
        this.movie = movie;
    }

    public int getMovieId() {
        return this.movie.getId();
    }

    public void setMovieId(int movieId) {
                        System.out.println("dsfdsf");

        if(DataManager.TYPE.equals("MONGODB")) {
            /*MongoClient mongoClient = DataManager.getMongoClient();
            DB db = mongoClient.getDB("MovieLens");
            DBCollection movies = db.getCollection("movies");
            BasicDBObject query = new BasicDBObject();
            DBObject doc;
            query.put("_id",movieId);
            doc = movies.findOne(query);
            DBCollection genres = mongo.getDB("MovieLens").getCollection("genres");
            BasicDBObject g;
            BasicDBObject query = new BasicDBObject();

            for(String tmp : s.split("|")){
                query.clear();

                query.put("name", tmp);
                g = (BasicDBObject) genres.findOne(query);

                if(g != null){
                    list.add(new Genre((Integer)g.get("id"), g.getString("name")));
                }
            }
            this.movie = new Movie( movieId, (String)doc.get("title"), Genre.getListGenre((String)doc.get("genres")));*/
        } else {
            Session neoClient = DataManager.getNeoClient();
                                    System.out.println("b");

            StatementResult result = neoClient.run( "MATCH (m:Movie {id:" + movieId +"})-->(g:Genre) RETURN m.title, g.id, g.name;" );
            Record record = null;
            List<Genre> genres = new ArrayList<>();                        System.out.println("dsfdsf");

            while (result.hasNext()) {
                                        System.out.println("a");

                record = result.next();
                System.out.println("c");
                genres.add(new Genre(record.get("g.id").asInt(),record.get("g.name").asString()));
            }
                            System.out.println("d");

            this.movie = new Movie( movieId, (String)record.get("m.title").asString(), genres);
        }
    }

    public int getUserId() {
        return this.userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getScore() {
        return this.score;
    }

    public void setScore(int score) {
        this.score = score;
    }
}
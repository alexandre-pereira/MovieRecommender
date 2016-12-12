package com.camillepradel.movierecommender.controller;

import com.camillepradel.movierecommender.model.DataManager;
import java.util.LinkedList;
import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.bind.annotation.RequestMethod;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.*;
import java.util.ArrayList;
import java.util.Date;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

@Controller
public class MainController {

    @RequestMapping("/movies")
    public ModelAndView showMovies(@RequestParam(value = "user_id", required = false) Integer userId) {
        List<Movie> moviesList = new LinkedList<Movie>();
        
        if(DataManager.TYPE.equals("MONGODB")) {
            MongoClient mongoClient = DataManager.getMongoClient();
            DB db = mongoClient.getDB("MovieLens");
            DBCollection ratings = db.getCollection("ratings");
            DBCollection movies = db.getCollection("movies");
            BasicDBObject query = new BasicDBObject();
            DBObject doc;

            if(userId != null) {
                query.put("user_id", userId);
            }
            DBCursor cursor = ratings.find(query);
            Movie m;

            while(cursor.hasNext()){
                doc = cursor.next();
                query.clear();
                query.put("_id", doc.get("mov_id"));
                doc = movies.findOne(query);
                if(doc != null){
                    m = new Movie((String) doc.get("title"));
                    moviesList.add(m);
                }
            }
            DataManager.closeMongoClient();
        } else {
            Session neoClient = DataManager.getNeoClient();
            StatementResult result;
            if(userId != null) {
                result = neoClient.run( "MATCH (u:User {id:" + userId +"})-->(m:Movie) RETURN m.title;" );
            } else {
                result = neoClient.run( "MATCH (m:Movie) RETURN m.title;" );
            }
            Movie m;
            Record record;
            while (result.hasNext()) {
                record = result.next();
                m = new Movie((String) record.get("m.title").asString());
                moviesList.add(m);
            }
            DataManager.closeNeoClient();
        }
        ModelAndView mv = new ModelAndView("movies");
        mv.addObject("userId", userId);
        mv.addObject("movies", moviesList);
        return mv;
    }

    @RequestMapping(value = "/movieratings", method = RequestMethod.GET)
    public ModelAndView showMoviesRattings(@RequestParam(value = "user_id", required = true) Integer userId) {
        List<Movie> allMovies = new LinkedList<Movie>();
        List<Rating> ratingsList = new LinkedList<Rating>();
        if(DataManager.TYPE.equals("MONGODB")) {
            MongoClient mongoClient = DataManager.getMongoClient();
            DB db = mongoClient.getDB("MovieLens");
            DBCollection ratings = db.getCollection("ratings");
            DBCollection movies = db.getCollection("movies");
            BasicDBObject query = new BasicDBObject();
            DBObject docR;
            DBObject docM;
            DBCursor cursor;

            query.put("user_id", userId);
            cursor = ratings.find(query);
            Movie m;
            List<Genre> genreList = new ArrayList<Genre>();

            while(cursor.hasNext()){
                docR = cursor.next();
                query.clear();
                query.put("_id", docR.get("mov_id"));
                docM = movies.findOne(query);
                if(docM != null){
                    ratingsList.add(
                        new Rating( 
                            new Movie(
                                (Integer)docM.get("_id"), 
                                "Titre 0", 
                                genreList
                            ),
                            userId,
                            (Integer)docR.get("rating")
                        )
                    );
                    allMovies.add(new Movie((String) docM.get("title")));
                }
            }
            DataManager.closeMongoClient();
        } else {
            Session neoClient = DataManager.getNeoClient();
            StatementResult result;
            result = neoClient.run( "MATCH (u:User {id:" + userId +"})-[r:RATED]->(m:Movie) RETURN m.title, m.id, r.note;" );
            
            
            Movie m;
            Record record;
            while (result.hasNext()) {
                record = result.next();
                m = new Movie(record.get("m.id").asInt(), (String) record.get("m.title").asString(), null);
                
                allMovies.add(m);

                if(record.get("r.note").asInt() > 0) {
                    ratingsList.add(
                    new Rating( 
                        m,
                        userId,
                        record.get("r.note").asInt()
                    )
                );
                }
            }
            DataManager.closeNeoClient();
        }
        ModelAndView mv = new ModelAndView("movieratings");
        mv.addObject("userId", userId);
        mv.addObject("allMovies", allMovies);
        mv.addObject("ratings", ratingsList);
        return mv;
    }

    @RequestMapping(value = "/movieratings", method = RequestMethod.POST)
    public String saveOrUpdateRating(@ModelAttribute("rating") Rating rating) {
        if(DataManager.TYPE.equals("MONGODB")) {
            MongoClient mongoClient = DataManager.getMongoClient();
            DB db = mongoClient.getDB("MovieLens");
            DBCollection ratings = db.getCollection("ratings");
            BasicDBObject query = new BasicDBObject();
            BasicDBObject results = (BasicDBObject) ratings.findOne(query);

            if(results.isEmpty()){
                query.put("user_id",rating.getUserId());
                query.put("mov_id",rating.getMovieId());
                query.put("rating",rating.getScore());
                query.put("timestamp",new java.util.Date().getTime());
                ratings.insert(query);
            } else {
                query.put("rating",rating.getScore());
                ratings.update(results, query);
            } 
            DataManager.closeMongoClient();
        } else {
            Session neoClient = DataManager.getNeoClient();
            int timestamp = (int) ((new Date()).getTime()/1000);
            neoClient.run("MERGE (u:User{ id:"+rating.getUserId()+" })-[r:RATED]->(m:Movie{id:"+rating.getMovieId()+"}) ON CREATE SET r = { note: "+rating.getScore()+", timestamp: "+timestamp+"} ON MATCH  SET r += { note: "+rating.getScore()+", timestamp: "+timestamp+"}");
            DataManager.closeNeoClient();
        }
        return "redirect:/movieratings?user_id=" + rating.getUserId();
    }
    
    @RequestMapping(value = "/recommendations", method = RequestMethod.GET)
    public ModelAndView ProcessRecommendations(@RequestParam(value = "user_id", required = true) Integer userId, @RequestParam(value = "processing_mode", required = false, defaultValue = "0") Integer processingMode){
        List<Rating> recommendations = new ArrayList<Rating>();
        
        if(DataManager.TYPE.equals("MONGODB") && 
                processingMode.equals(0)) {
            //Les recommandations ont été implémenté qu'avec la première variante
            List<Integer> userMovieList = new ArrayList<Integer>();
            MongoClient mongoClient = DataManager.getMongoClient();
            DB db = mongoClient.getDB("MovieLens");
            DBCollection users = db.getCollection("users");
            BasicDBObject query = new BasicDBObject();
            BasicDBObject projection = new BasicDBObject();
            DBObject doc;
            DBCursor cursor;

            query.put("user_id", userId);
            projection.put("movies.movieid",1);
            cursor = users.find(query,projection);

            while(cursor.hasNext()){
                doc = cursor.next();
                
                userMovieList.add((Integer)doc.get("movieid"));
            }
            
            // Unwind
            BasicDBObject unwind = new BasicDBObject("$unwind","$movies");
            // Project
            BasicDBObject projectParametersQuery = 
                    new BasicDBObject("_id", "$movies.movieid")
                            .append("userId","$_id");
 
            BasicDBObject projectQuery = new BasicDBObject("$project", projectParametersQuery);

            // Match
            BasicDBList userMovieIdList = new BasicDBList();
            userMovieList.forEach( m -> userMovieIdList.add(new BasicDBObject("_id",m)));

            DBObject orQuery = new BasicDBObject("$in", userMovieIdList);
            orQuery.put("userId", new BasicDBObject("$ne",userId));
            DBObject match = new BasicDBObject("$match",orQuery);
            // Group
            DBObject groupParameterQuery = new BasicDBObject("_id","$userId");
            groupParameterQuery.put("nbIteration", new BasicDBObject("$sum", 1));
            DBObject group = new BasicDBObject("$group", groupParameterQuery);
            // Sort
            DBObject sort = new BasicDBObject("$sort", new BasicDBObject("nbIteration", -1));
            // Limit
            DBObject limit = new BasicDBObject("$limit",1);

            Iterable<DBObject> result = users.aggregate(unwind,projectQuery,match, group, sort, limit).results();
            
            System.out.println(result.toString());
            
       } else {
            Session neoClient = DataManager.getNeoClient();
            /* Variante 1
            StatementResult result = neoClient.run("MATCH (target_user:User {id : " +userId+ "})-[:RATED]->(m:Movie)<-[:RATED]-(other_user:User) " +
            "WITH other_user, count(distinct m.title) AS num_common_movies, target_user " +
            "ORDER BY num_common_movies DESC " +
            "LIMIT 1 " +
            "MATCH (other_user:User)-[rat_other_user:RATED]->(m2:Movie) " +
            "WHERE NOT ((target_user:User)-[:RATED]->(m2:Movie)) " +
            "RETURN m2.title AS rec_movie_title, rat_other_user.note AS rating, " +
            "other_user.id AS watched_by " +
            "ORDER BY rat_other_user.note DESC"); */
            
            /* Variante 2 */
            StatementResult result = neoClient.run("MATCH (target_user:User {id : " +userId+ "})-[:RATED]->(m:Movie)<-[:RATED]-(other_user:User) " +
            "WITH other_user, count(distinct m.title) AS num_common_movies, target_user " +
            "ORDER BY num_common_movies DESC " +
            "LIMIT 5 " +
            "MATCH (other_user:User)-[rat_other_user:RATED]->(m2:Movie) " +
            "WHERE NOT ((target_user:User)-[:RATED]->(m2:Movie)) " +
            "RETURN m2.title AS rec_movie_title, rat_other_user.note AS rating, " +
            "other_user.id AS watched_by " +
            "ORDER BY rat_other_user.note DESC");
  
            Record record;
            while (result.hasNext()) {
                record = result.next();
                recommendations.add(new Rating(new Movie(1, record.get("rec_movie_title").asString(), null), record.get("watched_by").asInt(), record.get("rating").asInt()));
            }
       }
                
        /*MATCH (me:User{ id: 4})-->(movie:Movie)<--(him:User)
        WITH me, him, count(him.id) as commonRelations
        RETURN me, him, commonRelations ORDER BY commonRelations DESC LIMIT 1;*/



        ModelAndView mv = new ModelAndView("recommendations");
        mv.addObject("recommendations", recommendations);

        return mv;
    }

}
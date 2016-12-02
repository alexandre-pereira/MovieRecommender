/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.camillepradel.movierecommender.model;

import com.mongodb.MongoClient;
import org.neo4j.driver.v1.*;

public class DataManager
{
    public static final String TYPE = "NEO4J";
    private static MongoClient mongoClient;
    private static Session neoClient;
    private static Driver neoDriver;
   
    /** Constructeur privé */
    private DataManager() {
    }

    /** Instance unique pré-initialisée */
    private static DataManager INSTANCE = new DataManager();
    /** Point d'accès pour l'instance unique du singleton
     * @return  */
    public static DataManager getInstance()
    {	
        return INSTANCE;
    }
    
    public static MongoClient getMongoClient() {
        if(DataManager.mongoClient == null) { 
            DataManager.mongoClient = new MongoClient( "localhost" , 27017 ); 
        }
        return DataManager.mongoClient; 
    }
    
    public static void closeMongoClient(){
        DataManager.mongoClient.close();
        DataManager.mongoClient = null;
    }
    
    public static Session getNeoClient() {
        if(DataManager.neoClient == null) {
            try {
                DataManager.neoDriver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "azerty"));
                System.out.println(DataManager.neoDriver);
                DataManager.neoClient = DataManager.neoDriver.session();
                System.out.println(DataManager.neoClient);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
        return DataManager.neoClient;
    }
    
    public static void closeNeoClient(){
        DataManager.neoClient.close();
        DataManager.neoDriver.close();
        DataManager.neoDriver = null;
        DataManager.neoClient = null;
    }
}
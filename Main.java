import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.Arrays;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        MongoDB db = new MongoDB("localhost", 27022, "airbnb", "ratings");
        System.out.println("============Welcome to Airbnb Analytics============\n");
        displayOptions();
        Scanner in = new Scanner(System.in);
        while(in.hasNextInt()){
            int input = in.nextInt();
            switch (input){
                case 1: { //Find the average price of Airbnb listings group by country
                    System.out.println("YOU SELECTED: AVERAGE PRICE OF AIRBNB LISTINGS");
                    db.averagePriceByCountry();
                    break;
                }
                case 2: { //Find the listings and city with the most Airbnb reviews
                    break;
                }
                case 3: { //Find the oldest listings
                    System.out.println("YOU SELECTED: OLDEST LISTINGS");
                    db.findOldestHost();
                    break;
                }
                case 4: { // Rank countries based on Airbnb ratings
                    break;
                }
                case 5: { //Find top 5 cities with the most listings
                    break;
                }
                case 6: { //Find top 10 listings that has the highest reviews per month
                    break;
                }
                case 7: { //Show the number of hosts and super hosts in different countries in 2017
                    break;
                }
                case 8: { //Find the most popular and highly-rated Airbnb listing based on location
                    break;
                }
                case 9: { //Find listings that has a specific check-in or check-out time in a city
                    break;
                }
                case 10: { //Find listings that accommodates a specified duration in a city
                    break;
                }
                case 11: { //Find a specific host that satisfy user's demand for transit, house-rule, interaction
                    break;
                }
                case 12: { //Find top 10 places to stay in a specific city and under a specific price
                    break;
                }
                case 13: { //Find top 10 closest airbnb listings to a specific zipcode
                    break;
                }
                case 14: { //Find top 10 closest airbnb listings to a specific zipcode that has specified room type
                    break;
                }
                case 15: { //Find top 10 closest airbnb listings to a specific location that accommodates a number of users
                    break;
                }
                case 0: {
                    System.exit(0);
                }
                default:{
                    System.out.println("Please select option from 1-15");
                    displayOptions();
                    break;
                }
            }
        }
    }

    public static void displayOptions(){
        System.out.println("Please choose one of the following options or 0 to EXIT:");

        System.out.println("============General Statistic============"); //Does not require further input
        System.out.println("[1] Find the average price of Airbnb listings group by country"); //Nhu
        System.out.println("[2] Find the listings and city with the most Airbnb reviews");
        System.out.println("[3] Find cities and countries that has the earliest host"); //Nhu
        System.out.println("[4] Rank countries based on Airbnb ratings");
        System.out.println("[5] Find top 5 cities with the most listings");
        System.out.println("[6] Find top 10 listings that has the highest reviews per month"); //Nhu
        System.out.println("[7] Show the number of hosts and super hosts in different countries in 2017\n"); //Nhu


        System.out.println("============Exploration============"); //based on location - needs user's input
        System.out.println("[8] Find the most popular and highly-rated Airbnb listing based on location"); //Sinjin
        System.out.println("[9] Find listings that has a specific check-in or check-out time in a city");
        System.out.println("[10] Find listings that accommodates a specified duration in a city");
        System.out.println("[11] Find a specific host that satisfy user's demand for transit, house-rule, interaction and etc. "); //Sinjin
        System.out.println("[12] Find top 10 places to stay in a specific city and under a specific price"); //Sinjin
        System.out.println("[13] Find top 10 closest airbnb listings to a specific zipcode"); //Nhu
        System.out.println("[14] Find top 10 closest airbnb listings to a specific zipcode that has specified room type"); //Nhu
        System.out.println("[15] Find top 10 closest airbnb listings to a specific location that accommodates a number of users"); //Nhu

        System.out.print("Your Selection: ");
    }

}


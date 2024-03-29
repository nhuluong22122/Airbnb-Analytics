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
                    System.out.println("YOU SELECTED: Average price per month group by country");
                    db.findAveragePriceByCountry();
                    break;
                }
                case 2: { //Find the listings and city with the most Airbnb reviews
                    System.out.println("YOU SELECTED: Find the listings and city with the most Airbnb reviews");
                    db.findMostAirbnbReviews();
                    break;
                }
                case 3: { //Find the oldest listing
                    System.out.println("YOU SELECTED: Top 20 oldest listings");
                    db.findOldestListings();
                    break;
                }
                case 4: { // Rank countries based on Airbnb ratings
                    System.out.println("YOU SELECTED: Rank countries based on Airbnb ratings");
                    db.rankCountries();
                    break;
                }
                case 5: { //Find top 5 cities with the most listings
                    System.out.println("YOU SELECTED: Find top 5 cities with the most listings");
                    db.findMostListings();
                    break;
                }
                case 6: { //Find top 10 listings that has the highest reviews per month
                    System.out.println("YOU SELECT: Top 10 listings that has the highest reviews per month");
                    db.findHighestReviewPerMonth();
                    break;
                }
                case 7: { //Show the number of hosts and super hosts in different countries in 2017
                    System.out.println("YOU SELECT: Find the number of hosts and super hosts in different countries in 2017");
                    db.findHosts();
                    break;
                }
                case 8: { //Find the most popular and highly-rated Airbnb listing based on location
                	System.out.println("YOU SELECT: Find the most popular and highly-rated Airbnb listing based on location");
                	System.out.println("ENTER City");
                	  String line = "";
                	  String line1="";
                    int max = 0;
                    int min = 0;
                    while(in.hasNextLine()){
                        line = in.nextLine();
                        if(!line.equals("")){
                            System.out.println("Enter Country");
                            
                            if(in.hasNextLine()){
                            	line1=in.nextLine();
                                if(!line.equals("")){
                                   db.findMostPopularandHighlyRatedBasedonLocation(line, line1);;
                                    break;
                                }
                            }
                        }
                    }
                  
                	//db.findMostPopularandHighlyRatedBasedonLocation("Nashville","United States");
                    break;
                }
                case 9: { //Find listings that has a specific property_type (House, Townhouse, etc… )
                	  System.out.println("YOU SELECT: Find listings that has a specific property type");
                      System.out.println("ENTER PROPERTY TYPE (Options: House,Apartment, Bed & Breakfast): ");
                      String line;
                      int max = 0;
                      int min = 0;
                      while(in.hasNextLine()){
                          line = in.nextLine();
                          if(!line.equals("")){
                              db.findListingSpecificProperty(line);
                              break;
                          }
                      }
                      break;
                   
                }
                case 10: { //Find listings that accommodates a specified duration in a city
                	System.out.println("YOU SELECT: Find listings that accommodates a specified duration in a city");
                	System.out.println("ENTER City");
                	  String line = "";
                	  String line1="";
                    int max = 0;
                    int min = 0;
                    while(in.hasNextLine()){
                        line = in.nextLine();
                        if(!line.equals("")){
                            System.out.println("Enter how many nights");
                            
                            if(in.hasNextLine()){
                            	line1=in.nextLine();
                                if(!line.equals("")){
                                   db.findListingAccommodatesDuration(line, Integer.parseInt(line1));
                                    break;
                                }
                            }
                        }
                    }
                    break;
                }
                case 11: { //Find a specific host that satisfy user's specific number of accommodations
                	System.out.println("YOU SELECT: Find a specific host that satisfy user's specific number of accommodations");
                	System.out.println("ENTER City");
                	  String line = "";
                	  String line1="";
                    int max = 0;
                    int min = 0;
                    while(in.hasNextLine()){
                        line = in.nextLine();
                        if(!line.equals("")){
                            System.out.println("Enter how many People");
                            
                            if(in.hasNextLine()){
                            	line1=in.nextLine();
                                if(!line.equals("")){
                                   db.findListingSpecificAccommodations(line, Integer.parseInt(line1));
                                    break;
                                }
                            }
                        }
                    }
                    break;
                }
                case 12: { //Find top 10 places to stay in a specific city and under a specific price based on review score
                	System.out.println("YOU SELECT: Find top 10 places to stay in a specific city and under a specific price based on review score");
                	System.out.println("ENTER City");
                	  String line = "";
                	  String line1="";
                    int max = 0;
                    int min = 0;
                    while(in.hasNextLine()){
                        line = in.nextLine();
                        if(!line.equals("")){
                            System.out.println("Enter how much money");
                            
                            if(in.hasNextLine()){
                            	line1=in.nextLine();
                                if(!line.equals("")){
                                   db.findListingInCityBasedOnPrice(line, Integer.parseInt(line1));
                                    break;
                                }
                            }
                        }
                    }
                    break;
                }
                case 13: { //Find top 10 closest airbnb listings to a specific zipcode
                    System.out.println("YOU SELECT: Find closest 10 airbnb listings to a specific location/address");
                    System.out.println("ENTER LOCATION: ");
                    String line;
                    int max = 0;
                    int min = 0;
                    while(in.hasNextLine()){
                        line = in.nextLine();
                        if(!line.equals("")){
                            db.findListingsBasedOnLocation(line);
                            break;
                        }
                    }
                    break;
                }
                case 14: { //Find top 10 closest airbnb listings to a specific zipcode that has specified room type
                    System.out.println("YOU SELECT: Find top 10 closest airbnb listings based on location/address within a range");
                    System.out.println("ENTER LOCATION: ");
                    String line = "";
                    int max = 0;
                    int min = 0;
                    while(in.hasNextLine()){
                        line = in.nextLine();
                        if(!line.equals("")){
                            System.out.println("Enter Max & Min Range in km: ");
                            if(in.hasNextLine()){
                                String[] range = in.nextLine().split(" ");
                                if(!line.equals("")){
                                    db.findListingsWithinRange(line, Double.parseDouble(range[0]), Double.parseDouble(range[1]));
                                    break;
                                }
                            }
                        }
                    }
                    break;
                }
                case 15: { //Find top 10 closest airbnb listings to a specific location that accommodates a number of users
                    System.out.println("YOU SELECT: Find top 10 airbnb listings to a specific zipcode that has a certain amenities");
                    System.out.println("ENTER LOCATION: ");
                    String line = "";
                    int max = 0;
                    int min = 0;
                    while(in.hasNextLine()){
                        line = in.nextLine();
                        if(!line.equals("")){
                            System.out.println("INCLUDE(1): ");
                            if(in.hasNextLine()){
                                String item = in.nextLine();
                                if(!line.equals("")){
                                    db.findListingsWithAmenities(line,item);
                                    break;
                                }
                            }
                        }
                    }
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
        System.out.println("[3] Find top 20 the oldest listings"); //Nhu
        System.out.println("[4] Rank countries based on Airbnb ratings"); //Nhu
        System.out.println("[5] Find top 10 cities with the most listings");
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
        System.out.println("[15] Find top 10 closest airbnb listings to a specific zipcode that has a certain amenities"); //Nhu

        System.out.print("Your Selection: ");
    }

}
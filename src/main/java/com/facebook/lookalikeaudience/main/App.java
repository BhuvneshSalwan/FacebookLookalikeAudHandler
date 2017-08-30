package com.facebook.lookalikeaudience.main;

import java.util.List;

import com.facebook.lookalikeaudience.creator.LookalikeAudience;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableRow;
import com.google.bigquery.main.Authenticate;
import com.google.bigquery.main.TableResults;

/**
 * Hello world!
 *
 */
public class App {

	public static void main( String[] args )
    {
        
    	Bigquery bigquery;
    	
    	if((bigquery = Authenticate.getAuthenticated()) != null){
    		
    		System.out.println(bigquery);
    		
    		if(TableResults.ListDataSet(bigquery)){
    		
    			if(TableResults.ListTables(bigquery)){
    				
    				TableDataList listData = TableResults.getResults(bigquery);
    				
    				if(null != listData && listData.getTotalRows() > 0){
    					
    					List<TableRow> rows = listData.getRows();
    					
    					for(int arr_i = 0; arr_i < listData.getTotalRows(); arr_i++){
    						
    						TableRow row = rows.get(arr_i);
    						
    						if(LookalikeAudience.createLookalikeAudience(row)){
    							System.out.println("Audience is created Successfully : " + row.getF().get(0).getV());
    						}
    						else{
    							System.out.println("Response Message : Page Engagement Audience Creation Failed.");
    						}
    						
    					}
    					
    				}
    				else{
    					System.out.println("Response Message : Some Error while retrieving data from Table.");
    				}
    				
    			}
    			else{
    				System.out.println("Response Message : Error while Listing Tables.");
    			}
    			
    		}
    		else{
    			System.out.println("Response Message : Error while Listing Datasets.");
    		}
    		
    		System.exit(0);
    		
    	}
    	else{
    		
    		System.out.println("Response Message : Didn't got the object of Big Query from get Authenticated Method.");
    		System.exit(0);
    		
    	}
    	
    }
	
}
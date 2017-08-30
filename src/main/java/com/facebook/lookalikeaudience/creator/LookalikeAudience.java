package com.facebook.lookalikeaudience.creator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONObject;

import com.facebook.lookalikeaudience.main.App;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest.Rows;
import com.google.api.services.bigquery.model.TableRow;

public class LookalikeAudience {

	public static final String URL = "https://graph.facebook.com";
	public static final String VERSION = "v2.9";
	private static final String SESSION_TOKEN = "CAAWXmQeQZAmcBANADF6ew1ZBXAAifj7REIcHmbTVjkAR5q6GAnRjrpcuVhhV435LHMXpb8HzUKzQaUU4uwkxIl5xpYSgzUNog43JX4qxe0pqVBvjHZCsPfgIpRRGY7xfFC2hb1Hi1s9EH0IhQu4KlnTGcsdgIq5FN2ufeNHOeEB9YGck36aah1rPHrdi10ZD";

	public static Boolean createLookalikeAudience(TableRow row){
		
		try{
			
			String audience_name;
			String subtype;
			String audience_id;
			String country;
			String ratio;
			String account_id;
			
			try{ audience_name = String.valueOf(row.getF().get(0).getV()); } catch(Exception e){ audience_name = "NULL";}
			try{ account_id = String.valueOf(row.getF().get(3).getV()); } catch(Exception e){ account_id = "NULL";}
			try{ subtype = String.valueOf(row.getF().get(1).getV()); } catch(Exception e){ subtype = "NULL"; }
			try{ audience_id = String.valueOf(row.getF().get(2).getV()); } catch(Exception e){ audience_id = "NULL"; }
			try{ country = String.valueOf(row.getF().get(5).getV()); } catch(Exception e){ country = "NULL"; }
			try{ ratio = String.valueOf(row.getF().get(6).getV()); } catch(Exception e){ System.out.println(e); ratio = "NULL"; }
			
			if(account_id.equals("NULL")){
				System.out.println("Response Message : Couldn't find the Account ID for the Audience.");
				return false;
			}
		
			String custom_url = URL + "/" + VERSION + "/act_" + account_id + "/customaudiences";
			
			HttpClient reqClient = new DefaultHttpClient();
			HttpPost reqpost = new HttpPost(custom_url);
			
			ArrayList<NameValuePair> urlparameters = new ArrayList<NameValuePair>();
			
			urlparameters.add(new BasicNameValuePair("access_token", SESSION_TOKEN));

			if(subtype.equals("NULL")){
				System.out.println("Response Message : Couldn't find the subtype for the Audience.");
				return false;
			}
			
			if(audience_id.equals("NULL")){
				System.out.println("Response Message : Couldn't find the Origin Audience ID for the Lookalike Audience.");
				return false;
			}
			
			if(ratio.equals("NULL")){
				System.out.println("Response Message : Couldn't find the ratio for the Audience.");
				return false;
			}
			
			urlparameters.add(new BasicNameValuePair("subtype", subtype));
			urlparameters.add(new BasicNameValuePair("origin_audience_id", audience_id));
			urlparameters.add(new BasicNameValuePair("lookalike_spec", "{\"country\":\""+ country +"\",\"ratio\":"+ ratio +",\"type\":\"custom_ratio\"}"));
			
			reqpost.setEntity(new UrlEncodedFormEntity(urlparameters));
		
			System.out.println("Sending POST Request : " + custom_url);
			System.out.println("POST Parameters : " + urlparameters.toString());
			
			HttpResponse response = reqClient.execute(reqpost);
			
			System.out.println("Response Code : " + response.getStatusLine().getStatusCode());
			
			StringBuffer buffer = new StringBuffer();
			BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			String line = null;
			
			while((line = reader.readLine()) != null){
				buffer.append(line);
			}
			
			System.out.println("Response Content : " + buffer.toString());
			
			Rows logsRow = new Rows();
			
			HashMap<String, Object> logsMap = new HashMap<String, Object>();
			
			logsMap.put("account_id", account_id);
			logsMap.put("operation", "LOOKALIKE_AUDIENCE_CREATE");
			logsMap.put("table_name", "LOOKALIKE_CREATE");
			logsMap.put("audience_name", audience_name);
			logsMap.put("status_code", response.getStatusLine().getStatusCode());
			logsMap.put("response_message", buffer.toString());
			
			logsRow.setJson(logsMap);
			App.logChunk.add(logsRow);
			
			if(response.getStatusLine().getStatusCode() >= 200 && response.getStatusLine().getStatusCode() < 300){
				
				JSONObject responseObj = new JSONObject(buffer.toString());
				
				if(responseObj.has("id")){
					System.out.println("Response Message : Audience Created Successfully with Audience ID : " + responseObj.getString("id"));
					return true;
				}
				else{
					System.out.println("Response Message : Please check the response. Wasn't able to find ID for the audience.");
					return false;
				}
				
			}
			else{
				System.out.println("Response Message : Request for Facebook Lookalike Audience Creation Failed.");
				return false;
			}
			
		}
		catch(Exception e){
			System.out.println("Exception : LookalikeAudience - createLookalikeAudience Method");
			System.out.println(e);
			return false;
		}
	
	}
	
}
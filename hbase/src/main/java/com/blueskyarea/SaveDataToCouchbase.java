package com.blueskyarea;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.CouchbaseClient;

public class SaveDataToCouchbase {	
	public static void main(String[] args) {
		CouchbaseClient client = null;

		try {
			List<java.net.URI> host = new ArrayList<java.net.URI>();
			host.add(new URI("http://localhost:8091/pools"));
			String bucket = "default";
			String password = "";

			client = new CouchbaseClient(host, bucket, password);
			client.set(args[0], args[1]).get();
			System.out.println(client.get(args[0]));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (client != null) {
				client.shutdown();
			}
		}
	}
}

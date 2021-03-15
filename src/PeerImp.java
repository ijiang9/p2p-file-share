
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.*;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.*;
import java.util.ArrayList;
import java.util.Random;
//Peer Implementation
public class PeerImp extends UnicastRemoteObject implements Peer{
	private String peerName;	
	private File f = null;
	private File f1 = null;
	private File[] filesList = null;
	private static Path peerDir;
	private ArrayList<String> neighbour;
	private boolean supperPeer;
	private HashMap <String, ArrayList<String>> fileIndex;
	private HashMap <String, ArrayList<String>> fileStat;
	//[filename, versionNum, consistant, originID, download, TTR]
	private int seq;
	private HashMap <String, String> routing;
	private ArrayList<String> searchResult;
	private HashMap <String, Integer> masterFile;
	private HashMap <String, ArrayList<String>> downloadFile;
	private boolean found;
	private int count;
	private long start;
	private long res;
	private boolean push;
	private boolean poll;
	private threadPoll threadP;
	
	
	//Constructor
	PeerImp(String name) throws RemoteException {
		super();
		this.seq = 1;
		this.peerName = name;
		this.supperPeer = false;
		peerDir = Paths.get(peerName);
        peerDir.toFile().mkdir();
        this.neighbour = new ArrayList<String>();
        int temp = Integer.parseInt(name);
        searchResult = new ArrayList<String>();
        if (temp > 0 && temp < 11) {
        	this.supperPeer = true;
        	fileIndex = new HashMap<String, ArrayList<String>>();
        	fileStat = new HashMap <String, ArrayList<String>>();
            routing = new HashMap<String, String>();
            this.threadP = new threadPoll(fileStat, fileIndex);
            this.threadP.start();
        }
        else {
        	masterFile = new HashMap<String, Integer>();
        	downloadFile = new HashMap<String, ArrayList<String>>();
        }
        
        this.found = false;
        this.count = 0;
        this.start = System.nanoTime();
        
        
	}
	//modify
	public void modify(String filename) {
		if (!this.masterFile.containsKey(filename)){
			System.out.println("file not exist!!!");
			return;
		}
		System.out.println("modify here");
		int i = this.masterFile.get(filename);
		i++;
		this.masterFile.put(filename, i);
		if (this.push){
			String messageID = this.peerName + Integer.toString(this.seq);
			try{
				Registry reg = LocateRegistry.getRegistry(8079);
				Peer n = (Peer) reg.lookup(neighbour.get(0));
				System.out.println(neighbour.get(0));
				n.invalidate(messageID, this.peerName, filename, this.masterFile.get(filename));
			}catch(Exception e){
				System.err.println("modify exception: "+ e.getMessage());
		        e.printStackTrace();
			}
		}
	}
	
	//invalidate remote call
	public void invalidate(String messageID, String originID, String filename, Integer versionNum) throws RemoteException{
		//System.out.println("message recieved");
		if (!this.routing.containsKey(messageID)){
			try {
				Registry reg = LocateRegistry.getRegistry(8079);
				//Update rounting table
				this.routing.put(messageID, originID);
				//
				
				if (this.fileIndex.containsKey(filename)){
					fileStat.get(filename).set(1, versionNum.toString());
					fileIndex.remove(filename);
					System.out.println(filename + " is modified!!!");
				}
				//broadcast
					
				for (String neighb : this.neighbour){
					Peer n = (Peer) reg.lookup(neighb);
					n.invalidate(messageID, originID, filename, versionNum);
					}
			}catch(Exception e) {
		         System.err.println("invalidate exception: "+ e.getMessage());
		         e.printStackTrace();
		      }
			
		}
	}
	public void refresh(){
		for (String filename : this.fileStat.keySet()){
        	ArrayList<String> stat = this.fileStat.get(filename);
        	if (stat.get(4).equals("d")){
        		//Update inconsistant files
        		if (stat.get(2).equals("I")){
        			System.out.println("Polling new file " + filename);
        			String p = fileIndex.get(filename).get(0);
        				try{
        					Registry reg = LocateRegistry.getRegistry(8079);
        					Peer n = (Peer) reg.lookup(p);
        					n.retrieve(stat.get(3),filename);
        				}catch(Exception e) {
        					System.err.println("threadP exception: "+ e.getMessage());
        					e.printStackTrace();
        				}
        				stat.set(2, "V");//update to valid
        				stat.set(5, Integer.toString(5000));//update TTR
        		}
        	}
		}
	}
	//Poll using thread
	private class threadPoll extends Thread {
        
        static final int period = 999;
        private HashMap<String, ArrayList<String>> fStat;
        
        public threadPoll(HashMap<String, ArrayList<String>> fileStat, HashMap<String, ArrayList<String>> fileIndex) {
            this.fStat = fileStat;
        }
      //[filename, versionNum, consistant, originID, download, TTR]
        @Override
        public void run() {
        	
            while (true) {
                for (String filename : this.fStat.keySet()){
                	ArrayList<String> stat = this.fStat.get(filename);
                	int ttr = Integer.parseInt(stat.get(5)) - period;
                	//monitoring downloaded file
                	if (stat.get(4).equals("d")&&stat.get(2).equals("V")){
                		//update TTR
                		if (ttr <= 0) {
                			System.out.println(filename + " expired!!!");
                			stat.set(2, "I");
                			stat.set(5, "0");
                		}
                		else {
                			stat.set(5, Integer.toString(ttr));
                		}
                	}
                	
                }
                try {
                    Thread.sleep(threadPoll.period);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
	
	
	
	
	
	//search
	public void search(String filename) throws InterruptedException{
		this.searchResult.clear();
		this.found = false;
		for (String neighb : this.neighbour) {
			try {
				Registry reg = LocateRegistry.getRegistry(8079);
				Peer n = (Peer) reg.lookup(neighb);
				String messageID = this.peerName + Integer.toString(this.seq);
				this.seq++;
				n.query(messageID, 5, filename, this.peerName);
			}catch(Exception e) {
		         System.err.println("search exception: "+ e.getMessage());
		         e.printStackTrace();
		      }
		}
//		Thread.sleep(100);
	}
	//query
	public void query(String messageID, int TTL, String filename, String upStream) throws RemoteException {
		if (!this.routing.containsKey(messageID)){
			try {
				Registry reg = LocateRegistry.getRegistry(8079);
				//Update rounting table
				this.routing.put(messageID, upStream);
				//search
				if (this.fileIndex.containsKey(filename)){
					for (String location : this.fileIndex.get(filename)){
						Peer h = (Peer) reg.lookup(upStream);
						h.queryHit(messageID, TTL, filename, location);
					}
				}
				//broadcast
				if (TTL > 0){
					
					for (String neighb : this.neighbour){
						Peer n = (Peer) reg.lookup(neighb);
						n.query(messageID, TTL -1, filename, this.peerName);
					}
				}
			}catch(Exception e) {
		         System.err.println("query exception: "+ e.getMessage());
		         e.printStackTrace();
		      }
			
		}
	}
	//queryHit
	public void queryHit(String messageID, int TTL, String filename, String peerName) throws RemoteException {
		String des = messageID.substring(0,2);
		try{
			if (!des.equals(this.peerName)) {//not the destination, back propagate
				String upStream = this.routing.get(messageID);
				Registry reg = LocateRegistry.getRegistry(8079);
				Peer up = (Peer) reg.lookup(upStream);
				up.queryHit(messageID, TTL, filename, peerName);
			}
			else {//destination found, report the result
				this.found = true;
				this.count++;
				this.searchResult.add(peerName);
				if (this.count == 1)
				{
					this.res = System.nanoTime() - start;
//					System.out.println(this.res);
				}
				System.out.println(this.searchResult);
			}
		}catch(Exception e) {
	         System.err.println("queryHit exception: "+ e.getMessage());
	         e.printStackTrace();
	      }
	}
	
	//Initialize Peer and update local file to server
	public void peerHi(Peer supperPeer) {
		System.out.println("Initializing Peer "+ this.peerName);
		f1 = new File(this.peerName, "master");
		f = new File(this.f1,".");
		filesList = f.listFiles();
		for(int i = 0; i < filesList.length; i++){
			File file = filesList[i];
			if(file.isFile()){
				try{
					supperPeer.addToIndex(this.peerName,file.getName(),0,"m",this.peerName);
					this.masterFile.put(file.getName(), 0);
				} catch(Exception e) {
			         System.err.println("addToIndex exception: "+ e.getMessage());
			         e.printStackTrace();
			      }
			}
		}
		System.out.println("Server List Updated");
	}
	//[filename, versionNum, consistant, originID, download, TTR]
	//update supper peer
	public void addToIndex(String peerName, String filename, Integer versionNum, String d, String originID) throws RemoteException
	{
		synchronized(this){
			if (fileIndex.containsKey(filename)) {//other peer has the file, add to the peer list
				if (!fileIndex.get(filename).contains(peerName)){
					fileIndex.get(filename).add(peerName);}
			}
			else {//no other peer has the file, add new key/value pair
				ArrayList<String> peerList = new ArrayList<String>();
				peerList.add(peerName);
				fileIndex.put(filename,  peerList);
			}
			//System.out.println(this.fileIndex);
			if (fileStat.containsKey(filename)) {//other peer has the file, add to the peer list
				fileStat.get(filename).set(1, versionNum.toString());
				
			}
			else {//no other peer has the file, add new key/value pair
				ArrayList<String> fstat = new ArrayList<String>();
				fstat.add(filename);
				String num = versionNum.toString();
				fstat.add(num);
				fstat.add("V");
				if (d.equals("d")){
					fstat.add(originID);
					fstat.add("d");
				}
				else {
					fstat.add(peerName);
					fstat.add("m");
				}
				
				fstat.add(Integer.toString(5000));
				
				fileStat.put(filename,  fstat);
			}
			//System.out.println(this.fileStat);
		}
	}
	//List file at local sharing folder
	public void listMFile() {
		System.out.println("Master files in this peer:");
		f1 = new File(this.peerName, "master");
		f = new File(this.f1,".");
		filesList = f.listFiles();
		for(int i = 0; i < filesList.length; i++){
			File file = filesList[i];
			if(file.isFile()){
				System.out.println(file.getName());
			}
		}
		if (this.supperPeer){
			System.out.println("Leaf node file list:");
			System.out.println(this.fileIndex);
		}
	}
	//List file at local sharing folder
		public void listDFile() {
			System.out.println("Downloaded files in this peer:");
			f1 = new File(this.peerName, "download");
			f = new File(this.f1,".");
			filesList = f.listFiles();
			for(int i = 0; i < filesList.length; i++){
				File file = filesList[i];
				if(file.isFile()){
					
						System.out.println(file.getName());
					
				}
			}
			if (this.supperPeer){
				System.out.println("Leaf node file list:");
				System.out.println(this.fileIndex);
			}
		}
	
	// Download file invoked by peer
	public void retrieve(String peer, String fileName) {
		System.out.println("Downloading... " + fileName);
		try{
			Registry reg = LocateRegistry.getRegistry(8079);
			Peer p = (Peer) reg.lookup(peer);
			byte[] filedata = p.downloadService(fileName);
			File file1 = new File(this.peerName, "download");
			System.out.println(file1.getAbsolutePath());
			this.peerDir = file1.toPath();
			System.out.println(this.peerDir.toString());
			File file = new File(this.peerDir.resolve(fileName).toString());
			System.out.println(file.getAbsolutePath());
			BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(this.peerDir.resolve(fileName).toString()));
			output.write(filedata,0,filedata.length);
			output.flush();
			output.close();
			System.out.println("Download Complete! Updating IndexServer");
			Peer sp = (Peer) reg.lookup(this.neighbour.get(0));
			sp.addToIndex(this.peerName, fileName, 0,"d", peer);
			System.out.println("IndexServer Updated!");
		} catch(Exception e) {
	         System.err.println("FileServer exception: "+ e.getMessage());
	         e.printStackTrace();
	      }
	}
	
//	// Delete local file and update server
//	public void delFile(String fileName) {
//		File file = new File(this.peerDir.resolve(fileName).toString()); 
//        try{
//        	if(file.delete()) 
//        	{ 
//        		System.out.println("File deleted");
//        		Registry reg = LocateRegistry.getRegistry(8079);
//            	IndexServer server = (IndexServer) reg.lookup("server");
//				server.delFromIndex(this.peerName, fileName);
//				System.out.println("IndexServer Updated!");
//        	} 
//        	else
//        	{ 
//        		System.out.println("Failed to delete the file"); 
//        	}
//        } catch(Exception e) {
//	         System.err.println("FileServer exception: "+ e.getMessage());
//	         e.printStackTrace();
//	      }
//	}
//	
	//Remote call implementation using bufferedInputStream
	@Override
	public byte[] downloadService(String fileName) throws RemoteException {
		try {
			System.out.println("Processing download Service...");
			File file1 = new File(this.peerName, "master");
			
			this.peerDir = file1.toPath();
	        File file = new File(peerDir.resolve(fileName).toString());
	        System.out.println(file.getAbsolutePath());
	        byte buffer[] = new byte[(int)file.length()];
	        BufferedInputStream input = new BufferedInputStream(new FileInputStream(peerDir.resolve(fileName).toString()));
	        input.read(buffer,0,buffer.length);
	        input.close();
	        System.out.println("Download Service Done!");
	        return(buffer);
	        } catch(Exception e){
	        System.out.println("FileImpl: "+e.getMessage());
	        e.printStackTrace();
	        return(null);
	        }
	}
	public String getPeerName() {
		return this.peerName;
	}
	//remote call by the server for testing multiple peers sending request concurrently
	public long concurrentTest() throws RemoteException{
		try{
			long start = System.nanoTime();
        	
            for (int j = 0; j != 200; j++)

            	this.searchResult.clear();
    			this.found = false;
//    		if (this.supperPeer){
//    			if (this.fileIndex.containsKey(filename)){
//    				for (String n : fileIndex.get(filename)){
//    					this.searchResult.add(n);
//    				}
//    			}
//    		}
    			for (String neighb : this.neighbour) {
    				try {
    					Registry reg = LocateRegistry.getRegistry(8079);
    					Peer n = (Peer) reg.lookup(neighb);
    					String messageID = this.peerName + Integer.toString(this.seq);
    					this.seq++;
    					n.query(messageID, 5, "08.srt", this.peerName);
    				}catch(Exception e) {
    					System.err.println("search exception: "+ e.getMessage());
    					e.printStackTrace();
    		      }
    		}
//    		Thread.sleep(100);
    		if (!found){
    			System.out.println("Not Found!!!");
    		}
            	
            	
            	
            	
            	
            long res = (System.nanoTime() - start)/200;
			System.out.println("Performance testing...");
			return res;
			
		} catch(Exception e){
	        System.out.println("FileImpl: "+e.getMessage());
	        e.printStackTrace();
	        return 1L;
	    }
	}
	
	
	static void usage() {
        System.out.println("Usage:");
        System.out.println("  ls                 - List all master files.");
        System.out.println("  search <filename>     - Search file on Indexing Server.");
        System.out.println("  obtain <peer name> <filename>     - Download file.");
        System.out.println("  lsd           - List all download files.");
        System.out.println("  refresh           - Download new files.");
        System.out.println("  modify <filename>           - Modify a file.");
        System.out.println("  ranM           - Randomly modify a file.");
        System.out.println("  ranSearch           - Randomly search for a file.");
    }
	
	public static void main(String[] args) throws Exception {
		//Locating index server by looking at the RMI registry
		Registry reg = LocateRegistry.getRegistry(8079);
        InitControl control = (InitControl) reg.lookup("control");
        //Initialize a peer
        PeerImp p = new PeerImp(args[0]);
        //register to the rmi registry as a file server
        reg.rebind(p.peerName, p);
        /*
         * leaf node find its supper peer
         * supper peer find its neighbours
         */
        p.neighbour = control.getNeighbour(p.peerName);
        p.push = control.getPush();
        p.poll = control.getPoll();
        //update supper peer file index
        if (!p.supperPeer){
        	System.out.println("Leaf node");
        	System.out.println(p.neighbour.get(0));
        	Peer supperPeer = (Peer) reg.lookup(p.neighbour.get(0));
        	p.peerHi(supperPeer);
        }
        System.out.println("Peer Ready");
        String location;
        //Interactive interface
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        usage();
        Random ran = new Random();
        for (;;) {
            String order = null;
            try {
                order = br.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (order == null) break;
            String[] tokens = order.split(" ");
            
            switch (tokens[0]) {
            case "ls"://list files
            	p.listMFile();
            	break;
            case "lsd"://list files
            	p.listDFile();
            	break;	
            
            // Send a request for searching.
            case "search"://send search request to the server
            	p.search(tokens[1]);
//            	location = server.searchIndex(p.peerName, tokens[1]);
//            	if (!location.equals("Not found")){
//            		System.out.println(tokens[1] + " is in peer " + location);
//            	}
//            	else System.out.println(location);
                break;

            //send 200 search requests to server
            case "ranSearch":
            	
            	p.count = 0;
            	for(int i =0; i<100; i++){
            	    int num = ran.nextInt(30)+111;
            	    String filename = Integer.toString(num) + ".srt";
            	    p.search(filename);
            	}

            	System.out.println(p.count);
                
                System.out.println("Found: " + p.count + "out of 100");
                break;
            case "ranM":
            	
            	p.count = 0;
            	Thread.sleep(1000);
            	for(int i =0; i<100; i++){
            	    int num = ran.nextInt(30)+111;
            	    String filename = Integer.toString(num) + ".srt";
            	    p.modify(filename);
            	    Thread.sleep(50);
            	}

            //download a file
            case "obtain":
            	
            	
            	p.retrieve(tokens[1], tokens[2]);
            	//System.out.println(p.fileStat);
                break;
          
            case "refresh":
            	p.refresh();
            	break;
        
            case "modify":
            	p.modify(tokens[1]);
            	break;
            default:
                usage();
            }
        }
//        
//        
//       
//        //System.out.println(server.searchIndex("03.srt"));
////        System.out.println(p.getPeerName());
////        if (p.getPeerName().equals("02")){
////        	p.download("01", "03.srt");
////        	p.delFile("03.srt");
////        }
////        server.sayHello();
//        
//        
//        //Peer pp = (Peer) reg.lookup("01");
//        
//		
	}
	
	

}

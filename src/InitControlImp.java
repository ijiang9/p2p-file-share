
import java.util.ArrayList;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.io.*;
import java.util.*;



public class InitControlImp extends UnicastRemoteObject
							implements InitControl
{
	
	private static final long serialVersionUID = 1L;
	//Store the file location in a hash map <fileName, list<peerName>>
	private HashMap <String, ArrayList<String>> networkTopology;
	private boolean push;
	private boolean poll;
	//Constructor
	InitControlImp() throws RemoteException
	{
		super();
		networkTopology = new HashMap<>();
		push = false;
		poll = false;
	}
	//remote call to setup network topology
	public ArrayList<String> getNeighbour(String peerName) throws RemoteException
	{
		//return this.networkTopology.get(peerName);
		return networkTopology.get(peerName);
	}
	public boolean getPush() throws RemoteException{
		return this.push;
	}
	public boolean getPoll() throws RemoteException{
		return this.poll;
	}
	
	
	
	
	static void usage() {
        System.out.println("Usage:");
        System.out.println("  pushON                 - Setup all-to-all topology.");
        System.out.println("  2  - linear topology.");
        System.out.println("  ctest    - Search for peers containing the file");
	}
	
	public static void main(String[] args) throws RemoteException {
		try {
			//Initialize the server and register to the rmi registry
			Registry reg = LocateRegistry.createRegistry(8079);
			InitControlImp control = new InitControlImp();
			reg.bind("control", control);
            System.err.println("Control ready");
            //Set up all-to-all topology
            File f = new File("src/ata.txt");
        	BufferedReader b = new BufferedReader(new FileReader(f));
        	String readLine;
        	while ((readLine = b.readLine()) != null) {
                String[] tokenss = readLine.split(" ");
                ArrayList<String> temp = new ArrayList<String>();
                for (int i = 1; i<tokenss.length; i++){
                	temp.add(tokenss[i]);
                }
                control.networkTopology.put(tokenss[0], temp);
            }
        	System.out.println(control.networkTopology);
        	
            //Interactive interface
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            usage();
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
                case "pushON":
                	control.push = true;
                	break;
                case "pushOFF":
                	
                	break;
                	
                case "pollON":
                	
                	break;
                
                case "pollOFF":
                	
                	break;
                	
                default:
                    usage();
                } 	
                
                
//                else {//multiple nodes sending queries. Doesn't work
//                	long totalTime = 0;
//                	long start = System.currentTimeMillis();
//                	for (int i = 0; i< Integer.parseInt(tokens[1]); i++)
//                	{
//                		String tempPeer = "1" + Integer.toString(i+1);
//                		//System.out.println(tempPeer);
//                		Peer p = (Peer) reg.lookup(tempPeer);
//                		totalTime += p.concurrentTest();
//                		//p.concurrentTest();
//                	}
//                	long res = (System.currentTimeMillis() - start);
//                	System.out.println("Average time: " + res + "ms");
//                	break;
//                }
            }
        } catch (Exception e) {
            System.err.println("Server exception: " + e.getMessage());
        }
	}

	
 }

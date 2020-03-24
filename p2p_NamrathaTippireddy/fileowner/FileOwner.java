import java.io.*;
import java.util.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;


public class FileOwner {

	private static int sPort = 0;  // The FileOwner will listen on this port, remember this has to be dynamic
	private static Map<Integer, ArrayList<Integer>> chunksListToPeerFromFMMap;
	public static int totalChunkCount = 0;

	public static void main(String[] args) throws Exception {

		// Getting server portnumber from command prompt
		if (args.length > 0)
		{
			//serveraddr = args[0];
			sPort = Integer.parseInt(args[0]);
		}
		// 1. First split the file in to chucks of 100KB

		File testFile = new File("./file/" + "test.pdf");
		totalChunkCount =  Utils.splitFile(testFile);
		Logger logger = new Logger("FileOwner");
		logger.doneSplitting(totalChunkCount);

		int noOfClients = 5; //hardcoded num of peers to 5
		//Deciding which peer can get which chunks

		chunksListToPeerFromFMMap = new LinkedHashMap<Integer, ArrayList<Integer>>();
		for (int i = 5001, k=1; i < (5001 + noOfClients); i++, k++) {
			ArrayList<Integer> arr = new ArrayList<Integer>();
			for (int j = k; j <= totalChunkCount; j += noOfClients) {
				arr.add(j);
			}
			chunksListToPeerFromFMMap.put(i, arr);
			//i is the peer number and arr has the chunks it can have
		}


		// 2. Split the incoming args and assign the port, ToDo: clientNum to be set correct yet 
		System.out.println("The server is running.");
		ServerSocket listener = new ServerSocket(sPort);
		int clientNum = 1;
		try {
			while (true) {
				new Handler(listener.accept(), clientNum, logger).start();
				System.out.println("Client " + clientNum + " is connected!");
				clientNum++;
			}
		} finally {
			listener.close();
		}
	}	

	/**
	 * A handler thread class. Handlers are spawned from the listening loop and are
	 * responsible for dealing with a single client's requests.
	 */
	private static class Handler extends Thread {
		private String message; // message received from the client
		private Socket connection;
		private ObjectInputStream in; // stream read from the socket
		private ObjectOutputStream out; // stream write to the socket
		private DataOutputStream dos;
		private int no; // The index number of the client
		private Logger logger;
		public Handler(Socket connection, int no, Logger logger) {
			this.connection = connection;
			this.logger = logger;
			this.no = no;
		}

		public void run() {
			try {
				// initialize Input and Output streams
				out = new ObjectOutputStream(connection.getOutputStream());
				dos = new DataOutputStream(connection.getOutputStream());
				dos.flush();
				out.flush();
				in = new ObjectInputStream(connection.getInputStream());
				try {
					while (true) {
						// 3. Now read the incoming msg understand the request: Send ChunkLink/ Send Chunk
						// receive the message sent from the client
						String[] msgFromClient = ((String) in.readObject()).split(":");	// msg format be liks => 5001:Ready or 5001:ChunkList or 5001:Chunk:1
						String currentPeer = msgFromClient[0];
						String msg = msgFromClient[1].toUpperCase();

						if(msg.equals("READY")){
							logger.receivedMsg(currentPeer, msg);
						} else if(msg.equals("CHUNKLIST")){
							collectAndSendChunkList(currentPeer);
						} else if(msg.equals("CHUNK")){
							String chunkNumToSend = msgFromClient[2];
							// sending  the chunk to client  logic gets called here
							sendChunkToPeer(chunkNumToSend);
							logger.sameChunkList(currentPeer);
						}else if(msg.equals("TOTALCHUNKS"))
						{
							sendTotalNumOfChunks(totalChunkCount);
						}
						
					}
				} catch (ClassNotFoundException classnot) {
					System.err.println("Data received in unknown format");
				}
			} catch (IOException ioException) {
				System.out.println("Disconnect with Client " + no);
			} finally {
				// Close connections
				try {
					in.close();
					out.close();
					dos.close();
					connection.close();
				} catch (IOException ioException) {
					System.out.println("Disconnect with Client " + no);
				}
			}
		}

		public void collectAndSendChunkList(String peerID){
			try {
    			
    			String chunkNamesList = "";
     			ArrayList<Integer> chunksToBeSent = chunksListToPeerFromFMMap.get(Integer.parseInt(peerID));
     			System.out.println("size is "+chunksToBeSent.size());
     			for (int i=0; i<chunksToBeSent.size();i++)
     			{
     				chunkNamesList = chunkNamesList+Integer.toString(chunksToBeSent.get(i))+"\n";
     			}
    			out.writeObject(chunkNamesList);
    			out.flush();
    			logger.sentChunkList(peerID);

    		}
    		catch(Exception e) {
    			System.out.println("FileOwner couldn't give the chunks list");
    			e.printStackTrace();
            	System.out.println("Exception: "+e);
    		}
		}	


		public void sendTotalNumOfChunks(int totalChunkCount){
	    	try{
		    	System.out.println("TOTALCHUNKS are "+totalChunkCount);
		    	out.writeObject(Integer.toString(totalChunkCount));
		    	out.flush();
	   		}
	    	catch(Exception exce)
	    	{
	    		System.out.println("Server couldn't give the total num  of chunks");
	     		exce.printStackTrace();
		    }
	    }

		public void sendChunkToPeer(String chunkNum) {
    		try {
	    		File reqChunk = new File("./file/chunks/"+chunkNum);  //search for the required file and throw an error if its not there
	    		System.out.println("Chunk exist: "+reqChunk.exists());
	    		if (!reqChunk.exists())
	    		{	
	    			dos.writeUTF("FileNotFound");
	    			dos.flush();
	    			System.out.println("Chunk '" + chunkNum +"' doesn't exist on server ");
	    		}
	    		else {
		           // System.out.println("File length in bytes is "+reqChunk.length());
		            FileInputStream fileIS = new FileInputStream(reqChunk);
			        int count = fileIS.available();
		            // System.out.println("Chunk length is "+count);
		            DataInputStream dataIS = new DataInputStream(fileIS);
		            byte[] servbuffer = new byte[count];
		            dataIS.read(servbuffer);
		            dos.writeUTF(reqChunk.getName());
		            dos.writeLong(servbuffer.length);
		            dos.write(servbuffer, 0, servbuffer.length);
		            dos.flush();
		            System.out.println("Chunk "+chunkNum+" is sent to the requesting client");
	    		}
    		}
    		catch(Exception exe) {
    			exe.printStackTrace();
    		} 
    	}
	}

}

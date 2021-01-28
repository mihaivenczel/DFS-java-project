import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.io.Console;
import java.io.File;
import java.io.FileFilter;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Main {
    
  static   String ANSI_BLUE = "\u001B[34m";
      static      String ANSI_RESET = "\u001B[0m";
     static       BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
         //dir();
     static  String file_delete;
 static String str1;
        static      String name;
        static      String filename;
        static      int nr;
            // Client c = new Client();
        static      byte[] rez;
        static      byte[] ret;
        static      byte[] gol = null;
        static      boolean ok = true;
        static     String text;
       static String file;
        static           SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
              static String file_name ;
        static           String newline = System.lineSeparator();
         static          char[] ss;
        static           byte[] data;
     static Date date;
    public static void dir(){
        File dir = new File("C:\\");
      File[] files = dir.listFiles();
      FileFilter fileFilter = new FileFilter() {
         public boolean accept(File file) {
            return file.isDirectory();
         }
      };
      files = dir.listFiles(fileFilter);
      System.out.println(files.length);
      
      if (files.length == 0) {
         System.out.println("Either dir does not exist or is not a directory");
      } else {
         for (int i = 0; i< files.length; i++) {
            File filename = files[i];
            System.out.println(filename.toString());
         }
      }
    }

	static int regPort = Configurations.REG_PORT;

	static Registry registry ;

	/**
	 * respawns replica servers and register replicas at master
	 * @param master
	 * @throws IOException
	 */
	static void respawnReplicaServers(Master master)throws IOException{
		System.out.println("[@main] respawning replica servers ");
		// TODO make file names global
		BufferedReader br = new BufferedReader(new FileReader("repServers.txt"));
		int n = Integer.parseInt(br.readLine().trim());
		ReplicaLoc replicaLoc;
		String s;

		for (int i = 0; i < n; i++) {
			s = br.readLine().trim();
			replicaLoc = new ReplicaLoc(i, s.substring(0, s.indexOf(':')) , true);
			ReplicaServer rs = new ReplicaServer(i, "./"); 

			ReplicaInterface stub = (ReplicaInterface) UnicastRemoteObject.exportObject(rs, 0);
			registry.rebind("ReplicaClient"+i, stub);

			master.registerReplicaServer(replicaLoc, stub);

			System.out.println("replica server state [@ main] = "+rs.isAlive());
		}
		br.close();
	}

    public static void afisare_comenzi() {
        System.out.println(ANSI_BLUE + "Possible actions are:" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "ls" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "write" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "read" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "rm" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "Type 'Exit' to exit" + ANSI_RESET);
        System.out.println();
    }

    public static int input_command() throws IOException {
        System.out.println(ANSI_BLUE + "Choose an action : 'ls', 'write', 'read' si 'rm' " + ANSI_RESET);
        name = reader.readLine();
        
         if(name.equalsIgnoreCase("Exit"))
         return 0;
        if (name.equalsIgnoreCase("ls")) 
            return 1;
        if (name.equalsIgnoreCase("write")) 
            return 2;
        if (name.equalsIgnoreCase("read")) 
            return 3;
        if (name.equalsIgnoreCase("rm")) 
            return 4;
         return 5;
    }

    public static void show_files() {
        Client c3 = new Client(); 
        File directoryPath = new File("C:\\Users\\Mihai\\Documents\\NetBeansProjects\\DFS\\Replica_0"); 
        File directoryPathAux;
        String contents[] = directoryPath.list(); 
 
        System.out.println(ANSI_BLUE + "Number of files : " + contents.length + ANSI_RESET); 
        System.out.println(ANSI_BLUE + "Files and folder: " + ANSI_RESET);
        for (int i = 0; i < contents.length; i++) {
            directoryPathAux=new File("C:\\Users\\Mihai\\Documents\\NetBeansProjects\\DFS\\Replica_0"+contents[i]);
            if(directoryPathAux.isFile()==true)
            System.out.println(contents[i]+" "
                    + "(file)");
            else 
                 System.out.println(contents[i]+" (folder)");
            
        }
      
    }

    public static void show_file_content() throws IOException, NotBoundException, MessageNotFoundException {
        Client c = new Client();

        System.out.println(ANSI_BLUE + "TYPE selected: enter the name of the file you want to read" + ANSI_RESET);
        System.out.println();
        file = reader.readLine(); 
        ss = " ".toCharArray(); 
        data = new byte[ss.length]; 
        for (int i = 0; i < ss.length; i++) { 
            data[i] = (byte) ss[i];
        }
        c.write(file, data); 

        String str = new String(c.read(file)); 
        date = new Date(System.currentTimeMillis()); 
        text = newline + "Client " + c + " read file: '" + file + "'" + " at: " + formatter.format(date); 
        ss = text.toCharArray(); 
        data = new byte[ss.length]; 
        for (int i = 0; i < ss.length; i++) {
            data[i] = (byte) ss[i];
        }
        c.write("logs_type", data); 
        System.out.println(ANSI_BLUE + "Text in file: :" + ANSI_RESET);
        System.out.println(str); 

    }

    public static void delete_file() throws IOException, NotBoundException, MessageNotFoundException {
        Client c2 = new Client();
        System.out.println(ANSI_BLUE + "DELETE selected: enter the name of the file you want to delete" + ANSI_RESET);
        System.out.println();
        file_delete = reader.readLine(); 
        file_name = file_delete;
        for (int x = 0; x < 3; x++) { 
            String fileName = "C:\\Users\\Mihai\\Documents\\NetBeansProjects\\DFS\\Replica_" + x + "\\" + file_delete;
            try {
                boolean exista = Files.deleteIfExists(Paths.get(fileName)); 
                if(exista==true) 
                System.out.println(ANSI_BLUE + "File deleted succesfully" + ANSI_RESET);
                else 
                   System.out.println(ANSI_BLUE + "File not found" + ANSI_RESET); 

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        date = new Date(System.currentTimeMillis()); // inscriem in fisierul de log-uri actiunea
        text = newline + "Client " + c2.toString() + " deleted file: '" + file_name + "'" + " at: " + formatter.format(date);
        ss = text.toCharArray();
        data = new byte[ss.length];
        for (int i = 0; i < ss.length; i++) {
            data[i] = (byte) ss[i];
        }
        c2.write("logs_delete", data);
    }

    public static void write_file() throws IOException, NotBoundException, MessageNotFoundException {
        Client c1 = new Client();
        System.out.println(ANSI_BLUE + "Write selected: enter your text" + ANSI_RESET);
        System.out.println();
        filename = reader.readLine();
        System.out.println(ANSI_BLUE + "Add text: " + ANSI_RESET);
        System.out.println();
        text = reader.readLine();
        ss = text.toCharArray();
        data = new byte[ss.length];
        for (int i = 0; i < ss.length; i++) {
            data[i] = (byte) ss[i];
        }
        c1.write(filename, data);

        date = new Date(System.currentTimeMillis());
        text = newline + "Client " + c1 + " created file: '" + filename + "'" + " at: " + formatter.format(date);
        ss = text.toCharArray();
        data = new byte[ss.length];
        for (int i = 0; i < ss.length; i++) {
            data[i] = (byte) ss[i];
        }
        c1.write("logs_write", data);
        ;
    }
         
	   public static void launchClients() {
        try {
            while (ok) {
                nr = input_command();
                switch (nr) {
                    case 5:
                        System.out.println("Wrong command");
                        break;
                    case 1:
                        show_files();
                        break;
                    case 2: 
                        write_file();
                        break;
                    case 3: 
                        show_file_content();
                        break;
                    case 4:
                        delete_file();
                        break;
                    default:
                        System.out.println(ANSI_BLUE + "Exit" + ANSI_RESET);
                        System.exit(0);
                }
            }


        } catch (NotBoundException | IOException | MessageNotFoundException e) {
            e.printStackTrace();
        }
    }
//  ----------------------------------------------------------------------------------------------------------------------------------------- -----------------------------------------------------------------------------------------------------------------------------------------
	/**
	 * runs a custom test as follows
	 * 1. write initial text to "file1"
	 * 2. reads the recently text written to "file1"
	 * 3. writes a new message to "file1"
	 * 4. while the writing operation in progress read the content of "file1"
	 * 5. the read content should be = to the initial message
	 * 6. commit the 2nd write operation
	 * 7. read the content of "file1", should be = initial messages then second message 
	 * 
	 * @throws IOException
	 * @throws NotBoundException
	 * @throws MessageNotFoundException
	 */
	public  static void customTest() throws IOException, NotBoundException, MessageNotFoundException{
		Client c = new Client();
		String fileName = "file1";

		char[] ss = "[INITIAL DATA!]".toCharArray(); // len = 15
		byte[] data = new byte[ss.length];
		for (int i = 0; i < ss.length; i++) 
			data[i] = (byte) ss[i];

		c.write(fileName, data);

		c = new Client();
		ss = "File 1 test test END".toCharArray(); // len = 20
		data = new byte[ss.length];
		for (int i = 0; i < ss.length; i++) 
			data[i] = (byte) ss[i];

		
		byte[] chunk = new byte[Configurations.CHUNK_SIZE];

		int seqN =data.length/Configurations.CHUNK_SIZE;
		int lastChunkLen = Configurations.CHUNK_SIZE;

		if (data.length%Configurations.CHUNK_SIZE > 0) {
			lastChunkLen = data.length%Configurations.CHUNK_SIZE;
			seqN++;
		}
		
		WriteAck ackMsg = c.masterStub.write(fileName);
		ReplicaServerClientInterface stub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+ackMsg.getLoc().getId());

		FileContent fileContent;
		@SuppressWarnings("unused")
		ChunkAck chunkAck;
		//		for (int i = 0; i < seqN; i++) {
		System.arraycopy(data, 0*Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
		fileContent = new FileContent(fileName, chunk);
		chunkAck = stub.write(ackMsg.getTransactionId(), 0, fileContent);


		System.arraycopy(data, 1*Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
		fileContent = new FileContent(fileName, chunk);
		chunkAck = stub.write(ackMsg.getTransactionId(), 1, fileContent);

		// read here 
		List<ReplicaLoc> locations = c.masterStub.read(fileName);
		System.err.println("[@CustomTest] Read1 started ");

		// TODO fetch from all and verify 
		ReplicaLoc replicaLoc = locations.get(0);
		ReplicaServerClientInterface replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+replicaLoc.getId());
		fileContent = replicaStub.read(fileName);
		System.err.println("[@CustomTest] data:");
		System.err.println(new String(fileContent.getData()));


		// continue write 
		for(int i = 2; i < seqN-1; i++){
			System.arraycopy(data, i*Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
			fileContent = new FileContent(fileName, chunk);
			chunkAck = stub.write(ackMsg.getTransactionId(), i, fileContent);
		}
		// copy the last chuck that might be < CHUNK_SIZE
		System.arraycopy(data, (seqN-1)*Configurations.CHUNK_SIZE, chunk, 0, lastChunkLen);
		fileContent = new FileContent(fileName, chunk);
		chunkAck = stub.write(ackMsg.getTransactionId(), seqN-1, fileContent);

		
		
		//commit
		ReplicaLoc primaryLoc = c.masterStub.locatePrimaryReplica(fileName);
		ReplicaServerClientInterface primaryStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+primaryLoc.getId());
		primaryStub.commit(ackMsg.getTransactionId(), seqN);

		
		// read
		locations = c.masterStub.read(fileName);
		System.err.println("[@CustomTest] Read3 started ");

		replicaLoc = locations.get(0);
		replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+replicaLoc.getId());
		fileContent = replicaStub.read(fileName);
		System.err.println("[@CustomTest] data:");
		System.err.println(new String(fileContent.getData()));

	}

	static Master startMaster() throws AccessException, RemoteException{
		Master master = new Master();
		MasterServerClientInterface stub = 
				(MasterServerClientInterface) UnicastRemoteObject.exportObject(master, 0);
		registry.rebind("MasterServerClientInterface", stub);
		System.err.println("Server ready");
		return master;
	}

	public static void main(String[] args) throws IOException {


		try {
			LocateRegistry.createRegistry(regPort);
			registry = LocateRegistry.getRegistry(regPort);

			Master master = startMaster();
			respawnReplicaServers(master);

//			customTest();
                        afisare_comenzi();
			launchClients();
                        

		} catch (RemoteException   e) {
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}
	}

}
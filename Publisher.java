
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Scanner;


public class Publisher {
	
	private static final String QUIT = "QUIT";
	private static final String LOCALHOST = "localhost";

	private static int port;
	

	public static void main(String[] args) {
		// Get the port number to connect Publisher with Broker
		port = Integer.parseInt(args[0]);
		
		Publisher pub = new Publisher();
		pub.invoke();
	}
	
	public void invoke() {
		Scanner scanner = new Scanner(System.in);

		try {
			InetAddress ip = InetAddress.getByName(LOCALHOST);

			while(true) {
				String publisherMsg = scanner.nextLine();

				// Establish the connection with Broker port 8080
				Socket socket = new Socket(ip, port);

				// Get the input and output streams
				DataInputStream inputStream = new DataInputStream(socket.getInputStream());
				DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());

				System.out.println("Publishing data to Broker: "+publisherMsg);
				outputStream.writeUTF(publisherMsg);

				// Printing date or time as requested by client
				String received = inputStream.readUTF();
				System.out.println("received : "+received);
				Thread.sleep(1000);

				if(publisherMsg.equals(QUIT)) {
					quitPublisher(socket, inputStream, outputStream, scanner);
				}
			}
		}
		catch(IOException e){
			System.err.println("IOException occurred inside the invoke() :::::: Publisher");
			e.printStackTrace();
			invoke();
		}
		catch(Exception e){
			System.err.println("Exception occurred inside the invoke() :::::: Publisher");
			e.printStackTrace();
			invoke();
		}
	}
	
	public void quitPublisher(Socket socket, DataInputStream inputStream, DataOutputStream outputStream, Scanner scanner) {
		try {
			System.out.println("About to QUIT the Publisher connection : " + socket);
			socket.close();
			inputStream.close();
			outputStream.close();
			scanner.close();
			System.out.println("Publisher connection closed");
		}
		catch(Exception e) {
			System.err.println("Exception occurred inside the quitPublisher() :::::: Publisher");
		}
	}
	
}

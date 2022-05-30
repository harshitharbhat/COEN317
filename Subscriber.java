
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Scanner;


public class Subscriber {
	
	private static final String QUIT = "QUIT";

	private static int port;
	private static String previous = null;

	
	public static void main(String[] args) {
		// Get the port number to connect Subscriber with Broker
		port = Integer.parseInt(args[0]);
		
		Subscriber sub = new Subscriber();
		sub.invoke();
	}

	public void invoke() {
		Scanner scanner = new Scanner(System.in);
		System.out.println("Please input the topic you wish to subscribe to: for ex: sub topic");
		
		try{
			String subscriberMsg = scanner.nextLine();
			String topic = subscriberMsg.split(" ")[1];
			System.out.println("Subsribing to the "+topic);
	
			while(true) {
				InetAddress ip = InetAddress.getByName("localhost");
				Socket socket = new Socket(ip, port);
				DataInputStream inputStream = new DataInputStream(socket.getInputStream());
				DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
				outputStream.writeUTF(subscriberMsg);

				
				// printing date or time as requested by client
				String received = inputStream.readUTF();
				if(!received.equals(previous)){
					System.out.println("Subscriber received: "+received);
					previous = received;
				}
				Thread.sleep(1000);

				if(subscriberMsg.equals(QUIT)){
					quitSubscriber(socket, inputStream, outputStream, scanner);
				}
			}
		}
		catch(IOException e){	
			System.err.println("IOException occurred inside the invoke() :::::: Subscriber");
			e.printStackTrace();
		}
		catch(Exception e){
			System.err.println("Exception occurred inside the invoke() :::::: Subscriber");
			e.printStackTrace();
		}
	}
	
	public void quitSubscriber(Socket socket, DataInputStream inputStream, DataOutputStream outputStream, Scanner scanner) {
		try {
			System.out.println("About to QUIT the Subscriber connection : " +socket);
			socket.close();
			inputStream.close();
			outputStream.close();
			scanner.close();
			System.out.println("Subscriber connection closed");
		}
		catch(Exception e) {
			System.err.println("Exception occurred inside the quitPublisher() :::::: Publisher");
		}
	}
	
}

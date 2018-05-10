package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import java.io.IOException;
import java.net.ServerSocket;
import android.content.Context;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.database.MatrixCursor;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;


public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	public static int initialized=0;
	public static int inserting=0;
	public static int querying=0;
	static final String[] REMOTE_PORTS = {"11124","11112","11108","11116","11120"};
	static final int SERVER_PORT = 10000;
	private Uri providerUri=Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
	private static String myPort;
	static ConcurrentHashMap<String, Integer> versionMap = new ConcurrentHashMap<String, Integer>();

	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		while(initialized==0) {
			try {
				Thread.sleep(200);
			} catch (Exception e) {
				Log.e("Delete Task","Exception in delete thread");
			}
		}
		String key = selection;
		String keyHash = "";
		try{
			keyHash = genHash(key);
		}catch (Exception e){
			Log.e("DeleteTask","Exception in generating keyhash");
		}
		if (key.equalsIgnoreCase("*")) {

			Message message = new Message();
			message.setMessageType("GLOBALDELETE");
			ObjectOutputStream outputStream;
			Socket socket;
			for (String port: REMOTE_PORTS) {
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(port));
					outputStream = new ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject(message);
					outputStream.flush();
				} catch (Exception e) {
					Log.e("Delete Task","Delete Operation failed"+e.getMessage());
				}
			}
		}

		else if (key.equalsIgnoreCase("@")) {
			Log.i("DeleteTask","If it is star query");
			String[] files=getContext().fileList();
			for(String file: files){
				try {
					getContext().deleteFile(file);
				}
				catch(Exception e){
					Log.e(TAG, "Delete operation failed:" + e.getMessage());
				}
			}
		}
		//If deleting by specific key
		else{
			String[] deletePorts = new String[3];
			Message message = new Message();
			message.setKey(key);
			message.setMessageType("SINGLEDELETE");
			deletePorts[0] = keyLookUp(keyHash);
			int index = Arrays.asList(REMOTE_PORTS).indexOf(deletePorts[0]);
			deletePorts[1] = REMOTE_PORTS[(index + 1) % 5];
			deletePorts[2] = REMOTE_PORTS[(index + 2) % 5];
			ObjectOutputStream outputStream;
			Socket socket;
			for (String port: deletePorts) {
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(port));
					outputStream = new ObjectOutputStream(socket.getOutputStream());
					Log.i(TAG,"Sending delete message to : " + port + " to delete key : " + message.getKey());
					outputStream.writeObject(message);
					outputStream.flush();
				} catch (Exception e) {
					Log.e("Delete Task","Exception in delete task--"+e.getMessage());
				}
			}
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		inserting = 1;
		while(initialized==0) {
			try {
				Thread.sleep(200);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		String key = values.getAsString("key");
		String value = values.getAsString("value");
		Message message = new Message();
		message.setMessageType("INSERT");
		message.setKey(key);
		message.setValue(value);
		String[] insertionPorts = new String[3];
		String keyHash = "";
		try {
			keyHash = genHash(key);
		} catch (Exception e) {
			Log.e("Insert task","key hash generation failed");
		}

		insertionPorts[0] = keyLookUp(keyHash);
		int index = Arrays.asList(REMOTE_PORTS).indexOf(insertionPorts[0]);
		insertionPorts[1] = REMOTE_PORTS[(index + 1) % 5];
		insertionPorts[2] = REMOTE_PORTS[(index + 2) % 5];
		ObjectOutputStream outputStream;
		ObjectInputStream inputStream;
		Socket socket;
		for (String port : insertionPorts) {
			int count = 1;
			while(count<=4){
				try {
					if (count > 1) {
						Thread.sleep(100);
					}
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(port));
					outputStream = new ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject(message);
					outputStream.flush();
					inputStream = new ObjectInputStream(socket.getInputStream());
					inputStream.readObject();
					break; //Break the loop once received notification from server else try 3 more time
				}catch (Exception e) {
					Log.e("Insert Task","Exception in while loop"+e.getMessage());
				}
				count++;
			}
		}
		inserting = 0;
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String thisPort = String.valueOf((Integer.parseInt(portStr) * 2));
		myPort = thisPort;

		String[] files=getContext().fileList();
		//Checking if initial startup or recovery

		if(files!=null && files.length>0){
			initialized=0;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "RECOVER");
		}
		else{
			initialized = 1;
		}
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT,25);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "IOException Can't create a ServerSocket");
			return false;
		} catch (Exception e){
			Log.e(TAG, "Exception can't create a ServerSocket");
			return false;
		}
		return false;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		querying = 1;
		while(initialized==0) {
			try {
				Thread.sleep(200);
			} catch (Exception e) {
				Log.e("Query Task","Query operation failed");
			}
		}
		MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
		String key = selection;
		String keyHash = "";
		try{
			keyHash = genHash(key);
		}catch (Exception e){
			Log.e("Query Task","Exception in generating hash");
		}
		if (key.equalsIgnoreCase("*")) {
			Log.i("Query Task","If it is * query");
			Message message = new Message();
			message.setMessageType("STARQUERY");
			ObjectOutputStream outputStream;
			ObjectInputStream inputStream;
			Socket socket;
			List<Message> messageList;
			ConcurrentHashMap<String, String> messageMap = new ConcurrentHashMap<String, String>();
			for (String port: REMOTE_PORTS) {
				try{
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(port));
					outputStream = new ObjectOutputStream(socket.getOutputStream());
					inputStream = new ObjectInputStream(socket.getInputStream());
					outputStream.writeObject(message);
					outputStream.flush();
					messageList = (List<Message>)inputStream.readObject(); //reading data from the server
					for (Message msg: messageList) {
						messageMap.put(msg.getKey(), msg.getValue().split("#")[0]);
					}
				}catch (Exception e) {
					Log.e("Query Task","Connection to server failed");
				}
				//Adding all the data from messageMap to matrix cursor
				for (Map.Entry<String, String> messageData: messageMap.entrySet()) {
					String[] row = new String[]{messageData.getKey(), messageData.getValue()};
					matrixCursor.addRow(row);
				}
			}
		}
		else if (key.equalsIgnoreCase("@")) {
			Log.i("Query Task","If it is @ query");
			StringBuilder sb = new StringBuilder();
			InputStream inputStream;
			String[] files = getContext().fileList();
			for (String file : files) {
				sb = new StringBuilder();
				try {
					inputStream = getContext().openFileInput(file);
					BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
					String temp;
					while ((temp = br.readLine()) != null) {
						sb.append(temp);
					}
					br.close();
				} catch (FileNotFoundException e) {
					Log.e(TAG, " QueryTask UnknownHostException");
				} catch (IOException ie) {
					Log.e(TAG, "QueryTask IOException");
				} catch (Exception e) {
					Log.e(TAG, "QueryTask General Exception");
				}
				matrixCursor.addRow(new String[]{file, sb.toString().split("#")[0]});
			}
		}
		else{
			Message message = new Message();
			message.setMessageType("SINGLEQUERY");
			message.setKey(key);
			String [] destPorts = new String[3];

			destPorts[0] = keyLookUp(keyHash);
			int index = Arrays.asList(REMOTE_PORTS).indexOf(destPorts[0]);
			destPorts[1] = REMOTE_PORTS[(index + 1) % 5];
			destPorts[2] = REMOTE_PORTS[(index + 2) % 5];
			ObjectOutputStream outputStream;
			ObjectInputStream inputStream;
			String queryResult=null;
			Socket socket;
			ConcurrentHashMap<String, String> responseMap = new ConcurrentHashMap<String, String>();
			for (String port: destPorts) {
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(port));
					outputStream = new ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject(message);
					outputStream.flush();
					inputStream = new ObjectInputStream(socket.getInputStream()); //Receiving data from the server
					message = (Message)inputStream.readObject();
					queryResult = message.getValue();
					responseMap.put(port, queryResult);
				} catch (Exception e) {
					Log.e("Query Task","Connection to server failed");
				}
			}
			String result = "";
			Integer tempVersion = 0;
			for (String port: destPorts) {
				Log.i("getLatestValue","Value of Port"+port);
				String response = responseMap.get(port);
				if(null != response){
					if(response.contains("#")) {
						Integer version = Integer.valueOf(response.split("#")[1]); //Getting version value
						if(version>tempVersion){
							tempVersion = version;
							result = response.split("#")[0]; //Putting in result only if version value is greater than tempvalue
						}
					}else{
						continue;
					}
				}else{
					continue;
				}
			}

			String[] row = new String[]{key, result};
			matrixCursor.addRow(row);
		}
		querying = 0;
		return matrixCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			Log.i("Server Task started","server task");
			ServerSocket serverSocket = sockets[0];
			ObjectInputStream inputStream;
			ObjectOutputStream outputStream;
			Socket socket;
			Message message = new Message();
			FileOutputStream fileOutputStream;
			StringBuilder sb = new StringBuilder();
			InputStream is;
			while (true) {
				try {
					socket = serverSocket.accept();
					//Initialize input and output stream for duplex socket connection
					inputStream = new ObjectInputStream(socket.getInputStream());
					outputStream = new ObjectOutputStream(socket.getOutputStream());
					message = (Message) inputStream.readObject();
					while (initialized==0) {
						try {
							Thread.sleep(200);
						} catch (Exception e) {
							Log.e("Server Task","Exception in thread");
						}
					}
					if (message.getMessageType().equals("INSERT")) {
						Log.i("Server Task started","If messagetype is insert");
						String key = message.getKey();
						String value = message.getValue();
						Integer version = versionMap.get(key);
						if (null != version) {
							version = version++;
						} else {
							version = 1;
						}
						versionMap.put(key, version);
						value = value + "#" + version; //Storing value in format value#version
						fileOutputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
						fileOutputStream.write(value.getBytes());
						fileOutputStream.close();
						//Sending response back to client to notify
						outputStream.writeObject(key + "#" + value);
						outputStream.flush();
					}
					if (message.getMessageType().equals("SINGLEQUERY")) {
						try {
							sb = new StringBuilder();
							is = getContext().openFileInput(message.getKey());
							BufferedReader br = new BufferedReader(new InputStreamReader(is));
							String temp;
							while ((temp = br.readLine()) != null) {
								sb.append(temp);
							}
							br.close();
						} catch (FileNotFoundException e) {
							Log.e(TAG, " QueryTask UnknownHostException");
						} catch (IOException ie) {
							Log.e(TAG, "QueryTask IOException");
						} catch (Exception e) {
							Log.e(TAG, "QueryTask General Exception");
						}
						Log.i("Single query","msg value-->"+sb.toString());
						message.setValue(sb.toString());
						outputStream.writeObject(message);
						outputStream.flush();

					}
					if (message.getMessageType().equals("STARQUERY")) {
						List<Message> messageList = new ArrayList<Message>();
						Message msg;
						String[] files = getContext().fileList();
						for (String file : files) {
							sb = new StringBuilder();
							msg = new Message();
							try {
								is = getContext().openFileInput(file);
								BufferedReader br = new BufferedReader(new InputStreamReader(is));
								String temp;
								while ((temp = br.readLine()) != null) {
									sb.append(temp);
								}
								br.close();
							} catch (FileNotFoundException e) {
								Log.e(TAG, " QueryTask UnknownHostException");
							} catch (IOException ie) {
								Log.e(TAG, "QueryTask IOException");
							} catch (Exception e) {
								Log.e(TAG, "QueryTask General Exception");
							}
							msg.setKey(file);
							msg.setValue(sb.toString());
							messageList.add(msg);
						}
						outputStream.writeObject(messageList);
						outputStream.flush();
					}
					if (message.getMessageType().equals("SINGLEDELETE")) {

						try {
							getContext().deleteFile(message.getKey());
						} catch (Exception e) {
							Log.e(TAG, "Delete operation failed:" + e.getMessage());
						}
					}
					if (message.getMessageType().equals("GLOBALDELETE")) {

						String[] files=getContext().fileList();
						for(String file: files){
							try {
								getContext().deleteFile(file);
							}
							catch(Exception e){
								Log.e(TAG, "Delete operation failed:" + e.getMessage());
							}
						}
					}
					if (message.getMessageType().equals("RECOVER")) {
						List<Message> messageList = new ArrayList<Message>();
						//If Insert or Query request comes stop current thread and perform that operation
						if((inserting==1) || (querying==1)){
							try {
								Log.i(TAG, "Yielding Server task as insert/query in progress");
								Thread.yield();
							} catch (Exception e) {
								Log.e("Server Task","Exception in thread");
							}
						}
						Message msg;
						String[] files = getContext().fileList();
						for (String file : files) {
							sb = new StringBuilder();
							msg = new Message();
							try {
								is = getContext().openFileInput(file);
								BufferedReader br = new BufferedReader(new InputStreamReader(is));
								String temp;
								while ((temp = br.readLine()) != null) {
									sb.append(temp);
								}
								br.close();
							} catch (FileNotFoundException e) {
								Log.e(TAG, " QueryTask UnknownHostException");
							} catch (IOException ie) {
								Log.e(TAG, "QueryTask IOException");
							} catch (Exception e) {
								Log.e(TAG, "QueryTask General Exception");
							}
							msg.setKey(file);
							msg.setValue(sb.toString());
							messageList.add(msg);

						}
						outputStream.writeObject(messageList);
						outputStream.flush();
					}
				}catch (Exception e) {
					Log.e("Server Task","Exception in server task"+e.getMessage());
				}
			}
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {

				Log.i("ClientTask","Recovery Message");
				ObjectOutputStream outputStream;
				ObjectInputStream inputStream;
				Socket socket;
				Message message = new Message();
				message.setMessageType("RECOVER");
				List<Message> localMessageList = new ArrayList<Message>();
				List<Message> globalMessageList = new ArrayList<Message>();
				for (String port: REMOTE_PORTS) {
					try {
						//Sending to all port except current port
						if (port.equals(myPort)) {
							continue;
						}
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port));
						outputStream = new ObjectOutputStream(socket.getOutputStream());
						inputStream = new ObjectInputStream(socket.getInputStream());
						outputStream.writeObject(message);
						outputStream.flush();
						localMessageList = (List<Message>)inputStream.readObject();
						globalMessageList.addAll(localMessageList);
					} catch (Exception e) {
						Log.e("Client task","Exception in client task"+e.getMessage());
					}
				}
				String predecessor1 = getPredecessors(myPort,1);
				String predecessor2 = getPredecessors(myPort,2);
				for (Message temp : globalMessageList) {
					try {
						String key = temp.getKey();
						String valueData = temp.getValue();
						String keyHash = genHash(key);
						String cord = keyLookUp(keyHash);

						if (cord.equals(myPort) || cord.equals(predecessor1) || cord.equals(predecessor2)) {
							Integer currentVersion = versionMap.get(key);
							Integer receivedVersion = Integer.valueOf(valueData.split("#")[1]);
							Integer finalVersion = null;
							if (null != currentVersion) {
								if (receivedVersion > currentVersion) {
									finalVersion = receivedVersion;
									versionMap.put(key,finalVersion);
									FileOutputStream fileOutputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
									fileOutputStream.write(valueData.getBytes());
									fileOutputStream.close();
								}
							}
							else{
								finalVersion = receivedVersion;
								versionMap.put(key,finalVersion);
								FileOutputStream fos = getContext().openFileOutput(key, Context.MODE_PRIVATE);
								fos.write(valueData.getBytes());
								fos.close();
							}
						}
					}catch(Exception e) {
						Log.e("Client Task","Exception in client task");
					}
				}
				initialized = 1;

			return null;
		}
	}



	private String keyLookUp(String hashNewkey){
		try {

			if(genHash(String.valueOf((Integer.parseInt(REMOTE_PORTS[0]) / 2))).compareTo(hashNewkey) >=0 || genHash(String.valueOf((Integer.parseInt(REMOTE_PORTS[4]) / 2))).compareTo(hashNewkey)<0){
				return REMOTE_PORTS[0];
			}else if(genHash(String.valueOf((Integer.parseInt(REMOTE_PORTS[1]) / 2))).compareTo(hashNewkey)>=0 && genHash(String.valueOf((Integer.parseInt(REMOTE_PORTS[0]) / 2))).compareTo(hashNewkey)<0 ){
				return REMOTE_PORTS[1];
			}else if(genHash(String.valueOf((Integer.parseInt(REMOTE_PORTS[2]) / 2))).compareTo(hashNewkey)>=0 && genHash(String.valueOf((Integer.parseInt(REMOTE_PORTS[1]) / 2))).compareTo(hashNewkey)<0 ){
				return REMOTE_PORTS[2];
			}else if(genHash(String.valueOf((Integer.parseInt(REMOTE_PORTS[3]) / 2))).compareTo(hashNewkey)>=0 && genHash(String.valueOf((Integer.parseInt(REMOTE_PORTS[2]) / 2))).compareTo(hashNewkey)<0 ){
				return REMOTE_PORTS[3];
			}else if(genHash(String.valueOf((Integer.parseInt(REMOTE_PORTS[4]) / 2))).compareTo(hashNewkey)>=0 && genHash(String.valueOf((Integer.parseInt(REMOTE_PORTS[3]) / 2))).compareTo(hashNewkey)<0 ){
				return REMOTE_PORTS[4];
			}

		}catch(Exception e){
			Log.e(TAG,"Key LookUp Error "+ e.getMessage());
		}
		Log.e(TAG, "KeyLookupError Null value returned");
		return null;
	}

	private String getPredecessors(String node, int num ){
		if(num==1) {
			if (node.equals("11124")) {
				return "11120";
			} else if (node.equals("11112")) {
				return "11124";
			} else if (node.equals("11108")) {
				return "11112";
			} else if (node.equals("11116")) {
				return "11108";
			} else if (node.equals("11120")) {
				return "11116";
			}
		}
		if(num==2) {
			if (node.equals("11124")) {
				return "11116";
			} else if (node.equals("11112")) {
				return "11120";
			} else if (node.equals("11108")) {
				return "11124";
			} else if (node.equals("11116")) {
				return "11112";
			} else if (node.equals("11120")) {
				return "11108";
			}
		}

		return null;
	}
	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}
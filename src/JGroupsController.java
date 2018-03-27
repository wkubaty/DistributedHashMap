import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.io.*;
import java.net.InetAddress;

public class JGroupsController extends ReceiverAdapter {
    private StringMap stringMap = new StringMap();
    private JChannel jChannel;
    private String userName = System.getProperty("user", "n/a");

    public void start() throws Exception{
        jChannel = new JChannel();
        setProtocol();
        jChannel.setReceiver(this);
        jChannel.connect("StringMapChannel");
        jChannel.getState(null, 10000);
        eventLoop();
        jChannel.close();
    }
    private void setProtocol() throws Exception{
        ProtocolStack stack=new ProtocolStack();
        jChannel.setProtocolStack(stack);
        stack.addProtocol(new UDP().setValue("mcast_group_addr", InetAddress.getByName("230.0.0.50")))
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL().setValue("timeout", 12000).setValue("interval", 3000))
                .addProtocol(new VERIFY_SUSPECT())
                .addProtocol(new BARRIER())
                .addProtocol(new NAKACK2())
                .addProtocol(new UNICAST3())
                .addProtocol(new STABLE())
                .addProtocol(new GMS())
                .addProtocol(new UFC())
                .addProtocol(new MFC())
                .addProtocol(new FRAG2())
                .addProtocol(new STATE_TRANSFER());

        stack.init();
    }
    private void eventLoop(){
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

        while (true){
            try {
                System.out.println(">");
                String line = in.readLine();
                String delimiter = "[ ]+";
                String[] tokens = line.split(delimiter);
                String operation = tokens[0].toLowerCase();
                System.out.println("operation: " + operation);
                if(operation.equals("c")){
                    String key = tokens[1];
                    System.out.println("checking: key: " + key);
                    System.out.println(stringMap.containsKey(key));
                }
                else if(operation.equals("g")) {
                    String key = tokens[1];
                    System.out.println("getting: key: " + key);
                    System.out.println(stringMap.get(key));
                }
                else if(operation.equals("p") || operation.equals("r")){
                    line = "[" + userName + "] " + line;
                    Message msg = new Message(null, null, line);
                    jChannel.send(msg);
                }
                else if(line.startsWith("exit")){
                    break;
                }
                else{
                    System.out.print("Type <'C','G','R'> <key> or <'P'> <key value> or 'exit'");
                }
            }

            catch(Exception e){

            }
        }

    }
    public void viewAccepted(View new_view) {
        System.out.println("** view: " + new_view);
    }


    public void receive(Message msg) {
        System.out.println("receiving");
        String line = msg.getSrc() + ": " + msg.getObject();
        System.out.println(line);

        synchronized (stringMap){
            String delimiter = "[ ]+";
            String request = (String) msg.getObject();
            String[] tokens = request.split(delimiter);
            try{
                String operation = tokens[1].toLowerCase();
                String key = tokens[2];
                if(operation.equals("p")){
                    String value = tokens[3];
                    System.out.println("putting: key: " + key + ", value: " + value);
                    stringMap.put(key, value);
                }
                else if(operation.equals("r")){
                    System.out.println("removing: key: " + key);
                    stringMap.remove(key);
                }
            }
            catch (IndexOutOfBoundsException e){
                System.out.println("Got wrong request: " + request);
                return;
            }

        }
    }
    public void getState(OutputStream output) throws Exception {
        System.out.println("Getting state");
        synchronized(stringMap) {
            Util.objectToStream(stringMap, new DataOutputStream(output));
        }

    }
    public void setState(InputStream input) throws Exception{
        System.out.println("Setting state");
        StringMap map  = (StringMap) Util.objectFromStream(new DataInputStream(input));
        synchronized (stringMap){
            stringMap.clear();
            stringMap = map;
        }
        printStringMap();
    }
    public void printStringMap(){
        System.out.println("stringMap: ");
        for(String key: stringMap.keySet()){
            System.out.println("key: " + key + ", value: " + stringMap.get(key));
        }

    }


}

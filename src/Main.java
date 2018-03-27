
public class Main {

    public static void main(String[] args) throws Exception {
    System.setProperty("java.net.preferIPv4Stack", "true");
    new JGroupsController().start();
    }
}

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Created by Robinson on 02/11/2016.
 */

public class Hbase {

    public static void main(String[] args) throws IOException{

        String firstname, lastname, address, birthdate, email, BF, friend, choice;
        ArrayList<String> friends;
        boolean exit, otherFriend;
        HTable table;
        Scanner sc;

        sc = new Scanner(System.in);

        friends = new ArrayList<String>();

        exit = false;
        otherFriend = true;

        table = configTable("rroySocialHbase");

        while (!exit){
            friends.clear();
            otherFriend = true;

            System.out.print("1 : create a new member \n2 : exit programme \n ---> ");
            choice = sc.nextLine();

            if(choice.equals("1")){
                System.out.print("Firstname : ");
                firstname = sc.nextLine();
                System.out.print("Lastname : ");
                lastname = sc.nextLine();
                System.out.print("Address : ");
                address = sc.nextLine();
                System.out.print("Birthdate (mm/dd/yyyy) : ");
                birthdate = sc.nextLine();
                System.out.print("Email : ");
                email = sc.nextLine();

                do {
                    System.out.print("Best friends : ");
                    BF = sc.nextLine();
                }while (BF.equals(""));

                System.out.print("Now I will ask you all the " + firstname + "'s friends one by one. To finish just write stop.\n");
                do {
                    System.out.print("friend : ");
                    friend = sc.nextLine();
                    if (friend.equals("stop")){
                        otherFriend = false;
                    }else {
                        friends.add(friend);
                    }
                }while (otherFriend);
                createUser(table, firstname, lastname, address, birthdate, email, BF, friends);

            }else if(choice.equals("2") || choice.equals("exit")){
                exit = true;
            }
        }
    }

    private static void createUser(HTable t, String firstname, String lastname, String address, String birthdate, String email, String BF, ArrayList<String> friends) throws IOException{
        Put p = new Put(Bytes.toBytes(firstname));

        p.add(Bytes.toBytes("info"), Bytes.toBytes("lastname"),
                Bytes.toBytes(lastname));

        p.add(Bytes.toBytes("info"), Bytes.toBytes("address"),
                Bytes.toBytes(address));

        p.add(Bytes.toBytes("info"), Bytes.toBytes("birthdate"),
                Bytes.toBytes(birthdate));

        p.add(Bytes.toBytes("info"), Bytes.toBytes("email"),
                Bytes.toBytes(email));

        p.add(Bytes.toBytes("friends"), Bytes.toBytes("BF"),
                Bytes.toBytes(BF));

        p.add(Bytes.toBytes("friends"), Bytes.toBytes("others"), WritableUtils.toByteArray(toWritable(friends)));

        t.put(p);
    }

    private static Writable toWritable(ArrayList<String> list) {
        Writable[] content = new Writable[list.size()];
        for (int i = 0; i < content.length; i++) {
            content[i] = new Text(list.get(i));
        }
        return new ArrayWritable(Text.class, content);
    }

    private static HTable configTable(String name) throws IOException{
        Configuration config = HBaseConfiguration.create();

        return new HTable(config, name);
    }
}

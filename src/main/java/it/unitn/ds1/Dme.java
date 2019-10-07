package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Dme {
    
    static List<ActorRef> tree;
    static ActorSystem system;
    static List<Integer[]> neighbor;
    static Integer N_nodi;
    
    public static void main(String[] args) {
        
        system = ActorSystem.create("dme");
        tree = new LinkedList<ActorRef>();
        neighbor = new ArrayList<>();
        neighbor.add(new Integer[]{1});
        neighbor.add(new Integer[]{0, 2});
        neighbor.add(new Integer[]{1, 3});
        neighbor.add(new Integer[]{2, 4, 5});
        neighbor.add(new Integer[]{3});
        neighbor.add(new Integer[]{3});
                
        
                
        for(int id=0; id<neighbor.size(); id++){
            tree.add(system.actorOf(
                Node.props(id, neighbor.get(id), false), "node"+id));
        }

        
        // Ensure that no one can modify the group
        //tree = Collections.unmodifiableList(tree);

        // Send the tree node list to everyone in the group
        Node.TreeCreation join = new Node.TreeCreation(tree);
        for (ActorRef peer : tree) {
            peer.tell(join, null);
        }

        // Flood token position
        floodTokenPosition(2);

        // Creation of two token request
        /*system.scheduler().schedule(
                Duration.create(1, TimeUnit.SECONDS),
                Duration.create(5, TimeUnit.SECONDS),
                tree.get(3), new Node.StartTokenRequest(2000), system.dispatcher(), null);*/
        system.scheduler().schedule(
                Duration.create(1, TimeUnit.SECONDS),
                Duration.create(3, TimeUnit.SECONDS),
                tree.get(4), new Node.StartTokenRequest(10000), system.dispatcher(), null);
        system.scheduler().schedule(
                Duration.create(2, TimeUnit.SECONDS),
                Duration.create(5, TimeUnit.SECONDS),
                tree.get(2), new Node.StartTokenRequest(2000), system.dispatcher(), null);
        system.scheduler().schedule(
                Duration.create(2, TimeUnit.SECONDS),
                Duration.create(5, TimeUnit.SECONDS),
                tree.get(5), new Node.StartTokenRequest(2000), system.dispatcher(), null);
        
        system.scheduler().scheduleOnce(
                Duration.create(15, TimeUnit.SECONDS),
                tree.get(4), new Node.Stop(), system.dispatcher(), null);
        
        system.scheduler().scheduleOnce(
            Duration.create(20, TimeUnit.SECONDS),
            new Runnable() {
                @Override
                public void run() {
                    restart(4);
            }
        },system.dispatcher());
        
        // schedula creazione 
        
        
        System.out.println(">>> Press ENTER to exit <<<");
            
/*
        try {
            int inChar;
            System.out.println(">>> Wait for the chats to stop and press ENTER <<<");
            inChar = System.in.read();
            if(inChar != 13){
                throw new IOException();
            }
            Node.PrintHistoryMsg msg = new Node.PrintHistoryMsg();
            for (ActorRef peer : tree) {
                peer.tell(msg, null);
            }
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } catch (IOException ioe) {
        }finally{
            system.terminate();
        }
     */   

    }
    
    private static void restart(int node){         
            ActorRef a =system.actorOf(
                Node.props(node, neighbor.get(node), true), "Node"+node);
            tree.set(node,a);
         // Send the tree node list to everyone in the group
        Node.TreeCreation join = new Node.TreeCreation(tree);
        for (ActorRef peer : tree) {
            peer.tell(join, null);
        }
        
    }

    private static void floodTokenPosition(int node) {
        try {
            Thread.sleep(100);   // aspetto che tutti i nodi siano stati creati   
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Token position flooded
        tree.get(node).tell(new Node.StartTokenFlood(), null);
    }

    
}

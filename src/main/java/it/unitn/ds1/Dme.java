package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Dme {
    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("dme");

        List<ActorRef> tree = new ArrayList<>();
        int id = 1;

        // Tree creation
        tree.add(system.actorOf(
                Node.props(id++, new Integer [] {2}), "node1"));
        tree.add(system.actorOf(
                Node.props(id++, new Integer [] {1, 3}), "node2"));
        tree.add(system.actorOf(
                Node.props(id++, new Integer [] {2, 4}), "node3"));
        tree.add(system.actorOf(
                Node.props(id++, new Integer [] {3, 5, 6}), "node4"));
        tree.add(system.actorOf(
                Node.props(id++, new Integer [] {4}), "node5"));
        tree.add(system.actorOf(
                Node.props(id++, new Integer [] {4}), "node6"));

        // Ensure that no one can modify the group
        tree = Collections.unmodifiableList(tree);

        // Send the tree node list to everyone in the group
        Node.TreeCreation join = new Node.TreeCreation(tree);
        for (ActorRef peer: tree) {
            peer.tell(join, null);
        }

        // Flood token position
        floodTokenPosition(tree);

        // Creation of two token request
        system.scheduler().schedule(
                Duration.create(1, TimeUnit.SECONDS),
                Duration.create(5, TimeUnit.SECONDS),
                tree.get(3), new Node.StartTokenRequest(2000), system.dispatcher(), null);
        system.scheduler().schedule(
                Duration.create(1, TimeUnit.SECONDS),
                Duration.create(3, TimeUnit.SECONDS),
                tree.get(4), new Node.StartTokenRequest(2000), system.dispatcher(), null);

        try {
            System.out.println(">>> Wait for the chats to stop and press ENTER <<<");
            System.in.read();

            Node.PrintHistoryMsg msg = new Node.PrintHistoryMsg();
            for (ActorRef peer: tree) {
                peer.tell(msg, null);
            }
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        }
        catch (IOException ioe) {}
        system.terminate();
    }

    private static void floodTokenPosition(List<ActorRef> tree){
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Token position flooded
        tree.get(1).tell(new Node.StartTokenFlood(), null);
    }
}

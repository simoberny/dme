package it.unitn.ds1;

import akka.actor.*;
import java.io.IOException;

import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Dme{
    static ActorSystem system;
    static ActorRef master = null;

    public static void main(String[] args){
        system = ActorSystem.create("dme");
        master = system.actorOf(Props.create(ParentNode.class));
    }
}

class ParentNode extends AbstractActor{
    static List<ActorRef> tree;
    static List<Integer[]> neighbor;

    public ParentNode(){
        tree = new LinkedList<ActorRef>();
        neighbor = new ArrayList<>();
        neighbor.add(new Integer[]{1});
        neighbor.add(new Integer[]{0, 2});
        neighbor.add(new Integer[]{1, 3});
        neighbor.add(new Integer[]{2, 4, 5});
        neighbor.add(new Integer[]{3});
        neighbor.add(new Integer[]{3});

        for(int id=0; id<neighbor.size(); id++){
            tree.add(context().actorOf(
                    Node.props(id, neighbor.get(id)), "node"+id));
        }

        // Ensure that no one can modify the group
        //tree = Collections.unmodifiableList(tree);

        // Send the tree node list to everyone in the group
        Node.TreeCreation join = new Node.TreeCreation(tree);
        for (ActorRef peer : tree) {
            peer.tell(join, getSelf());
        }

        // Flood token position
        floodTokenPosition(2);

        
        getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(1, TimeUnit.SECONDS),
                tree.get(2), new Node.Stop(), getContext().getSystem().dispatcher(), getSelf());
        getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(3, TimeUnit.SECONDS),
                tree.get(0), new Node.StartTokenRequest(16000), getContext().getSystem().dispatcher(), getSelf());
        getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(5, TimeUnit.SECONDS),
                tree.get(4), new Node.StartTokenRequest(1000), getContext().getSystem().dispatcher(), getSelf());
       
        getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(12, TimeUnit.SECONDS),
                tree.get(4), new Node.StartTokenRequest(1000), getContext().getSystem().dispatcher(), getSelf());  
        
        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } 
        catch (IOException ioe) {}
        
        getContext().getSystem().terminate();
    }
    
    private void onNodeTerminated(Node.Terminated msg){
        getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(20, TimeUnit.SECONDS),
                new Runnable() {
                    @Override
                    public void run() {
                        restart(2);
                    }
                }, getContext().getSystem().dispatcher());
    }

    private void restart(int node){
        ActorRef a =  context().actorOf(Node.props(node, neighbor.get(node)), "Node" + node);
        tree.set(node,a);
        // Send the tree node list to everyone in the group
        Node.TreeCreation join = new Node.TreeCreation(tree);
        for (ActorRef peer : tree) {
            peer.tell(join, null);
        }
        tree.get(node).tell(new Node.Restart(), getSelf());
    }

    private void floodTokenPosition(int node) {
        try {
            Thread.sleep(100);   // aspetto che tutti i nodi siano stati creati
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Token position flooded
        tree.get(node).tell(new Node.StartTokenFlood(), getSelf());
    }

    

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Node.Terminated.class, this::onNodeTerminated)
                .build();
    }
}
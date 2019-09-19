package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.io.IOException;

import it.unitn.ds1.Chatter.JoinGroupMsg;
import it.unitn.ds1.Chatter.StartChatMsg;
import it.unitn.ds1.Chatter.PrintHistoryMsg;

public class CausalMulticast {
  final private static int N_LISTENERS = 10; // number of listening actors

  public static void main(String[] args) {
    // Create the 'helloakka' actor system
    final ActorSystem system = ActorSystem.create("dme");

    List<ActorRef> group = new ArrayList<>();
    int id = 0;

    // the first two peers will be participating in a conversation
    group.add(system.actorOf(
          Chatter.props(id++, "a"),  // this one will start the topic "a"
          "chatter0")); 

    group.add(system.actorOf(
          Chatter.props(id++, "a"), // this one will catch up the topic "a"
          "chatter1"));

              // the first two peers will be participating in a conversation
    group.add(system.actorOf(
          Chatter.props(id++, "b"),  // this one will start the topic "a"
          "chatter2")); 

    group.add(system.actorOf(
          Chatter.props(id++, "b"), // this one will catch up the topic "a"
          "chatter3"));

    // the rest are silent listeners: they don't have topics to discuss
    for (int i=0; i<N_LISTENERS; i++) {
      group.add(system.actorOf(Chatter.props(id++, null), "listener" + i));
    }

    // ensure that no one can modify the group 
    group = Collections.unmodifiableList(group);

    // send the group member list to everyone in the group 
    JoinGroupMsg join = new JoinGroupMsg(group);
    for (ActorRef peer: group) {
      peer.tell(join, null);
    }

    // tell the first chatter to start conversation
    group.get(0).tell(new StartChatMsg(), null);
    group.get(2).tell(new StartChatMsg(), null);

    try {
      System.out.println(">>> Wait for the chats to stop and press ENTER <<<");
      System.in.read();

      PrintHistoryMsg msg = new PrintHistoryMsg();
      for (ActorRef peer: group) {
        peer.tell(msg, null);
      }
      System.out.println(">>> Press ENTER to exit <<<");
      System.in.read();
    } 
    catch (IOException ioe) {}
    system.terminate();
  }
}

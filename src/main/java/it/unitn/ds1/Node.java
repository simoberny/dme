package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.DeadLetter;
import akka.actor.Props;
import akka.event.LoggingAdapter;
import scala.concurrent.duration.Duration;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Node extends AbstractActor {
    // Node id
    private final int id;

    // Node that have the token boolean
    private boolean token = false;

    // Boolean to identify if a node has already request the token 
    private boolean requested = false;

    // Node is in the Critical Section
    private boolean cs = false;

    // Duration of Critical Section
    private int duration;

    // ID of the next node toward the token
    private int holder_id;

    // FIFO queue of the request
    private List<TokenRequest> mq = new ArrayList<>();

    // ID and ref of the neighbors
    private List<Integer> neighbors_id;
    private List<ActorRef> neighbors_ref;

    // List of nodes for flood
    private List<ActorRef> tree; // the list of peers (the multicast group)

    //Random generator
    private Random rnd = new Random();
    
    // Variable and class of the recovery data
    static List<Neighbor_data> neighbors_data;   

    // When a privilege is sent to the dead node
    private boolean pending_privilegeMessage = false;

    /**
     * @param id        ID of the node to initialize
     * @param neighbors Neighbors array
     */
    public Node(int id, Integer[] neighbors) {
        this.id = id;
        this.neighbors_id = Arrays.asList(neighbors);
        this.neighbors_ref = new ArrayList<>();

        context().system().eventStream().subscribe(this.getSelf(), DeadLetter.class);
    }

    static public Props props(int id, Integer[] neighbors) {        
        return Props.create(Node.class, () -> new Node(id, neighbors));
    }

    // Given the ID, generate the ref array of the neighbors
    private void onTreeCreation(Node.TreeCreation msg) {
        this.tree = msg.tree;

        for (Integer id : neighbors_id)
            this.neighbors_ref.add(this.tree.get(id));

        System.out.println("FLOOD PROCEDURE: \t \t Tree update on node: " + this.id);
    }

    /**
     * Procedure to initialize the flood for token position
     * @param msg Default message placeholder
     */
    private void onStartTokenFlood(Node.StartTokenFlood msg) {
        this.token = true;
        this.holder_id = this.id;

        System.out.println("FLOOD PROCEDURE: \t \t Inizio flood token position!\n");

        FloodMsg mx = new FloodMsg(this.id, getSelf(), this.id, 0);
        multicast(mx, getSelf());
    }

    /**
     * Flood message with information of the token position
     * @param msg Info of sender flood
     */
    private void onFloodMsg(Node.FloodMsg msg) {
        this.holder_id = msg.senderId;
        //System.out.println("Nodo " + this.id + " --> Ricevuto da " + msg.senderId + " -- Il token è a " + msg.tokenId + " -- Holder: " + this.holder_id + " -- Distanza: " + msg.distance);

        FloodMsg mx = new FloodMsg(this.id, getSelf(), msg.tokenId, msg.distance +1);
        multicast(mx, msg.sender);
    }

    /**
     * Procedure to initialize a token request from a node
     * @param msg Contain the time the node will stay in the CS
     */
    private void onStartTokenRequest(Node.StartTokenRequest msg) {
        if (!this.requested && !this.token) { // If is not the token and has not already send a request      
            this.duration = msg.cs_duration;
            System.out.println("TOKEN REQUEST: \t \t Avvio richiesta token da " + this.id);
            
            TokenRequest re = new TokenRequest(this.id, this.id);
            this.mq.add(re);
            
            sendTokenRequest(this.id);
        }
    }

    /**
     * Token request endpoint
     * @param msg Contain senderID e original node that makes the request
     */
    private void onTokenRequest(Node.TokenRequest msg) {
        // if applicant not in list, add it
        if (notInList(msg))
            this.mq.add(msg);
        
        if (this.token) { // if I own the token
            System.out.println("TOKEN REQUEST: \t \t Richiesta arrivata! da (sender): " + msg.senderId + " per conto di (richiesta) " + msg.req_node_id);

            checkPrivilege();

            // Check if the node is in the CS
            if (!cs && !mq.isEmpty())
                dequeueAndPrivilege();
        } else { // Otherwise I forward            
            sendTokenRequest(msg.req_node_id);
            
        }
    }

    /**
     * Method to send and forward token request in unicast
     * @param source_req ID of node generating the request
     */
    private void sendTokenRequest(int source_req) {
        System.out.println("TOKEN REQUEST: \t \t Nodo " + this.id + " richiede il token a " + this.holder_id + " da parte di " + source_req );
        this.requested = true;
        
        // Generate request and send in unicast
        TokenRequest re = new TokenRequest(this.id, source_req);
        unicast(re, this.holder_id);
    }

    /**
     * Procedure to initialize the priviligere exchange
     */
    private void dequeueAndPrivilege() {
        // Get the first request
        TokenRequest rq = this.mq.get(0);

        System.out.print("TOKEN REQUEST: \t \t Accetto richiesta del nodo " + rq.req_node_id + " -- Mando privilegio a " + rq.senderId + " con coda richieste [");
        for (int i = 0; i < mq.size(); i++) System.out.print(this.mq.get(i).req_node_id + ", ");
        System.out.println("]");

        this.token = false;
        this.holder_id = rq.senderId;

        PrivilegeMessage pv = new PrivilegeMessage(this.id, rq.req_node_id, rq.senderId, new ArrayList<>(mq));
        unicast(pv, rq.senderId);
        
        this.mq.remove(0);
    }

    /**
     *  Procedure to check after the CS if myself is on top of the requests
     */
    private void checkPrivilege(){
        if(this.mq.size()!= 0){
            if(this.mq.get(0).req_node_id == this.id) {
                this.mq.remove(0);
            }
        }
    }

    /**
     * Priviligere message endpoint
     * @param msg Contain sender id and queue of not served requests 
     */
    private void onPrivilegeMessage(PrivilegeMessage msg) {
        System.out.print("PRIVILEGE MESSAGE: \t \t lista locale nodo: " + this.id + " : [");
        for (int i = 0; i < this.mq.size(); i++) System.out.print(this.mq.get(i).req_node_id + ", ");
        System.out.println("] ");
            
        // Merge the requests
        for (TokenRequest i : msg.requests){
            if(notInList(i)){
                TokenRequest t = new TokenRequest(msg.senderId, i.req_node_id);
                this.mq.add(t);
                System.out.println("PRIVILEGE MESSAGE: \t \t +  Aggiungo a lista: " + i.req_node_id);
            }
        }        

        if (this.id == msg.new_owner) { // If i arrive to the original applicant
            this.token = true;
            this.holder_id = this.id;
            this.requested = false;

            System.out.println("\n----------------------------------------------------------------------------------------------------");
            System.out.print("PRIVILEGE MESSAGE: \t \t \t Nuovo proprietario token! " + " Nodo - " + this.id + " con coda di richieste: [");
            for (int i = 0; i < mq.size(); i++) System.out.print(mq.get(i).req_node_id + ", ");
            System.out.println("]");
            System.out.println("----------------------------------------------------------------------------------------------------");

            enterCS();

        } else { // For the other node 
            Iterator<TokenRequest> I = mq.iterator();  
            while (I.hasNext()) {
                TokenRequest m = I.next();

                // Check if in the middle nodes is passed a request with the id equal to the new owner
                if (m.req_node_id == msg.new_owner) {
                    I.remove();                    

                    System.out.print("PRIVILEGE MESSAGE: \t \t Inoltro privilegio a " + m.senderId + " con coda [");
                    for (int i = 0; i < msg.requests.size(); i++) System.out.print(msg.requests.get(i).req_node_id + ", ");
                    System.out.println("] \n");

                    // New holder
                    this.holder_id = m.senderId;

                    // If i found the request i forward the privilege
                    PrivilegeMessage pv = new PrivilegeMessage(this.id, msg.new_owner,m.senderId, msg.requests);
                    unicast(pv, m.senderId);
                }

                // If queue is empty I have no active request
                if(this.mq.isEmpty()) requested = false;
            }
        }
    }

    /**
     * Function to check if a node is in a list
     * @param msg nodo to check
     * @return boolean
     */
    private boolean notInList(TokenRequest msg) {
        Iterator<TokenRequest> I = this.mq.iterator();
        while (I.hasNext()) {
            TokenRequest m = I.next();
            if (msg.req_node_id == m.req_node_id)
                return false;
        }

        return true;
    }
    
    private void enterCS() {
        System.out.println("\nCS: \t\t Node " + this.id + " entering CS... \n");
        this.cs = true;

        // I send a message to myself at the end of process. NOT BLOCKING
        getContext().system().scheduler().scheduleOnce(
                Duration.create(this.duration, TimeUnit.MILLISECONDS),
                getSelf(),
                new CS(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    /**
     * When the node has finished using the token
     * @param msg placeholder
     */
    public void onCS(CS msg) {
        this.cs = false;

        System.out.print("\nCS: \t\t Node " + this.id + " exiting CS... La mia coda: [");
        for (int i = 0; i < this.mq.size(); i++) System.out.print(this.mq.get(i).req_node_id + ", ");
        System.out.println("] \n");

        checkPrivilege();

        // When i exit the CS, check if i have requests on the queue
        if (!this.mq.isEmpty())
            dequeueAndPrivilege();
    }
    
    /**
     * Method to send a unicast message
     *
     * @param m  Message to send
     * @param to Id of the node to send
     */
    private void unicast(Serializable m, int to) {
        ActorRef p = tree.get(to);
        p.tell(m, getSelf());

        try {
            Thread.sleep(rnd.nextInt(10));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to send in broadcast
     *
     * @param m    Message to send
     * @param from Node sender
     */
    private void multicast(Serializable m, ActorRef from) {
        List<ActorRef> shuffledGroup = new ArrayList<>(neighbors_ref);
        Collections.shuffle(shuffledGroup);
        for (ActorRef p : shuffledGroup) {
            if (!p.equals(getSelf()) && !p.equals(from)) { // not sending to self
                p.tell(m, getSelf());
                try {
                    Thread.sleep(rnd.nextInt(10));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void printHistory(Node.PrintHistoryMsg msg) {
        System.out.printf("%02d: %b holderid: " + this.holder_id + "\n", this.id, this.token);
    }
    

    /*** FAILURE MANAGEMENT PROCEDURE ***/

    private void onRestart(Node.Restart msg){
        System.out.println("RECOVER PROCEDURE: \t \t Restarting node " + id + "  sending to all neighbor a Recover request");
        neighbors_data = new LinkedList();
        multicast(new RecoverRequest(this.id), getSelf());       
    }
        
    private void onStop(Node.Stop msg) {
        if(!cs){
            System.out.println("\n................................................................................");
            System.out.print("STOP: \t\t Node " + this.id + " stopping... La mia coda: [");
            for (int i = 0; i < mq.size(); i++) System.out.print(mq.get(i).req_node_id + ", ");
            System.out.println("]");
            System.out.println("................................................................................\n");

            getContext().parent().tell(new Terminated(msg.time,this.id), this.getSelf());
            getContext().stop(getSelf());
        }else{
            System.out.println("STOP: \t\t Node " + this.id + "is in the CS and can't be stopped! try to stop it later");
        }
    } 
    
    private void onRecoverRequest(Node.RecoverRequest m) throws InterruptedException {
        System.out.println("RECOVER PROCEDURE: \t \t Received a Recover request from "+m.id+" to "+ this.id);
        unicast(new Neighbor_data(this.id, this.mq, this.holder_id, this.requested), m.id); 
        if(this.pending_privilegeMessage){
            Thread.sleep(3000);
            this.holder_id = m.id;
            this.pending_privilegeMessage = false;
        }       
    }
    
    private void onRecoverResponse(Node.Neighbor_data d) {
        System.out.println("RECOVER PROCEDURE: \t \t Received a Recover RESPONSE from "+d.getID());
        neighbors_data.add(d);
        if(neighbors_data.size() == neighbors_id.size()){
            System.out.println("RECOVER PROCEDURE: \t \t All the neighbors data are collected, start to recover the internal varibles");
            for (Neighbor_data n : neighbors_data){
                 System.out.print("\t\t neighbor: "+n.neighbor_id+" with mq queue: [");
                 for (int i = 0; i < n.mq.size(); i++) System.out.print(n.mq.get(i).req_node_id + ", ");
                 System.out.println("],  holder_ID: "+n.holder_id+ " ha richiesto token? "+n.requested );
            }             
           
            recover_internal_varibles();
        }       
    }
    
    private void recover_internal_varibles(){
        this.holder_id = this.id;
        this.token = true;
        for (Neighbor_data n : neighbors_data) { 
            if(n.holder_id != this.id){
                //if all neighbords except one have as holder id my id, I'm not the token holder,                
                this.token = false; 
                //and my holder_id is the different one
                this.holder_id = n.neighbor_id; 
            }
            
        }
        
        if(!this.token){
            // If in the mq of my holder id an entry with my id exists request is true
            Neighbor_data holder = null;
            for (Neighbor_data n : neighbors_data) { 
                if(n.holder_id != this.id)  
                    holder = n;                
            }
            for(TokenRequest i : holder.mq){
                if(this.id == i.req_node_id)
                    this.requested = true;
            }
        }else{
            this.requested = false;
        } 
        
        /** Find all the neighbors that has the request = true (has generated or forwarded a request) 
         *  and join my queue with their mq    
         */
        for (Neighbor_data n : neighbors_data) {
            if(n.requested && n.holder_id == this.id){                                
                for(TokenRequest t : n.mq){
                    this.mq.add(new TokenRequest(n.neighbor_id, t.req_node_id));
                }               
            }        
        }
        
        System.out.println(this);
    }
    
    public String toString(){
        String ret = "\n--------------------------------------------------------------------------------------------------------------";
        ret += "\nDATA RECOVERED: \t \t Node id: "+this.id+ " has token: "+this.token+" has send reqest for token: "+this.requested+ " with mq queue: [";
        for (int i = 0; i < this.mq.size(); i++) {
            ret += this.mq.get(i).req_node_id + ", ";
        }
        ret += "] \n--------------------------------------------------------------------------------------------------------------";
        
        return ret;
    }

    /**
     * Catch the Akka exception when sending to a deadnode
     */
    private void onDeadLetter(DeadLetter msg){
        if(msg.sender().equals(this.getSelf())){
            if(msg.message().getClass().equals(TokenRequest.class)){ //Message is a TokenRequest  
                System.out.println("RECEIVER NON DISPONIBILE: \t: x invio token request,  da: " + this.id);

                getContext().getSystem().scheduler().scheduleOnce(
                    Duration.create(4, TimeUnit.SECONDS),
                    new Runnable() {
                        @Override
                        public void run() {
                            sendTokenRequest(((TokenRequest)msg.message()).req_node_id);
                        }
                    }, getContext().getSystem().dispatcher());
                
            }else if(msg.message().getClass().equals(PrivilegeMessage.class)){ //Privilege Message 
                this.holder_id = this.id;
                this.pending_privilegeMessage = true;
                getContext().getSystem().scheduler().scheduleOnce(
                    Duration.create(2, TimeUnit.SECONDS),
                    new Runnable() {
                        @Override
                        public void run() {
                            PrivilegeMessage m = ((PrivilegeMessage)msg.message());
                            System.out.println("RECEIVER NON DISPONIBILE: \t: nodo: " + m.next + " non dispnibile per privilegeM, messagio inviato da: " + m.senderId);

                            unicast(m, m.next);
                        }
                }, getContext().getSystem().dispatcher());
            }            
        }
    }
    
   

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Node.TreeCreation.class, this::onTreeCreation)
                .match(Node.StartTokenFlood.class, this::onStartTokenFlood)
                .match(Node.FloodMsg.class, this::onFloodMsg)
                .match(Node.PrintHistoryMsg.class, this::printHistory)
                .match(Node.StartTokenRequest.class, this::onStartTokenRequest)
                .match(Node.TokenRequest.class, this::onTokenRequest)
                .match(Node.PrivilegeMessage.class, this::onPrivilegeMessage)
                .match(Node.CS.class, this::onCS)
                .match(Node.Restart.class, this::onRestart)
                .match(Node.Stop.class, this::onStop)
                .match(Node.RecoverRequest.class, this::onRecoverRequest)
                .match(Node.Neighbor_data.class, this::onRecoverResponse)
                .match(DeadLetter.class, this::onDeadLetter)
                .build();
    }

    
    /*** Message Classes ***/
    
    public static class TreeCreation implements Serializable {
        private final List<ActorRef> tree; // list of group members

        public TreeCreation(List<ActorRef> tree) {
            this.tree = Collections.unmodifiableList(tree);
        }
    }

    public static class StartTokenFlood implements Serializable {
    }

    public static class PrintHistoryMsg implements Serializable {
    }

    public static class CS implements Serializable {}

    public static class StartTokenRequest implements Serializable {
        private final int cs_duration;

        public StartTokenRequest(int duration) {
            this.cs_duration = duration;
        }
    }

    public static class FloodMsg implements Serializable {
        public final int distance;
        public final int tokenId;  
        public final int senderId;
        public final ActorRef sender;

        /**
         * Messaggio per il flood delle informazioni sul token
         *
         * @param senderId ID nodo mittente
         * @param sender   Referenza nodo mittente
         * @param tokenId  ID nodo che detiene il token
         * @param distance Distanza in passi dal token
         */
        public FloodMsg(int senderId, ActorRef sender, int tokenId, int distance) {
            this.tokenId = tokenId;
            this.distance = distance;
            this.senderId = senderId;
            this.sender = sender;
        }
    }

    public static class TokenRequest implements Serializable {
        public final int senderId;
        public final int req_node_id;
       
        /**
         * Messaggio richiesta Token
         *
         * @param senderId    ID nodo mittente
         * @param req_node_id ID nodo che ha originato la richiesta
         */
        public TokenRequest(int senderId, int req_node_id) {
            this.senderId = senderId;
            this.req_node_id = req_node_id;
        }

        public String toString(){
            return "" + this.req_node_id;
        }
    }

    public static class PrivilegeMessage implements Serializable {
        public final int senderId;
        public final int new_owner;
        public final List<TokenRequest> requests;
        public final int next;

        /**
         * Messaggio per la risoluzione della richiesta
         *
         * @param senderId  ID nodo mittente
         * @param new_owner ID nodo che deterrà il token
         * @param requests  Vettore con le eventuali richieste in sospeso del precedente proprietario
         */
        public PrivilegeMessage(int senderId, int new_owner,int next, List<TokenRequest> requests) {
            this.senderId = senderId;
            this.new_owner = new_owner;
            this.requests = requests;
            this.next = next;
            
        }
    }
    
    public static class Stop implements Serializable {
        public int time;
        public int node;
        public Stop(int time,int node){
            this.time = time; 
            this.node = node;
        }
    }
    
    public static class Restart implements Serializable {}

    public static class Terminated implements Serializable {
        public int time;
        public int node;
        public Terminated(int time, int node){
            this.time = time;
            this.node = node;
        }
    }
        
    public static class RecoverRequest implements Serializable {
        public int id;
        public RecoverRequest(int id){
            this.id = id;
        }
    }
    
    
    public static class Neighbor_data implements Serializable {
        int neighbor_id;
        private List<TokenRequest> mq;
        int holder_id;
        boolean requested;
         
        /**
         * Data structure when recovering data 
         * @param id Id of the neighbor 
         * @param mq Requests queue of the neigh
         * @param holder_id Holder_id of the neigh
         * @param requested if neigh generated a requests
         */
        public Neighbor_data(int id, List<TokenRequest> mq,int holder_id,boolean requested){
            this.neighbor_id = id;
            this.mq = mq;
            this.holder_id = holder_id;
            this.requested = requested;            
        }
        public int getID (){
            return neighbor_id;
        }
    }
}
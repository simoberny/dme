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

    // Boolean per identificare se un nodo ha già richiesto il token
    private boolean requested = false;

    // Identifica che il nodo è nella critical section
    private boolean cs = false;

    // Duration of Critical Section
    private int duration;

    // ID del nodo relativo verso il nodo token
    private int holder_id;

    // Lista FIFO delle richieste token
    private List<TokenRequest> mq = new ArrayList<>();

    // ID e riferimento dei vicini
    private List<Integer> neighbors_id;
    private List<ActorRef> neighbors_ref;

    // List of nodes for flood
    private List<ActorRef> tree; // the list of peers (the multicast group)

    // Vector for causal events

    // Generatore di random
    private Random rnd = new Random();
    
    // Variabili e classi per la procedura di recovery
    static List<Neighbor_data> neighbors_data;       

    /**
     * @param id        ID del nodo da inizializzare
     * @param neighbors Lista vicini
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

    // Dati gli id dei vicini, vado a prendermi gli actor ref e li metto in neighbors_ref!
    private void onTreeCreation(Node.TreeCreation msg) {
        this.tree = msg.tree;

        for (Integer id : neighbors_id) {
            this.neighbors_ref.add(this.tree.get(id));
        }

        // create the vector clock
        System.out.println("FLOOD PROCEDURE: \t \t Tree update on node: " + this.id);
    }

    /**
     * Procedura per inizializzare il flood del posizionamento del token
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
     * Messaggio di flood con le informazioni sul token
     * @param msg struttura con le informazioni sul mittente del flood
     */
    private void onFloodMsg(Node.FloodMsg msg) {
        this.holder_id = msg.senderId;

        //System.out.println("Nodo " + this.id + " --> Ricevuto da " + msg.senderId + " -- Il token è a " + msg.tokenId + " -- Holder: " + this.holder_id + " -- Distanza: " + msg.distance);

        FloodMsg mx = new FloodMsg(this.id, getSelf(), msg.tokenId, msg.distance +1);
        multicast(mx, msg.sender);
    }

    /**
     * Procedura per inizializzare l'invio di una richiesta del token da parte di un nodo
     * @param msg Contiene la durata che il nodo passa nella critical section
     */
    private void onStartTokenRequest(Node.StartTokenRequest msg) {
        if (!requested && !token) {            
            this.duration = msg.cs_duration;
            System.out.println("TOKEN REQUEST: \t \t Avvio richiesta token da " + this.id);
            TokenRequest re = new TokenRequest(this.id, this.id);
            mq.add(re);
            sendTokenRequest(this.id);
        }
    }

    /**
     * Arrivo del messaggio di richiesta Token
     * @param msg Messaggio di tipo TokenRequest che contiene richiedente originale e mittente relativo
     */
    private void onTokenRequest(Node.TokenRequest msg) {

        // Se il richiedente non c'è in lista, aggiungo una entry
        if (notInList(msg))
            mq.add(msg);

        // Se ho il token
        if (this.token) {
            System.out.println("TOKEN REQUEST: \t \t Richiesta arrivata! da (sender): " + msg.senderId + " per conto di (richiesta) " + msg.req_node_id);

            checkPrivilege();

            // Controllo se il nodo token lo sta utilizzando
            if (!cs && !mq.isEmpty())
                dequeueAndPrivilege();
        } else { // Altrimenti inoltro
            sendTokenRequest(msg.req_node_id);
        }
    }

    /**
     * Metodo per l'invio/inoltro in unicast della richiesta di token
     *
     * @param source_req Id del nodo che ha generato la richiesta
     */
    private void sendTokenRequest(int source_req) {
        System.out.println("TOKEN REQUEST: \t \t Nodo " + this.id + " richiede il token a " + this.holder_id + " da parte di " + source_req );
        this.requested = true;
        
        // Creo richiesta e mando in unicast
        TokenRequest re = new TokenRequest(this.id, source_req);
        unicast(re, this.holder_id);
    }

    /**
     * Funzione per avviare la procedura per mandare il privilegio
     */
    private void dequeueAndPrivilege() {
        TokenRequest rq = this.mq.get(0);

        System.out.print("TOKEN REQUEST: \t \t Accetto richiesta del nodo " + rq.req_node_id + " -- Mando privilegio a " + rq.senderId + " con coda richieste [");
        for (int i = 0; i < mq.size(); i++) System.out.print(mq.get(i).req_node_id + ", ");
        System.out.println("]");

        this.token = false;
        this.holder_id = rq.senderId;

        PrivilegeMessage pv = new PrivilegeMessage(this.id, rq.req_node_id, new ArrayList<>(mq));
        unicast(pv, rq.senderId);

        // Rimuovo tutte le richieste in quanto le ho inoltrate con il messaggio di PRIVILEGIO al nuovo owner del token
        this.mq.remove(0);
    }

    /**
     *  Procedure to check after the CS if myself is on top of the requests
     */
    private void checkPrivilege(){
        if(mq.size()!= 0){
            if(mq.get(0).req_node_id == this.id) {
                System.out.println("CHECK PRIVILEGE: \t \t Mi auto elimino (id): " + this.id);
                mq.remove(0);
            }
        }
    }

    /**
     * Arrivo di un PRIVILEGE MESSAGE
     * @param msg contiene mittente e richieste in sospeso del nodo che possedeva il token
     */
    private void onPrivilegeMessage(PrivilegeMessage msg) {
        System.out.print("PRIVILEGE MESSAGE: \t \t lista locale nodo: " + this.id + " : [");
        for (int i = 0; i < this.mq.size(); i++) System.out.print(this.mq.get(i).req_node_id + ", ");
        System.out.println("] ");
            
        for (TokenRequest i : msg.requests){
            if(notInList(i)){
                TokenRequest t = new TokenRequest(msg.senderId, i.req_node_id);
                this.mq.add(t);
                System.out.println("PRIVILEGE MESSAGE: \t \t +  Aggiungo a lista: " + i.req_node_id);
            }
        }        

        // Se ho raggiunto il richiedente del token
        if (this.id == msg.new_owner) {
            this.token = true;
            this.holder_id = this.id;
            this.requested = false;

            System.out.println("\n----------------------------------------------------------------------------------------------------");
            System.out.print("PRIVILEGE MESSAGE: \t \t \t Nuovo proprietario token! " + " Nodo - " + this.id + " con coda di richieste: [");
            for (int i = 0; i < mq.size(); i++) System.out.print(mq.get(i).req_node_id + ", ");
            System.out.println("]");
            System.out.println("----------------------------------------------------------------------------------------------------");

            //Nella cs faccio check privlege per eliminare il primo valore nella coda che sono io
            enterCS();

        } else { // Se non l'ho raggiunto
            Iterator<TokenRequest> I = mq.iterator();  
            while (I.hasNext()) {
                TokenRequest m = I.next();

                // Controllo se nel nodo intermedio è passata una richiesta con id = a quello che sarà il nuovo owner
                if (m.req_node_id == msg.new_owner) {
                    I.remove();                    

                    System.out.print("PRIVILEGE MESSAGE: \t \t Inoltro privilegio a " + m.senderId + " con coda [");
                    for (int i = 0; i < msg.requests.size(); i++) System.out.print(msg.requests.get(i).req_node_id + ", ");
                    System.out.println("] \n");

                    // Nuovo holder
                    this.holder_id = m.senderId;

                    // Se ho trovato la richiesta, inoltro il privilegio al sender da cui arrivò la richiesta
                    PrivilegeMessage pv = new PrivilegeMessage(this.id, msg.new_owner, msg.requests);
                    unicast(pv, m.senderId);
                }

                if(this.mq.size() == 0) requested = false;
            }
        }
    }

    /**
     * Funzione per controllare che un nodo non sia in lista
     * @param msg nodo da controllare
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

        // Setto l'invio di un messaggio a se stessi per la fine del processo. NON BLOCCANTE
        getContext().system().scheduler().scheduleOnce(
                Duration.create(this.duration, TimeUnit.MILLISECONDS),
                getSelf(),
                new CS(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    /**
     * Quando il nodo ha finito di usare il token
     * @param msg
     */
    public void onCS(CS msg) {
        this.cs = false;

        System.out.print("\nCS: \t\t Node " + this.id + " exiting CS... La mia coda: [");
        for (int i = 0; i < this.mq.size(); i++) System.out.print(this.mq.get(i).req_node_id + ", ");
        System.out.println("] \n");

        checkPrivilege();

        // Se quando esco dalla critical ho già richieste nella coda, invio dreoman
        if (!this.mq.isEmpty()) {
            dequeueAndPrivilege();
        }
    }
    
    /**
     * Metodo per l'invio in unicast di un messaggio ad uno specifico nodo
     *
     * @param m  Messaggio da inviare
     * @param to L'id del nodo a cui inviare il messaggio
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
     * Metodo per l'invio di un messaggio in broadcast (tranne se stesso e il sender)
     *
     * @param m    Messaggio da inviare
     * @param from Referenza del sender
     */
    private void multicast(Serializable m, ActorRef from) { // our multicast implementation
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

    // FAILURE MANAGEMENT PROCEDURE //

    private void onRestart(Node.Restart msg){
        
        System.out.println("RECOVER PROCEDURE: \t \t Restarting node "+id+"  sending to all neighbor a Recover request");
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

            getContext().parent().tell(new Terminated(), this.getSelf());
            getContext().stop(getSelf());
        }else{
            System.out.println("STOP: \t\t Node " + this.id + "is in the CS and can't be stopped! try to stop it later");
        }
    } 
    
    private void onRecoverRequest(Node.RecoverRequest m) {
        System.out.println("RECOVER PROCEDURE: \t \t Received a Recover request from "+m.id+" to "+ this.id);
        unicast(new Neighbor_data(this.id, this.mq, this.holder_id, this.requested), m.id);       
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
        this.token = true;
        for (Neighbor_data n : neighbors_data) { 
            if(n.holder_id != this.id) this.token = false;
        }
        if(!this.token){
            Neighbor_data holder = null;
            for (Neighbor_data n : neighbors_data) { 
                if(n.holder_id != this.id)  holder = n;                
            }
            for(TokenRequest i : holder.mq){
                if(this.id == i.req_node_id){
                    this.requested = true;
                }
            }
        }else{
            this.requested = false;
        } 
        
        for (Neighbor_data n : neighbors_data) {
            if(n.requested){                                
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

    private void onDeadLetter(DeadLetter msg){
        
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

    // All the message classes

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
        public final int tokenId;       //proprietario del token
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

        /**
         * Messaggio per la risoluzione della richiesta
         *
         * @param senderId  ID nodo mittente
         * @param new_owner ID nodo che deterrà il token
         * @param requests  Vettore con le eventuali richieste in sospeso del precedente proprietario
         */
        public PrivilegeMessage(int senderId, int new_owner, List<TokenRequest> requests) {
            this.senderId = senderId;
            this.new_owner = new_owner;
            this.requests = requests;
            
        }
    }
    
    public static class Stop implements Serializable {}
    
    public static class Restart implements Serializable {}

    public static class Terminated implements Serializable {}
        
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
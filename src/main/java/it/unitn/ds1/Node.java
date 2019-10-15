

//!!!!!!!!!!!!TODO: verficare se il nodo non si è fermato perchè era nella cs allora non può nemmeno ripartire!
package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import static it.unitn.ds1.Dme.neighbor;
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
    private int[] vc;        // the local vector clock

    // Generatore di random
    private Random rnd = new Random();
    
    //variabili e classi per la procedura di recovery    
    
    static List<Neighbor_data> neighbors_data;       
    


    /**
     * @param id        ID del nodo da inizializzare
     * @param neighbors Lista vicini
     */
    public Node(int id, Integer[] neighbors) {
        this.id = id;
        this.neighbors_id = Arrays.asList(neighbors);
        this.neighbors_ref = new ArrayList<>();        
    }

    static public Props props(int id, Integer[] neighbors) {        
        return Props.create(Node.class, () -> new Node(id, neighbors));
        
    }

    //dati gli id dei vicini, vado a prendermi gli actor ref e li metto in neighbors_ref!
    private void onTreeCreation(Node.TreeCreation msg) {
        this.tree = msg.tree;

        for (Integer id : neighbors_id) {
            this.neighbors_ref.add(this.tree.get(id));
        }

        // create the vector clock
        this.vc = new int[this.tree.size()];
        System.out.printf("%s: joining a group of %d peers with ID %02d. Neighbors: " + this.neighbors_ref.toString() + "\n",
                getSelf().path().name(), this.tree.size(), this.id);
    }

    /**
     * Procedura per inizializzare il flood del posizionamento del token
     * @param msg Default message placeholder
     */
    private void onStartTokenFlood(Node.StartTokenFlood msg) {
        this.token = true;
        this.holder_id = this.id;

        //System.out.println("Nodo " + this.id + " --> Mando in flood pos token!\n");

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
            this.requested = true;
            this.duration = msg.cs_duration;
            System.out.println("Avvio richiesta token da " + this.id + "\n");
            sendTokenRequest(this.id);
        }
    }

    /**
     * Arrivo del messaggio di richiesta Token
     * @param msg Messaggio di tipo TokenRequest che contiene richiedente originale e mittente relativo
     */
    private void onTokenRequest(Node.TokenRequest msg) {
        updateVC(msg.vc);

        // Se il richiedente non c'è in lista, aggiungo una entry
        if (notInList(msg))
            mq.add(msg);

        // Se ho il token
        if (this.token) {
            System.out.println("Richiesta arrivata! \n");

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
        vc[id]++;
        System.out.println("Nodo " + this.id + " richiede il token a " + this.holder_id + " da parte di " + source_req + " -- vc: " + Arrays.toString(this.vc));

        // Creo richiesta e mando in unicast
        TokenRequest re = new TokenRequest(this.id, source_req, vc);
        unicast(re, this.holder_id);
    }

    /**
     * Funzione per avviare la procedura per mandare il privilegio
     */
    private void dequeueAndPrivilege() {
        TokenRequest rq = mq.get(0);
        mq.remove(0);

        System.out.print("Accetto richiesta del nodo " + rq.req_node_id + " -- Mando privilegio a " + rq.senderId + " con coda richieste [");
        for (int i = 0; i < mq.size(); i++) System.out.print(mq.get(i).req_node_id + ", ");
        System.out.println("] \n");

        this.token = false;
        this.holder_id = rq.senderId;

        PrivilegeMessage pv = new PrivilegeMessage(this.id, rq.req_node_id, mq, vc);
        unicast(pv, rq.senderId);

        // Rimuovo tutte le richieste in quanto le ho inoltrate con il messaggio di PRIVILEGIO al nuovo owner del token
        mq.clear();
    }

    private void checkPrivilege(){
        if(mq.size()!= 0){
            if(mq.get(0).req_node_id == this.id)
                mq.remove(0);
        }
    }

    /**
     * Arrivo di un PRIVILEGE MESSAGE
     * @param msg contiene mittente e richieste in sospeso del nodo che possedeva il token
     */
    private void onPrivilegeMessage(PrivilegeMessage msg) {
        updateVC(msg.vc);

        // Se ho raggiunto il richiedente del token
        if (this.id == msg.new_owner) {
            this.token = true;
            this.holder_id = this.id;
            this.requested = false;

            // Assegno al nuovo owner le eventuali richieste ancora in sospeso
            for(int i = 0; i < msg.requests.size(); i++) {
                TokenRequest t = new TokenRequest(msg.senderId, msg.requests.get(i).req_node_id, msg.requests.get(i).vc);
                this.mq.add(t);
            }

            System.out.println("Nuovo proprietario token! " + " Nodo - " + this.id + " con coda di richieste: [");
            for (int i = 0; i < mq.size(); i++) System.out.print(mq.get(i).req_node_id + ", ");
            System.out.println("] \n");

            //Nodo entra nella critical section
            enterCS();

        } else { // Se non l'ho raggiunto
            Iterator<TokenRequest> I = mq.iterator();  
            while (I.hasNext()) {
                TokenRequest m = I.next();

                // Controllo se nel nodo intermedio è passata una richiesta con id = a quello che sarà il nuovo owner
                if (m.req_node_id == msg.new_owner) {
                    I.remove();

                    System.out.print("Inoltro privilegio a " + m.senderId + " -- vc: " + Arrays.toString(this.vc) + " con coda [");

                    for (int i = 0; i < msg.requests.size(); i++) System.out.print(msg.requests.get(i).req_node_id + ", ");

                    System.out.println("] \n");

                    // Nuovo holder
                    this.holder_id = m.senderId;

                    // Se ho trovato la richiesta, inoltro il privilegio al sender da cui arrivò la richiesta
                    PrivilegeMessage pv = new PrivilegeMessage(this.id, msg.new_owner, msg.requests, vc);
                    unicast(pv, m.senderId);
                }
            }
        }
    }

    /**
     * Funzione per controllare che un nodo non sia in lista
     * @param msg nodo da controllare
     * @return boolean
     */
    private boolean notInList(TokenRequest msg) {
        Iterator<TokenRequest> I = mq.iterator();
        while (I.hasNext()) {
            TokenRequest m = I.next();

            if (msg.req_node_id == m.req_node_id)
                return false;
        }

        return true;
    }

    private void updateVC(int[] msgVC) {
        vc[id]++;
        for (int i = 0; i < vc.length; i++)
            if (i != id) vc[i] = Math.max(vc[i], msgVC[i]);
    }

    private void enterCS() {
        System.out.println("Node " + this.id + " entering CS... ");
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
        System.out.print("Node " + this.id + " exiting CS... La mia coda: [");

        for (int i = 0; i < mq.size(); i++) System.out.print(mq.get(i).req_node_id + ", ");

        System.out.println("] \n");

        checkPrivilege();

        // Se quando esco dalla critical ho già richieste nella coda, invio dreoman
        if (!mq.isEmpty()) {
            dequeueAndPrivilege();
        }
    }
    /**
     * Destroy a node (for failure simulation); to restart the node call props with restart param true
     * @param msg
     */
    
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
    
    
    
    /////////////////////////// failure management porcedures/////////////////////////////////
    
    
    private void onRestart(Node.Restart msg){
        
        System.out.println("RECOVER PROCEDURE \t \t Restarting node "+id+"  sending to all neighbor a Recover request");
        neighbors_data = new LinkedList();
        multicast(new RecoverRequest(this.id), getSelf());       
    }
        
    private void onStop(Node.Stop msg) {
        if(!cs){
            System.out.print("SSSSSTOP  \t \t Node " + this.id + " stopping... La mia coda: [");

            for (int i = 0; i < mq.size(); i++) System.out.print(mq.get(i).req_node_id + ", ");

            System.out.println("] \n");
            getContext().stop(getSelf());
        }else{
            System.out.println("Node " + this.id + "is in the CS and can't be stopped! try to stop it later");            
        }
    } 
    
    private void onRecoverRequest(Node.RecoverRequest m) {
        System.out.println("RECOVER PROCEDURE \t \t Received a Recover request from "+m.id+" to "+ this.id);
        unicast(new Neighbor_data(this.id, this.mq, this.holder_id, this.requested), m.id);       
    }
    
    private void onRecoverResponse(Node.Neighbor_data d) {
        System.out.println("RECOVER PROCEDURE \t \t Received a Recover RESPONSE from "+d.getID());
        neighbors_data.add(d);
        if(neighbors_data.size() == neighbors_id.size()){
            System.out.println("RECOVER PROCEDURE \t \t All the neighbors data are collected, start to recover the internal varibles");
            recover_internal_varibles();
        }       
    }
    
    private void recover_internal_varibles(){
         for (Neighbor_data n : neighbors_data) {
            if( n.requested && n.holder_id==this.id){
                System.out.println("caso 2 per "+n.holder_id+" aggiungo il nodo all coda mq");
                mq.add()
            }
            if
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
                .build();
    }
    
    //all the message classes

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
        public final int[] vc;

        /**
         * Messaggio richiesta Token
         *
         * @param senderId    ID nodo mittente
         * @param req_node_id ID nodo che ha originato la richiesta
         * @param vc          Vettore Vectorclock
         */
        public TokenRequest(int senderId, int req_node_id, int[] vc) {
            this.senderId = senderId;
            this.req_node_id = req_node_id;
            this.vc = new int[vc.length];
            for (int i = 0; i < vc.length; i++)
                this.vc[i] = vc[i];
        }

        public String toString(){
            return "" + this.req_node_id;
        }
    }

    public static class PrivilegeMessage implements Serializable {
        public final int senderId;
        public final int new_owner;
        public final List<TokenRequest> requests;
        public final int[] vc;

        /**
         * Messaggio per la risoluzione della richiesta
         *
         * @param senderId  ID nodo mittente
         * @param new_owner ID nodo che deterrà il token
         * @param requests  Vettore con le eventuali richieste in sospeso del precedente proprietario
         */
        public PrivilegeMessage(int senderId, int new_owner, List<TokenRequest> requests, int[] vc) {
            this.senderId = senderId;
            this.new_owner = new_owner;
            this.requests = requests;
            this.vc = new int[vc.length];
            for (int i = 0; i < vc.length; i++)
                this.vc[i] = vc[i];
        }
    }
    
    public static class Stop implements Serializable {}
    
    public static class Restart implements Serializable {}
        
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
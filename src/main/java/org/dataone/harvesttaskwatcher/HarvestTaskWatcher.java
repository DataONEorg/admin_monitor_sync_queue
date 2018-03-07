/*
 * Connect to the hzProcess group and monitor additionas and removals on the
 * hzSyncObjectQueue queue. Also periodically reports the size of the queue
 * and sends that metric to measure-unm-1.dataone.org via udp
 *
 * Build with:
 *  mvn package
 *
 */
package org.dataone.harvesttaskwatcher;

import java.util.concurrent.Callable;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.Instance;
import com.hazelcast.core.Instance.InstanceType;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import java.util.Collection;

//https://github.com/tim-group/java-statsd-client
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;

//
import org.dataone.cn.synchronization.types.SyncObject;

/**
 *
 * @author vieglais
 *
 * Example startup:
 *   java -Dlogback.configurationFile=logback.xml \
 *     -jar harvest-task-watcher-1.0-SNAPSHOT-jar-with-dependencies.jar \
 *     -p the password
 *     -m stage.syncqueue
 */
@Command(name="HarvestTaskWatcher")
public class HarvestTaskWatcher implements Callable<Void>, ItemListener {

    @Option(names={"-i","--info"}, 
            description="Show cluster structures and exit")
    Boolean show_info_exit = false;
    
    @Option(names={"--ttl","-t"}, 
            description="Time to refresh (5000ms)")
    Integer ttl_refresh=5000;
    
    @Option(names={"--group","-g"}, 
            description="Group (hzProcess)")
    String group_name = "hzProcess";

    @Option(names={"-p","--password"}, 
            description="Connection password")
    String client_password;
    
    @Option(names={"-m","--metric"}, 
            description="Name of statsd metric (production.syncqueue, stage.syncqueue, sandbox.syncqueue)")
    String metric_name = "";
    
    @Option(names={"-s", "--metric-server"}, 
            description="Statsd server name (measure-unm-1.dataone.org)")
    String metric_server = "measure-unm-1.dataone.org";
    
    @Option(names={"-q", "--queue"}, 
            description = "Name of queue to watch (hzSyncObjectQueue)")
    String queue_name = "hzSyncObjectQueue";
    
    static final Logger LOG = LoggerFactory.getLogger(HarvestTaskWatcher.class);
    private boolean done_working = false;
    private HazelcastClient client = null;
    private IQueue<Object> queue_obj = null;
    private Gson gson = new GsonBuilder().create();
    private StatsDClient statsd = null;

    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        System.setProperty( "hazelcast.logging.type", "slf4j" );        
        CommandLine.call( new HarvestTaskWatcher(), System.out, args);
    }
   
    
    public void addClient(String hz_group_name,
                          String hz_group_password,
                          String hz_address) {
        String client_key = hz_address+"/"+hz_group_name;
        LOG.info("Adding client: " + client_key);
        ClientConfig config = new ClientConfig();
        config.addAddress(hz_address);
        GroupConfig group_config = new GroupConfig();
        group_config.setName(hz_group_name);
        group_config.setPassword(hz_group_password);
        config.setGroupConfig(group_config);
        this.client = HazelcastClient.newHazelcastClient(config);
    }
    
    
    public HashMap syncObjectToHashMap(SyncObject item) {
        HashMap obj = new HashMap();
        obj.put("pid", item.getPid());
        obj.put("nodeid", item.getNodeId());
        obj.put("attempt", item.getAttempt());
        return obj;
    }
    
    //http://docs.hazelcast.org/docs/latest-development/manual/html/Distributed_Events/Distributed_Object_Events/Listening_for_Item_Events.html
    @Override
    public void itemAdded(ItemEvent ie) {
        HashMap item = syncObjectToHashMap((SyncObject) ie.getItem());
        item.put("event", "added");
        LOG.info( this.gson.toJson(item));
    }

    
    @Override
    public void itemRemoved(ItemEvent ie) {
        HashMap item = syncObjectToHashMap((SyncObject) ie.getItem());
        item.put("event", "removed");
        LOG.info( this.gson.toJson(item));
    }
    
    
    public void addQueue(String queue_name) {
        this.queue_obj = this.client.getQueue(queue_name);
        this.queue_name = queue_name;                
        this.queue_obj.addItemListener(this, true);
    }
    
    
    public HashMap getInstances() {
        HashMap info = new HashMap();
        Collection<Instance> instances = this.client.getInstances();
        for (Instance instance : instances) {
            InstanceType t = instance.getInstanceType();
            String id = instance.getId().toString();
            if (null != t) switch (t) {
                case LOCK:
                    break;
                case MAP:
                    LOG.debug("MAP instance: " + id);
                    info.put(id, "MAP");
                    break;
                case SET:
                    LOG.debug("SET instance: " + id);
                    info.put(id, "SET");
                    break;
                case QUEUE:
                    LOG.debug("QUEUE instance: " + id);
                    info.put(id, "QUEUE");
                    break;
                default:
                    break;
            }
        }
        return info;
    }

    
    public void info() {
        HashMap info = new HashMap();
        int qsize = 0;
        if (this.queue_obj != null) {
            qsize = this.queue_obj.size();
            info.put("name", this.queue_name);
            info.put("size", qsize);            
        }
        if (statsd != null) {
            statsd.recordGaugeValue("size", qsize);
        }
        LOG.info( this.gson.toJson( info ) );
    }
    
    
    @Override    
    public Void call() {
        LOG.debug(this.gson.toJson("Starting call()."));
        
        // Add shutdown hook to close Hazelcast connections if interrupted
        Thread mainThread=Thread.currentThread();
        Thread shutdownHook = new Thread() {
            @Override
            public void run() {
                try {
                    LOG.debug("\"Shutdown hook: shutting down stat\"");
                    if (client != null) {
                        //LifecycleService life = client.getLifecycleService();
                        //if (life.isRunning()) {
                        //    life.shutdown();
                        //}                        
                    } 
                    done_working = true;
                    mainThread.join();
                } catch (InterruptedException ex) {
                    java.util.logging.Logger.getLogger(HarvestTaskWatcher.class.getName()).log(Level.SEVERE, null, ex);
                }
            }            
        };

        Runtime.getRuntime().addShutdownHook(shutdownHook);
        if (!this.metric_name.equals("")) {
            statsd = new NonBlockingStatsDClient(this.metric_name, this.metric_server, 8125);
        } else {
            LOG.warn(this.gson.toJson("Metric name not set. Metrics will not be reported."));
        }
        addClient(this.group_name,this.client_password,"127.0.0.1:5702");
        
        if (show_info_exit) {
            System.out.println( this.gson.toJson(getInstances()) );
            LOG.debug("Show info and exit.");
        } else { //Loop until 'Q' pressed or ctrl-c
            LOG.info("Entering monitor loop");
            addQueue(this.queue_name);
            BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
            long start_time = System.currentTimeMillis();
            long elapsed_time = 0;
            char input_key;
            while (!this.done_working) {
                if (Thread.interrupted()) {
                    break;
                }
                elapsed_time = System.currentTimeMillis() - start_time;
                if (elapsed_time >= ttl_refresh) {
                    LOG.debug("elapsed_time: {}", elapsed_time);
                    //HashMap results = stat.getOverview(); //new HashMap();
                    //stat.writeOverview( results );                     
                    //System.out.println( stat.overviewToString( results ));
                    info();
                    start_time = System.currentTimeMillis();                        
                }
                try {
                    Thread.sleep(500);
                } catch(InterruptedException ex) {
                    LOG.info(this.gson.toJson(ex));
                    break;
                }
                //Check for 'Q' key pressed
                try {
                    if (input.ready()) {
                        input_key = (char)input.read();
                        if (input_key == 'Q') {
                            LOG.debug("Break by scanner");
                            break;
                        }
                    }
                } catch (IOException ex) {
                    LOG.debug("inpute exception: {}", ex);
                }
            }
            LOG.debug("Exiting while loop.");
        }
        //Shutdown the hazelcast connection if any are open.
        client.shutdown();
        while (client.isActive()) {
            try {
                Thread.sleep(500);
            } catch(InterruptedException ex) {
                LOG.info("Shutdown forced", ex);
                break;
            }
        }
        return null;
    }

}

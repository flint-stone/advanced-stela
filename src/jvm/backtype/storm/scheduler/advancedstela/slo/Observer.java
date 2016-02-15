package backtype.storm.scheduler.advancedstela.slo;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class Observer {
    private static final Logger LOG = LoggerFactory.getLogger(Observer.class);
    private static final String ALL_TIME = ":all-time";
    private static final String METRICS = "__metrics";
    private static final String SYSTEM = "__system";
    private static final String DEFAULT = "default";

    private Map config;
    private Topologies topologies;
    private NimbusClient nimbusClient;
    private File juice_log;

    public Observer(Map conf) {
        config = conf;
        //topologies = new Topologies(config);
        juice_log = new File("/tmp/output.log");
    }

    public TopologyPairs getTopologiesToBeRescaled() {
        return topologies.getTopologyPairScaling();
    }

    public void run() {
       // LOG.info("Running observer at: " + System.currentTimeMillis() / 1000);
        writeToFile(juice_log, "Running observer at: " + System.currentTimeMillis() + "\n");
        if (config != null) {
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST));
                topologies = new Topologies(config);
                topologies.constructTopologyGraphs();
                HashMap<String, Topology> allTopologies = topologies.getStelaTopologies();

                collectStatistics(allTopologies);
                calculateJuicePerSource(allTopologies);
                logFinalSourceJuicesPer(allTopologies);

            } catch (TTransportException e) {
                e.printStackTrace();
            }
        }
    }

    private void collectStatistics(HashMap<String, Topology> allTopologies) {
        for (String topologyId : allTopologies.keySet()) {
            Topology topology = allTopologies.get(topologyId);
            TopologyInfo topologyInfo = null;

            try {
                topologyInfo = nimbusClient.getClient().getTopologyInfo(topologyId);
            } catch (TException e) {
                LOG.error(e.toString());
                continue;
            }

            List<ExecutorSummary> executorSummaries = topologyInfo.get_executors();

            HashMap<String, Integer> temporaryTransferred = new HashMap<>();
            HashMap<String, HashMap<String, Integer>> temporaryExecuted = new HashMap<>();

            for (ExecutorSummary executor : executorSummaries) {
                String componentId = executor.get_component_id();
                Component component = topology.getAllComponents().get(componentId);

                if (component == null) {
                    continue;
                }

                ExecutorStats stats = executor.get_stats();

                if (stats == null) {
                    continue;
                }

                ExecutorSpecificStats specific = stats.get_specific();
                Map<String, Map<String, Long>> transferred = stats.get_transferred();

                if (specific.is_set_spout()) {
                    Map<String, Long> statValues = transferred.get(ALL_TIME);
                    for (String key : statValues.keySet()) {
                        if (DEFAULT.equals(key)) {
                            if (!temporaryTransferred.containsKey(componentId)) {
                                temporaryTransferred.put(componentId, 0);
                            }
                            temporaryTransferred.put(componentId, temporaryTransferred.get(componentId) +
                                    statValues.get(key).intValue());
                        }
                    }
                } else {
                    Map<String, Long> statValues = transferred.get(ALL_TIME);
                    for (String key : statValues.keySet()) {
                        if (DEFAULT.equals(key)) {
                            if (!temporaryTransferred.containsKey(componentId)) {
                                temporaryTransferred.put(componentId, 0);
                            }
                            temporaryTransferred.put(componentId, temporaryTransferred.get(componentId) +
                                    statValues.get(key).intValue());
                        }
                    }

                    Map<String, Map<GlobalStreamId, Long>> executed = specific.get_bolt().get_executed();
                    Map<GlobalStreamId, Long> executedStatValues = executed.get(ALL_TIME);
                    for (GlobalStreamId streamId : executedStatValues.keySet()) {
                        if (!temporaryExecuted.containsKey(componentId)) {
                            temporaryExecuted.put(componentId, new HashMap<String, Integer>());
                        }

                        if (!temporaryExecuted.get(componentId).containsKey(streamId.get_componentId())) {
                            temporaryExecuted.get(componentId).put(streamId.get_componentId(), 0);
                        }

                        temporaryExecuted.get(componentId).put(streamId.get_componentId(),
                                temporaryExecuted.get(componentId).get(streamId.get_componentId()) +
                                        executedStatValues.get(streamId).intValue());
                    }
                }
            }

            for (String componentId : topology.getAllComponents().keySet()) {
                Component component = topology.getAllComponents().get(componentId);
                if (temporaryTransferred.containsKey(componentId)) {
                    component.setCurrentTransferred(temporaryTransferred.get(componentId));
                    component.setTotalTransferred(temporaryTransferred.get(componentId));
                }

                if (temporaryExecuted.containsKey(componentId)) {
                    HashMap<String, Integer> executedValues = temporaryExecuted.get(componentId);
                    for (String source : executedValues.keySet()) {
                        component.addCurrentExecuted(source, executedValues.get(source));
                        component.addTotalExecuted(source, executedValues.get(source));
                    }
                }
            }
        }
    }

    private void calculateJuicePerSource(HashMap<String, Topology> allTopologies) {

        for (String topologyId : allTopologies.keySet()) {
            Topology topology = allTopologies.get(topologyId);
            HashMap<String, Component> spouts = topology.getSpouts();

            HashMap<String, Component> parents = new HashMap<String, Component>();
            for (Component spout : spouts.values()) {
                HashSet<String> children = spout.getChildren();
                for (String child : children) {


                    Component component = topology.getAllComponents().get(child);

                    Integer currentTransferred = spout.getCurrentTransferred();
                    Integer executed = component.getCurrentExecuted().get(spout.getId());

                    if (executed == null || currentTransferred == null) {
                        continue;
                    }

                    Double value;
                    if (currentTransferred == 0) {
                        value = 1.0;
                    } else {
                        value = ((double) executed) / (double) currentTransferred;
                    }
                    writeToFile(juice_log, topologyId + "," +  component.getId() + "," + currentTransferred + "," + executed + "," + value + "\n");

                    component.addSpoutTransfer(spout.getId(), value);
                    parents.put(child, component);
                }

            }

            while (!parents.isEmpty()) {
                HashMap<String, Component> children = new HashMap<String, Component>();
                for (Component bolt : parents.values()) {
                    HashSet<String> boltChildren = bolt.getChildren();

                    for (String child : boltChildren) {
                        Component stelaComponent = topology.getAllComponents().get(child);

                     Integer currentTransferred = bolt.getCurrentTransferred();
                        Integer executed = stelaComponent.getCurrentExecuted().get(bolt.getId());


                        if (executed == null || currentTransferred == null) {
                            continue;
                        }

                        Double value;
                        if (currentTransferred == 0) {
                            value = 1.0;
                        } else {
                            value = ((double) executed) / (double) currentTransferred;
                        }
                        
                        
                        
                        for (String source : bolt.getSpoutTransfer().keySet()) {
                        	writeToFile(juice_log, "before calculation" + topologyId + "," +  stelaComponent.getId() + "," + value + ","  + source + "," + bolt.getSpoutTransfer().get(source) + "\n");
                        	if(stelaComponent.getSpoutTransfer().get(source)!=null){
                        		writeToFile(juice_log, "!!!spouttranfser is not null:" + stelaComponent.getSpoutTransfer().get(source));
                        	}
                        	stelaComponent.addSpoutTransfer(source, value * bolt.getSpoutTransfer().get(source));

                            writeToFile(juice_log, topologyId + "," +  stelaComponent.getId() + "," + currentTransferred + "," + executed + "," + source + "," + stelaComponent.getSpoutTransfer().get(source) + "\n");

                        }
                        children.put(stelaComponent.getId(), stelaComponent);
                    }
                }

                parents = children;
            }
        }
      //  System.out.println("End of bolts ");
      //  System.out.println("\n\n\n\n\n");

    }

    private void logFinalSourceJuicesPer(HashMap<String, Topology> allTopologies) {
        LOG.info("**************************************************************************************************");

        for (String topologyId : allTopologies.keySet()) {
            Double calculatedSLO = 0.0;
            Topology topology = allTopologies.get(topologyId);

            LOG.info("User specified SLO for topology {} is {}", topologyId, topology.getUserSpecifiedSLO());

            for (Component bolt : topology.getBolts().values()) {
                if (bolt.getChildren().isEmpty()) {
                    for (Double sourceProportion : bolt.getSpoutTransfer().values()) {
                    	writeToFile(juice_log, bolt.getId() +  ": source proportion: " +sourceProportion+ "\n");
                        calculatedSLO += sourceProportion;
                    }
                }
            }

            calculatedSLO = calculatedSLO / topology.getSpouts().size();
            topology.setMeasuredSLOs(calculatedSLO);
            LOG.info("Measured SLO for topology {} for this run is {} and average slo is {}.", topologyId, calculatedSLO,
                    topology.getMeasuredSLO());
            LOG.info("**************************************************************************************************");
            //System.out.println("**************************************************************************************************");
            //System.out.println("Measured SLO for topology " + topologyId + " for this run is " + calculatedSLO + " and average slo is " +
            //        topology.getMeasuredSLO());
            //System.out.println("**************************************************************************************************");

          //  writeToFile(juice_log, topologyId + "," + calculatedSLO + "," + topology.getMeasuredSLO() + "," + System.currentTimeMillis() + "\n");
            writeToFile(juice_log, topologyId + "," + calculatedSLO + "," + topology.getMeasuredSLO() + "," + System.currentTimeMillis() + "\n");
        }

    }

    public void clearTopologySLOs(String topologyId) {
        topologies.remove(topologyId);
    }

    public void writeToFile(File file, String data) {
        try {
            FileWriter fileWritter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            bufferWritter.append(data);
            bufferWritter.close();
            fileWritter.close();
            LOG.info("wrote to slo file {}",  data);
        } catch (IOException ex) {
            LOG.info("error! writing to file {}", ex);
        }
    }

    public void updateLastRebalancedTime(String topologyId, Long time)
    {
        topologies.updateLastRebalancedTime(topologyId, time);
    }
}

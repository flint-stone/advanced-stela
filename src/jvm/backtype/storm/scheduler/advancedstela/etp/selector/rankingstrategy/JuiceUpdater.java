package backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import backtype.storm.scheduler.advancedstela.etp.Component;
import backtype.storm.scheduler.advancedstela.etp.TopologySchedule;
import backtype.storm.scheduler.advancedstela.slo.Topology;

public class JuiceUpdater {
	
	public static Double juiceUpadate(TopologySchedule ts, TreeMap<String, Double> expectedEmitRate, TreeMap<String, Double> expectedExecutedRates, ArrayList<Component> sourceList){
		
		calculateJuicePerSource(ts, expectedEmitRate, expectedExecutedRates, sourceList);
		return 0.0;
	}
	
	private static void calculateJuicePerSource(TopologySchedule ts, TreeMap<String, Double> expectedEmitRate, TreeMap<String, Double> expectedExecutedRates, ArrayList<Component> sourceList) {
		A
		for(Component source: sourceList){
			
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
                    writeToFile(outlier_log, topologyId + "," + spout.getId() + "," + currentTransferred + "," + executed + "," + value + "\n");

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
                            stelaComponent.addSpoutTransfer(source,
                                    value * bolt.getSpoutTransfer().get(source));

                            writeToFile(outlier_log, topologyId + "," + bolt.getId() + "," + currentTransferred + "," + executed + "," + value + "\n");

                        }
                        children.put(stelaComponent.getId(), stelaComponent);
                    }
                }

                parents = children;
            }
        }

    }

    private void logFinalSourceJuicesPer(HashMap<String, Topology> allTopologies) {


        for (String topologyId : allTopologies.keySet()) {
            Double calculatedSLO = 0.0;
            Topology topology = allTopologies.get(topologyId);

            int spouts_transferred = 0;
            int sink_executed = 0;

            for (Map.Entry <String, Component> spout : topology.getSpouts().entrySet())
            {
                spouts_transferred += spout.getValue().getCurrentTransferred();
            }

            for (Component bolt : topology.getBolts().values()) {
                if (bolt.getChildren().isEmpty()) {
                    for (Double sourceProportion : bolt.getSpoutTransfer().values()) {
                        calculatedSLO += sourceProportion;
                    }
                    for (Integer boltExecuted : bolt.getCurrentExecuted().values()) {
                        sink_executed += boltExecuted;
                    }
                }
            }

            calculatedSLO = calculatedSLO / topology.getSpouts().size();
            topology.setMeasuredSLOs(calculatedSLO);
            writeToFile(juice_log, topologyId + "," + calculatedSLO + "," + topology.getMeasuredSLO() + "," + spouts_transferred + "," + sink_executed + "," + System.currentTimeMillis() + "\n");
            writeToFile(outlier_log, topologyId + "," + calculatedSLO + "," + topology.getMeasuredSLO() + "," + System.currentTimeMillis() + "\n");
            writeToFile(flatline_log, topologyId + "," + calculatedSLO + "," + topology.getMeasuredSLO() + "," + System.currentTimeMillis() + "\n");
        }

    }

}

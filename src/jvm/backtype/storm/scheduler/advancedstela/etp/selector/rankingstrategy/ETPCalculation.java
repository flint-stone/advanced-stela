package backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import backtype.storm.scheduler.advancedstela.etp.Component;
import backtype.storm.scheduler.advancedstela.etp.TopologySchedule;
import backtype.storm.scheduler.advancedstela.etp.TopologyStatistics;

public class ETPCalculation {

	public static void congestionDetection(TopologySchedule topologySchedule, TreeMap<String, Double> expectedExecutedRates, TreeMap<String, Double> expectedEmitRates, HashMap<Component, Double> congestionMap) {
	    HashMap<String, Component> components = topologySchedule.getComponents();
	    for (Map.Entry<String, Double> componentRate : expectedExecutedRates.entrySet()) {
	        Double out = componentRate.getValue();
	        Double in = 0.0;
	
	        Component self = components.get(componentRate.getKey());
	
	        if (self.getParents().size() != 0) {
	            for (String parent : self.getParents()) {
	                in += expectedEmitRates.get(parent);
	            }
	        }
	
	        if (in > 1.2 * out) {
	            Double io = in - out;
	            congestionMap.put(self, io);
	        }
	    }
	}

	public static Double etpCalculation(TopologySchedule topologySchedule, Component component, HashMap<String, Double> sinksMap, HashMap<Component, Double> congestionMap) {
	    Double ret = 0.0;
	    if (component.getChildren().size() == 0) {
	        return sinksMap.get(component.getId());
	    }
	
	    HashMap<String, Component> components = topologySchedule.getComponents();
	    for (String c : component.getChildren()) {
	        Component child = components.get(c);
	        if (congestionMap.get(child)==null) {
	            ret = ret + etpCalculation(topologySchedule, child, sinksMap, congestionMap);
	        }
	    }
	
	    return ret;
	}

	public static void collectRates(TopologySchedule topologySchedule, TopologyStatistics topologyStatistics, HashMap<String, Double> componentEmitRates, HashMap<String, Double> componentExecuteRates, HashMap<String, Integer> parallelism, TreeMap<String, Double> expectedEmitRates, TreeMap<String, Double> expectedExecutedRates, ArrayList<Component> sourceList) {
	    for (Map.Entry<String, List<Integer>> emitThroughput : topologyStatistics.getEmitThroughputHistory().entrySet()) {
	        componentEmitRates.put(emitThroughput.getKey(), computeMovingAverage(emitThroughput.getValue()));
	    }
	
	    expectedEmitRates.putAll(componentEmitRates);
	
	    for (Map.Entry<String, List<Integer>> executeThroughput : topologyStatistics.getExecuteThroughputHistory().entrySet()) {
	        componentExecuteRates.put(executeThroughput.getKey(), computeMovingAverage(executeThroughput.getValue()));
	    }
	    expectedExecutedRates.putAll(componentExecuteRates);
	
	    for (Map.Entry<String, Component> component : topologySchedule.getComponents().entrySet()) {
	        parallelism.put(component.getKey(), component.getValue().getParallelism());
	    }
	
	    for (Component component : topologySchedule.getComponents().values()) {
	        if (component.getParents().size() == 0) {
	            sourceList.add(component);
	        }
	    }
	}

	private static Double computeMovingAverage(List<Integer> rates) {
	    Double sum = 0.0;
	    for (Integer val : rates) {
	        sum += val;
	    }
	    return sum / (rates.size() * 1.0);
	}

}

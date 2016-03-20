package backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.advancedstela.etp.Component;
import backtype.storm.scheduler.advancedstela.etp.GlobalState;
import backtype.storm.scheduler.advancedstela.etp.TopologySchedule;
import backtype.storm.scheduler.advancedstela.etp.TopologyStatistics;
import backtype.storm.scheduler.advancedstela.etp.selector.ResultComponent;

import java.util.*;

public class ETPFluidPredictionStrategy implements RankingStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalState.class);

    private String id;
    private TopologySchedule topologySchedule;
    private TopologyStatistics topologyStatistics;
    private HashMap<String, Double> componentEmitRates;
    private HashMap<String, Double> componentExecuteRates;
    private TreeMap<String, Double> expectedEmitRates;
    private TreeMap<String, Double> expectedExecutedRates;
    private HashMap<String, Integer> parallelism;
    private ArrayList<Component> sourceList;
    private HashMap<Component, Double> congestionMap;
    private HashMap<Component, Double> topologyETPMap;


    public ETPFluidPredictionStrategy(TopologySchedule tS, TopologyStatistics tStats) {
        id = tS.getId();
        topologySchedule = tS;
        topologyStatistics = tStats;
        componentEmitRates = new HashMap<String, Double>();
        componentExecuteRates = new HashMap<String, Double>();
        parallelism = new HashMap<String, Integer>();
        congestionMap = new HashMap<Component, Double>();
        expectedEmitRates = new TreeMap<String, Double>();
        expectedExecutedRates = new TreeMap<String, Double>();
        sourceList = new ArrayList<Component>();
        topologyETPMap = new HashMap<Component, Double>();
    }

    /* (non-Javadoc)
	 * @see backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.RankingStrategy#topologyETPRankDescending()
	 */
    @Override
	public ArrayList<ResultComponent> executorRankDescending() {
        ETPCalculation.collectRates(topologySchedule, topologyStatistics, componentEmitRates, componentExecuteRates, parallelism, expectedEmitRates, expectedExecutedRates, sourceList); 
        ETPCalculation.congestionDetection(topologySchedule, expectedExecutedRates, expectedEmitRates, congestionMap);

        Double totalThroughput = 0.0;
        for (Component component: topologySchedule.getComponents().values()) {
            if (component.getChildren().size() == 0) {
                totalThroughput += expectedEmitRates.get(component.getId());
            }
        }

        if (totalThroughput == 0.0) {
            LOG.info("Nothing to do as throughput is 0.");
            new TreeMap<>();
        }

        HashMap<String, Double> sinksMap = new HashMap<String, Double>();
        for (Component component: topologySchedule.getComponents().values()) {
            if (component.getChildren().size() == 0) {
                Double throughputOfSink = expectedEmitRates.get(component.getId());
                sinksMap.put(component.getId(), throughputOfSink / totalThroughput);
            }
        }

        for (Component component : topologySchedule.getComponents().values()) {
            Double score = ETPCalculation.etpCalculation(topologySchedule, component, sinksMap, congestionMap);
            topologyETPMap.put(component, score);
        }

        ArrayList<ResultComponent> resultComponents = new ArrayList<ResultComponent>();
        for (Component component: topologyETPMap.keySet()) {
            resultComponents.add(new ResultComponent(component, topologyETPMap.get(component), id, "ED-JD"));
        }

        Collections.sort(resultComponents, Collections.reverseOrder());
        return resultComponents;
    }

    
	/* (non-Javadoc)
	 * @see backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.RankingStrategy#topologyETPRankAscending()
	 */
    @Override
	public ArrayList<ResultComponent> executorRankAscending() {
    	ETPCalculation.collectRates(topologySchedule, topologyStatistics, componentEmitRates, componentExecuteRates, parallelism, expectedEmitRates, expectedExecutedRates, sourceList); 
        ETPCalculation.congestionDetection(topologySchedule, expectedExecutedRates, expectedEmitRates, congestionMap);

        Double totalThroughput = 0.0;
        for (Component component: topologySchedule.getComponents().values()) {
            if (component.getChildren().size() == 0) {
                totalThroughput += expectedEmitRates.get(component.getId());
            }
        }

        if (totalThroughput == 0.0) {
            LOG.info("Nothing to do as throughput is 0.");
            new TreeMap<>();
        }

        HashMap<String, Double> sinksMap = new HashMap<String, Double>();
        for (Component component: topologySchedule.getComponents().values()) {
            if (component.getChildren().size() == 0) {
                Double throughputOfSink = expectedEmitRates.get(component.getId());
                sinksMap.put(component.getId(), throughputOfSink / totalThroughput);
            }
        }

        //calculate ETP for each component
        for (Component component : topologySchedule.getComponents().values()) {
        	Double score = ETPCalculation.etpCalculation(topologySchedule, component, sinksMap, congestionMap);
            topologyETPMap.put(component, score);
        }

        ArrayList<ResultComponent> resultComponents = new ArrayList<ResultComponent>();
        for (Component component: topologyETPMap.keySet()) {
            resultComponents.add(new ResultComponent(component, topologyETPMap.get(component), id, "EA-JD"));
        }

        Collections.sort(resultComponents);
        return resultComponents;
    }

	public TreeMap<String, Double> getExpectedEmitRates() {
		return expectedEmitRates;
	}

	public void setExpectedEmitRates(TreeMap<String, Double> expectedEmitRates) {
		this.expectedEmitRates = expectedEmitRates;
	}

	public TreeMap<String, Double> getExpectedExecutedRates() {
		return expectedExecutedRates;
	}

	public void setExpectedExecutedRates(TreeMap<String, Double> expectedExecutedRates) {
		this.expectedExecutedRates = expectedExecutedRates;
	}

	public HashMap<Component, Double> getCongestionMap() {
		return congestionMap;
	}

	public void setCongestionMap(HashMap<Component, Double> congestionMap) {
		this.congestionMap = congestionMap;
	}

	
}

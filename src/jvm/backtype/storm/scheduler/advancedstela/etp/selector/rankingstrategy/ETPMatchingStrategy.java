package backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.advancedstela.etp.Component;
import backtype.storm.scheduler.advancedstela.etp.GlobalState;
import backtype.storm.scheduler.advancedstela.etp.TopologySchedule;
import backtype.storm.scheduler.advancedstela.etp.TopologyStatistics;
import backtype.storm.scheduler.advancedstela.etp.selector.ResultComponent;

import java.util.*;

public class ETPMatchingStrategy implements RankingStrategy {
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
    HashMap<String, Double> sbJuiceDistMap;


    public ETPMatchingStrategy(TreeMap<String, Double> expectedEmitRates2, TreeMap<String, Double> expectedExecutedRates2, ArrayList<Component> sourceList2, TopologySchedule tS, HashMap<String, Double> sbJuiceDistMap) {
    	topologySchedule = tS;
    	expectedEmitRates = expectedEmitRates2;
    	expectedExecutedRates = expectedExecutedRates2;
    	congestionMap = new HashMap<Component, Double>();
    	sourceList = sourceList2;
    	topologyETPMap = new HashMap<Component, Double>();
    	this.sbJuiceDistMap = sbJuiceDistMap;
    }

    /* (non-Javadoc)
	 * @see backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.RankingStrategy#topologyETPRankDescending()
	 */
    @Override
	public ArrayList<ResultComponent> executorRankDescending() {
        //ETPCalculation.collectRates(topologySchedule, topologyStatistics, componentEmitRates, componentExecuteRates, parallelism, expectedEmitRates, expectedExecutedRates, sourceList); 
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
            resultComponents.add(new ResultComponent(component, topologyETPMap.get(component), id, "JD-ED", this.sbJuiceDistMap.get(topologySchedule.getId())));
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
        	resultComponents.add(new ResultComponent(component, topologyETPMap.get(component), id, "JD-EA", this.sbJuiceDistMap.get(topologySchedule.getId())));
        }

        Collections.sort(resultComponents);
        return resultComponents;
    }

	
}

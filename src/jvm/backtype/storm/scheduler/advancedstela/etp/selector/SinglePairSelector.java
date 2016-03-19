package backtype.storm.scheduler.advancedstela.etp.selector;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.scheduler.advancedstela.etp.GlobalState;
import backtype.storm.scheduler.advancedstela.etp.GlobalStatistics;
import backtype.storm.scheduler.advancedstela.etp.TopologySchedule;
import backtype.storm.scheduler.advancedstela.etp.TopologyStatistics;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.ETPPerTopoStrategy;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.ExecutorPair;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.RankingStrategy;
import backtype.storm.scheduler.advancedstela.slo.Observer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SinglePairSelector implements Selector {

    @Override
	public ArrayList<ExecutorPair> selectPairs(GlobalState globalState, GlobalStatistics globalStatistics, ArrayList<String> targetIDs, ArrayList<String> victimIDs, Observer sloObserver) {

        TopologySchedule targetSchedule = globalState.getTopologySchedules().get(targetIDs.get(0));
        TopologySchedule victimSchedule = globalState.getTopologySchedules().get(victimIDs.get(0));
        TopologyStatistics targetStatistics = globalStatistics.getTopologyStatistics().get(targetIDs.get(0));
        TopologyStatistics victimStatistics = globalStatistics.getTopologyStatistics().get(victimIDs.get(0));

        RankingStrategy targetStrategy = new ETPPerTopoStrategy(targetSchedule, targetStatistics);
        RankingStrategy victimStrategy = new ETPPerTopoStrategy(victimSchedule, victimStatistics);

        ArrayList<ResultComponent> rankTarget = targetStrategy.executorRankDescending();
        ArrayList<ResultComponent> rankVictim = victimStrategy.executorRankAscending();

        for (ResultComponent victimComponent : rankVictim) {
            List<ExecutorSummary> victimExecutorDetails = victimComponent.component.getExecutorSummaries();

            for (ResultComponent targetComponent : rankTarget) {
                List<ExecutorSummary> targetExecutorDetails = targetComponent.component.getExecutorSummaries();

                for (ExecutorSummary victimSummary : victimExecutorDetails) {
                    for (ExecutorSummary targetSummary : targetExecutorDetails) {

                        if (victimSummary.get_host().equals(targetSummary.get_host())) {
                            ArrayList<ExecutorPair> ret = new ArrayList<ExecutorPair>();
                            ExecutorPair p = new ExecutorPair(targetSummary, victimSummary);
                            ret.add(p);
                        	return ret;
                        }

                    }
                }
            }
        }
        return null;
    }
}

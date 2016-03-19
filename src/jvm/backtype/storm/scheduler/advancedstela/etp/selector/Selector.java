package backtype.storm.scheduler.advancedstela.etp.selector;

import java.util.ArrayList;

import backtype.storm.scheduler.advancedstela.etp.GlobalState;
import backtype.storm.scheduler.advancedstela.etp.GlobalStatistics;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.ExecutorPair;
import backtype.storm.scheduler.advancedstela.slo.Observer;

public interface Selector {

	ArrayList<ExecutorPair> selectPairs(GlobalState globalState, GlobalStatistics globalStatistics, ArrayList<String> targetIDs, ArrayList<String> victimIDs, Observer sloObserver);

}
package backtype.storm.scheduler.advancedstela;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by fariakalim on 1/25/16.
 */
public class TopologyPicker {
	
	private File advanced_scheduling_log;
	
	public TopologyPicker(String filename){
		advanced_scheduling_log = new File("/tmp/advanced_scheduling_log.log");
	}

    public ArrayList<String> bestTargetWorstVictim (ArrayList <String> receivers, ArrayList <String> givers)
// first index should be receiver. // second index should be giver
    {
    	writeToFile(advanced_scheduling_log, "---Using Strategy BTWV---");
        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(0));
        topologyPair.add(givers.get(givers.size() - 1));
        return topologyPair;
    }

    public ArrayList<String> bestTargetBestVictim (ArrayList <String> receivers, ArrayList <String> givers)

    {
    	writeToFile(advanced_scheduling_log, "---Using Strategy BTBV---");
        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(0));
        topologyPair.add(givers.get(0));
        return topologyPair;
    }

    public ArrayList<String> worstTargetBestVictim (ArrayList <String> receivers, ArrayList <String> givers)

    {
    	writeToFile(advanced_scheduling_log, "---Using Strategy WTBV---");
        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(receivers.size() - 1));
        topologyPair.add(givers.get(0));
        return topologyPair;

    }

    public ArrayList<String> worstTargetWorstVictim (ArrayList <String> receivers, ArrayList <String> givers)

    {
    	writeToFile(advanced_scheduling_log, "---Using Strategy WTWV---");
        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(receivers.size() - 1));
        topologyPair.add(givers.get(givers.size() - 1));
        return topologyPair;

    }
    
    public void writeToFile(File file, String data) {
        try {
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.append(data);
            bufferWriter.close();
            fileWriter.close();
            //LOG.info("wrote to file {}", data);
        } catch (IOException ex) {
            //LOG.info("error! writing to file {}", ex);
        }
    }
}

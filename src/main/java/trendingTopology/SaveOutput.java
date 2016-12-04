package trendingTopology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class SaveOutput extends BaseRichBolt{

	private static final long serialVersionUID = 1L;
	OutputCollector collector;
	String windowId;
	int[] top3Count;
	String[] top3Hashtags;
	ArrayList<Tuple> top3List;
	int windowCounter;

	public SaveOutput() {
		super();
		this.top3Count = new int[3];
		this.top3Hashtags = new String[3];
		this.top3List = new ArrayList<Tuple>();
		this.windowCounter = 1;
		this.windowId = "";
	}

	@Override
	public void execute(Tuple input) {

		String newWindowId = input.getString(0);

		if (!windowId.equals("") && !windowId.equals(newWindowId)) {
			// new window

			// order the collection
			Collections.sort(top3List, new Comparator<Tuple>() {
				@Override
				public int compare(Tuple tuple1, Tuple tuple2) {
					int valTuple1 = tuple1.getInteger(3);
					int valTuple2 = tuple2.getInteger(3);

					if (valTuple1 > valTuple2) {
						return 1;
					} else if(valTuple1 == valTuple2) {
						return (tuple1.getString(2).compareTo(tuple2.getString(2)));
					} else {
						return -1;
					}
				}
			});

			// get top 3 hashtags
			String toPrint = windowCounter+"";
			int top3Count = 0;
			for (int i = 0; i<top3List.size() && top3Count < 4; i++) {
				Tuple currentTuple = top3List.get(i);
				if(currentTuple != null){
					toPrint += "," + currentTuple.getString(2) + "," + currentTuple.getInteger(3);
				} else {
					toPrint += ",null,0";
				}
				top3Count++;
			}
			while (top3Count < 3) {
				toPrint += ",null,0";
				top3Count++;
			}
			windowCounter++;

			// save them to file
			System.out.println(toPrint);


			top3List = new ArrayList<Tuple>(); 
		}
		windowId = newWindowId;
		top3List.add(input);

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Nothing to declare
	}

}

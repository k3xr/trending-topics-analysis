package master2016;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Saves the top3 hashtags and the number of occurrences into a file
 */
public class SaveOutput extends BaseRichBolt{

	private static final long serialVersionUID = 1L;
	private String windowId;
	private ArrayList<Tuple> top3List;
	private int windowCounter;
	private String folder;

	public SaveOutput(String folder)
	{
		super();
		this.top3List = new ArrayList<Tuple>();
		this.windowCounter = 1;
		this.windowId = "";
		this.folder = folder;
	}

	private void saveToFile(String lineToSave)
	{
		File file = new File(folder + "/" + top3List.get(0).getString(1) + "_" + 13);
		BufferedWriter writer = null;

		try {
			writer = new BufferedWriter(new FileWriter(file, true));
			writer.append(lineToSave);
			writer.newLine();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input)
	{
		String newWindowId = input.getString(0);

		if (!windowId.equals("") && input.getInteger(3) == 0) {
			// order the old collection
			Collections.sort(top3List, new Comparator<Tuple>() {
				@Override
				public int compare(Tuple tuple1, Tuple tuple2) {
					int valTuple1 = tuple1.getInteger(3);
					int valTuple2 = tuple2.getInteger(3);

					if (valTuple1 > valTuple2) {
						return -1;
					} else if (valTuple1 == valTuple2) {
						return (tuple1.getString(2).compareTo(tuple2.getString(2)));
					} else {
						return 1;
					}
				}
			});

			// get top 3 hashtags
			String toSave = windowCounter+"";
			int top3Count = 0;
			for (int i = 0; i < top3List.size() && top3Count < 3; i++) {
				Tuple currentTuple = top3List.get(i);
				if (currentTuple != null) {
					toSave += "," + currentTuple.getString(2) + "," + currentTuple.getInteger(3);
				} else {
					toSave += ",null,0";
				}
				top3Count++;
			}
			while (top3Count < 3) {
				toSave += ",null,0";
				top3Count++;
			}
			windowCounter++;

			// save them to file
			System.out.println(toSave);
			saveToFile(toSave);

			top3List = new ArrayList<Tuple>(); 
		}
		else {
			windowId = newWindowId;
			top3List.add(input);
		}


	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

}

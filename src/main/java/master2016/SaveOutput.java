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
	private ArrayList<Tuple>[] top3List;
	private String folder;
	private String[] windowId;
	private int windowCounter;

	public SaveOutput(String folder)
	{
		super();
		this.top3List = new ArrayList[3];
		this.top3List[0] = new ArrayList<Tuple>();
		this.top3List[1] = new ArrayList<Tuple>();
		this.top3List[2] = new ArrayList<Tuple>();
		this.windowId = new String[3];
		this.windowId[0] = "";
		this.windowId[1] = "";
		this.windowId[2] = "";
		this.windowCounter = 1;
		this.folder = folder;
	}

	private void saveToFile(int list, String lineToSave)
	{
		File file = new File(folder + "/" + top3List[list].get(0).getString(1) + "_" + 13);
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
		int newNumHashtags = input.getInteger(4);
		
		System.out.println(newNumHashtags + " " + top3List[0].size());

		boolean windowExists = false;
		for (int i = 0; i < 3; i++) {
			if (!windowExists && newWindowId.equals(windowId[i])) {
				windowExists = true;
				top3List[i].add(input);
				// check if window is complete
				if (top3List[i].size() == newNumHashtags) {
					// order the old collection
					Collections.sort(top3List[i], new Comparator<Tuple>() {
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
					String toSave = windowCounter + "";
					int top3Count = 0;
					for (int j = 0; j < top3List[i].size() && top3Count < 3; j++) {
						Tuple currentTuple = top3List[i].get(j);
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
					saveToFile(i, toSave);

					top3List[i] = new ArrayList<Tuple>(); 
					windowId[i] = "";
				}				
			}
		}
		if (!windowExists) {
			// creates new window
			for (int i = 0; i < 3; i++) {
				if (windowId[i].equals("")) {
					windowId[i] = newWindowId;
					top3List[i].add(input);
					break;
				}
			}
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

}

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
	private int[] languagesCount;
	private String[] languages;

	@SuppressWarnings("unchecked")
	public SaveOutput(String[] languages, String folder)
	{
		super();
		this.languages = languages;
		languagesCount = new int[languages.length];
		for (int i = 0; i < languagesCount.length; i++) {
			languagesCount[i] = 1;
		}
		this.top3List = new ArrayList[3];
		this.top3List[0] = new ArrayList<Tuple>();
		this.top3List[1] = new ArrayList<Tuple>();
		this.top3List[2] = new ArrayList<Tuple>();
		this.folder = folder;
	}

	private void saveToFile(String lineToSave, String lang)
	{
		File file = new File(folder + "/" + lang + "_" + 13);
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

		if(newNumHashtags == 0){
			String currentLanguage = input.getString(1);
			String toSave = "";
			for (int j = 0; j < languages.length; j++) {
				if (languages[j].equals(currentLanguage)) {
					toSave += languagesCount[j];
					languagesCount[j]++;
					break;
				}
			}
			toSave += "," + currentLanguage + ",null,0,null,0,null,0";

			// save to file
			System.out.println(toSave);
			saveToFile(toSave, currentLanguage);
			return;
		}

		if(newNumHashtags == 1){
			String currentLanguage = input.getString(1);
			String toSave = "";
			for (int j = 0; j < languages.length; j++) {
				if (languages[j].equals(currentLanguage)) {
					toSave += languagesCount[j];
					languagesCount[j]++;
					break;
				}
			}
			toSave += "," + currentLanguage + "," + input.getString(2) + "," + input.getInteger(3) + 
					",null,0,null,0";

			// save to file
			System.out.println(toSave);
			saveToFile(toSave, currentLanguage);
			return;
		}

		boolean windowExists = false;		

		for (int i = 0; i < 3; i++) {
			if (!windowExists && !top3List[i].isEmpty() && top3List[i].get(0).getString(0).equals(newWindowId)) {
				windowExists = true;
				top3List[i].add(input);
				// check if window is complete
				if (top3List[i].size() == newNumHashtags) {
					// order the collection
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
					String currentLanguage = input.getString(1);
					String toSave = "";
					for (int j = 0; j < languages.length; j++) {
						if (languages[j].equals(currentLanguage)) {
							toSave += languagesCount[j];
							languagesCount[j]++;
							break;
						}
					}
					toSave += "," + currentLanguage;
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

					// save them to file
					System.out.println(toSave);
					saveToFile(toSave, currentLanguage);
					top3List[i] = new ArrayList<Tuple>();
				}				
			}
		}
		if (!windowExists) {
			// creates new window
			for (int i = 0; i < 3; i++) {
				if (top3List[i].isEmpty()) {
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

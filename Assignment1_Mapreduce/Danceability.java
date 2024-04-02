package gr.aueb.panagiotisl.mapreduce.danceability;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Danceability {
    public static class CountMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text final_key = new Text();                      //initialize key
        private Text final_value = new Text();                    //initialize value

        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            
            String[] columns = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            
            long lineNumber = key.get();

            
            if (lineNumber == 0) {
                return;                         //skip processing for the header
            }

            if (columns.length >= 14 && !columns[6].isEmpty()){
                String country = columns[6].trim();                    // Extract the countries
                String date = columns[7].trim().substring(1,Math.min(8, columns[7].trim().length()));        //Extract only month and year (from the format YYYY-MM-DD keep only the first 7 characters)
            
                String songname = columns[1].trim();
                String danceability = columns[13].trim();
                

                final_key.set(country + " :  " + date);                      //the key will be a combo of country and date
                final_value.set(songname + " : " + danceability);           // and the value will be the name of the song and the danceability metric value

                
                context.write(final_key, final_value);                 // Extract key-value pair

            }
            
           
        }
    }

    public static class CountReducer extends Reducer<Text, Text, Text, Text> {

        private Text new_value = new Text();                    // Initialize value
       
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           
            double max_danceability = -1.0;                 //Initialize a negative number since danceability can't take negative values anyway
            double total_danceability = 0;
            String max_danceability_song= "";               //Initialize the song with the max danceability
            int count = 0;
            double avg_danceability = -1;                 //Initialize average danceability
            double danceability;
            for (Text value:values){

                String[] parts = value.toString().split(":(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");  //split using : as a delimiter and use a regex to avoid : inside double quotes
               
                String song_name = parts[0].trim();                     //Extract the song name
                String danceability_str = parts[1].trim();              // Extract the danceability as a string
                
                danceability = Double.parseDouble(danceability_str.replaceAll("\"", ""));  //Convert the string to double, after having removed double quotes
                
                if (danceability > max_danceability){
                    max_danceability = danceability;                   //Update the max danceability
                    max_danceability_song = song_name;
                }
                total_danceability += danceability;
                count++;
            
                
                if (count > 0){
                    avg_danceability = total_danceability / count;           //Update avg danceability
                }else{
                    avg_danceability = 0;                                   
                }
             
            }
            new_value.set(max_danceability_song + " : " + max_danceability +  ", avg: " + avg_danceability);  
            context.write(key, new_value);                               //Extract the new key-value pair
        }
    }
}
                
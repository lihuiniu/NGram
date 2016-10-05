import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.mapred.lib.db.DBWritable;

public class DBOutputWritable implements DBWritable{

	private String starting_phrase;
	private String following_word;
	private int count;
	public DBOutputWritable() {
		// TODO Auto-generated constructor stub
	}

	public DBOutputWritable(String starting_phrase, String following_word, int count) {
		// TODO Auto-generated constructor stub
		this.starting_phrase = starting_phrase;
		this.following_word = following_word;
		this.count = count;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public void readFields(ResultSet arg0) throws SQLException {
		// TODO Auto-generated method stub
		//MySQL column index start from 1 not 0.
		this.starting_phrase = arg0.getString(1);
		this.following_word = arg0.getString(2);
		this.count = arg0.getInt(3);
	}

	@Override
	public void write(PreparedStatement arg0) throws SQLException {
		// TODO Auto-generated method stub
		arg0.setString(1, starting_phrase);
		arg0.setString(2, following_word);
		arg0.setInt(3, count);
	}

}

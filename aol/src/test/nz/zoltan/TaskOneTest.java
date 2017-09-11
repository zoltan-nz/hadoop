package nz.zoltan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TaskOneTest {

	@Mock
	private Mapper.Context mapperContext;
	@Mock
	private Reducer.Context reducerContext;
	@Mock
	private LongWritable key;
	@Mock
	private Text searchLogLine;
	@Mock
	private Configuration config;

	@InjectMocks
	private TaskOne.TaskOneMapper mapper;

	@InjectMocks
	private TaskOne.TaskOneReducer reducer;

	private ArgumentCaptor<Text> keyCaptor = ArgumentCaptor.forClass(Text.class);
	private ArgumentCaptor<Text> valueCaptor = ArgumentCaptor.forClass(Text.class);

	@Test
	public void testMapper() throws IOException, InterruptedException {

		// Arrange
		doReturn(config).when(mapperContext).getConfiguration();
		doReturn(7).when(config).getInt("anonId", 0);

		key = new LongWritable(0L);
		searchLogLine = new Text("7\tsearch string\t2000-01-01\t5\twww.some.url");

		// Act
		mapper.map(key, searchLogLine, mapperContext);
		mapper.run(mapperContext);

		// Assert
		verify(mapperContext).write(keyCaptor.capture(), valueCaptor.capture());
		assertEquals(new Text("7"), keyCaptor.getValue());
		assertEquals(new Text("7, search string, 5, www.some.url"), valueCaptor.getValue());
	}

	@Test
	public void testReducer() throws IOException, InterruptedException {

		// Arrange
		List<Text> searchresults = asList(
				new Text("7, first search string, 5, www.some.url"),
				new Text("7, second search string, 6, www.some-other.url"));

		// Act
		reducer.reduce(new Text("7"), searchresults, reducerContext);

		// Assert
		verify(reducerContext).write(keyCaptor.capture(), valueCaptor.capture());
		assertEquals(new Text("ANON_ID, QUERY, ITEM_RANK, CLICK_URL\n"), keyCaptor.getValue());
		assertEquals(new Text("7, first search string, 5, www.some.url\n7, " +
				"second search string, 6, www.some-other.url\n"), valueCaptor.getValue());
	}
}
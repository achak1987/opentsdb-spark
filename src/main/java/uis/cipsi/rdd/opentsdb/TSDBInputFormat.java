package uis.cipsi.rdd.opentsdb;

import java.nio.charset.Charset;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

/**
 * Convert HBase tabular data into a format that is consumable by Map/Reduce.
 */
public class TSDBInputFormat extends TableInputFormat implements Configurable {

	private final Log LOG = LogFactory.getLog(TSDBInputFormat.class);

	/**
	 * The starting timestamp used to filter columns with a specific range of
	 * versions.
	 */
	public static final String SCAN_TIMERANGE_START = "hbase.mapreduce.scan.timerange.start";
	/**
	 * The ending timestamp used to filter columns with a specific range of
	 * versions.
	 */
	public static final String SCAN_TIMERANGE_END = "hbase.mapreduce.scan.timerange.end";

	public static final String TSDB_UIDS = "net.opentsdb.tsdb.uid";
	/** The opentsdb metric to be retrived. */
	public static final String METRICS = "net.opentsdb.rowkey";
	/** The opentsdb metric to be retrived. */
	public static final String TAGKV = "net.opentsdb.tagkv";
	/** The tag keys for the associated metric (space seperated). */
	public static final String TSDB_STARTKEY = "net.opentsdb.start";
	/** The tag values for the tag keys (space seperated). */
	public static final String TSDB_ENDKEY = "net.opentsdb.end";

	/** The configuration. */
	private Configuration conf = null;

	/**
	 * Returns the current configuration.
	 * 
	 * @return The current configuration.
	 * @see org.apache.hadoop.conf.Configurable#getConf()
	 */
	@Override
	public Configuration getConf() {
		return conf;
	}

	public static byte[] hexStringToByteArray(String s) {
		s = s.replace("\\x", "");
		byte[] b = new byte[s.length() / 2];
		for (int i = 0; i < b.length; i++) {
			int index = i * 2;
			int v = Integer.parseInt(s.substring(index, index + 2), 16);
			b[i] = (byte) v;
		}
		return b;
	}

	/**
	 * Specific configuration for the OPENTSDB tables {tsdb, tsdb-uid}
	 * 
	 * @param configuration
	 *            The configuration to set.
	 * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
	 */
	@Override
	public void setConf(Configuration configuration) {
		this.conf = configuration;
		String tableName = conf.get(INPUT_TABLE);
		try {
			setHTable(new HTable(new Configuration(conf), tableName));
		} catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
		}

		Scan scan = null;
		try {
			scan = new Scan();
			// Configuration for extracting the UIDs for the user specified
			// metric and tag names.
			if (conf.get(TSDB_UIDS) != null) {
				// We get all uids for all specified column quantifiers
				// (metrics|tagk|tagv)
				String name = String.format("^%s$", conf.get(TSDB_UIDS));
				RegexStringComparator keyRegEx = new RegexStringComparator(name);
				RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, keyRegEx);
				scan.setFilter(rowFilter);
			} else {
				// Configuration for extracting & filtering the required rows
				// from tsdb table.
				if (conf.get(METRICS) != null) {
					String name = null;
					if (conf.get(TAGKV) != null) // If we have to extract based
													// on a metric and its group
													// of tags "^%s.{4}.*%s.*$"
						name = String.format("^%s.*%s.*$", conf.get(METRICS), conf.get(TAGKV));
					else
						// If we have to extract based on just the metric
						name = String.format("^%s.+$", conf.get(METRICS));
					
					RegexStringComparator keyRegEx = new RegexStringComparator(
							name, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
					keyRegEx.setCharset(Charset.forName("ISO-8859-1"));
					RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, keyRegEx);
					scan.setFilter(rowFilter);
					//scan.setFilter(rowFilter);
				}
				// Extracts data based on the supplied timerange. If timerange
				// is not provided then all data are extracted
				if (conf.get(SCAN_TIMERANGE_START) != null
						&& conf.get(SCAN_TIMERANGE_END) != null) {

					scan.setStartRow(hexStringToByteArray(conf.get(METRICS)
							+ conf.get(SCAN_TIMERANGE_START) + conf.get(TAGKV)));
					scan.setStopRow(hexStringToByteArray(conf.get(METRICS)
							+ conf.get(SCAN_TIMERANGE_END) + conf.get(TAGKV)));

				}
			}
			// false by default, full table scans generate too much BC churn
			scan.setCacheBlocks((conf.getBoolean(SCAN_CACHEBLOCKS, false)));
		} catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
		}
		setScan(scan);
	}
}
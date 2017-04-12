package in.sheki.jedis.benchmark;

import java.util.concurrent.LinkedBlockingQueue;

public class GlobalCache {
	
	public static final LinkedBlockingQueue<BenchmarkData> cacheQueue = new LinkedBlockingQueue<BenchmarkData>();

}

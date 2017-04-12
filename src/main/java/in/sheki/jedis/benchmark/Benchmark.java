package in.sheki.jedis.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.pool.impl.GenericObjectPool;

import com.beust.jcommander.JCommander;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @author abhishekk
 */
public class Benchmark
{

    private final int noOps_;
    private final LinkedBlockingQueue<Long> setRunTimes = new LinkedBlockingQueue<Long>();
    private PausableThreadPoolExecutor executor;
    private final JedisPool pool;
    private final String data;
    private CountDownLatch shutDownLatch;
    private long totalNanoRunTime;
    private int noJedisConn;
    private String type;
    private int fieldCount = 50;


    public Benchmark(final int noOps, final int noThreads, final int noJedisConn, final String host, final int port, int dataSize)
    {
        this.noOps_ = noOps;
        this.executor = new PausableThreadPoolExecutor(noThreads, noThreads, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        final GenericObjectPool.Config poolConfig = new GenericObjectPool.Config();
        poolConfig.testOnBorrow = true;
        poolConfig.testOnReturn = true;
        poolConfig.maxActive = noJedisConn;
        this.pool = new JedisPool(poolConfig, host, port);
        this.data = RandomStringUtils.random(dataSize);
        this.noJedisConn = noJedisConn;
        shutDownLatch = new CountDownLatch(noOps);
    }

    class HSetTask implements Runnable
    {
        private CountDownLatch latch_;

        HSetTask(CountDownLatch latch)
        {
            this.latch_ = latch;
        }

		public void run() {
			Jedis jedis = pool.getResource();
			try {
				String key = RandomStringUtils.random(15);
				String field;
				BenchmarkData benchMarkData = new BenchmarkData(key);
				for (int i = 0; i < fieldCount; i++) {
					field = RandomStringUtils.random(15);
					long startTime = System.nanoTime();
					jedis.hset(key, field, data);
					setRunTimes.offer(System.nanoTime() - startTime);
					benchMarkData.addField(field);
				}
				// save to memory
				GlobalCache.cacheQueue.offer(benchMarkData);
			} catch (Exception e) {
				System.out.println(e);
			} finally {
				pool.returnResource(jedis);
				latch_.countDown();
			}
		}
	}

    class HGetTask implements Runnable
    {
        private CountDownLatch latch_;
        private BenchmarkData benchmarkData;

        HGetTask(CountDownLatch latch, BenchmarkData benchmarkData)
        {
            this.latch_ = latch;
            this.benchmarkData= benchmarkData;
        }

		public void run() {
			Jedis jedis = pool.getResource();
			try{
				String hashKey = benchmarkData.getKey();
				List<String> fieldList = benchmarkData.getFiledList();
				for (String field : fieldList) {
					long startTime = System.nanoTime();
					jedis.hget(hashKey, field);
					setRunTimes.offer(System.nanoTime() - startTime);
				}
			} catch (Exception e) {
				System.out.println(e);
			} finally {
				pool.returnResource(jedis);
				latch_.countDown();
			}
		}
    }

    class HGetAllTask implements Runnable
    {
        private CountDownLatch latch_;
        private BenchmarkData benchmarkData;

        HGetAllTask(CountDownLatch latch, BenchmarkData benchmarkData)
        {
            this.latch_ = latch;
            this.benchmarkData= benchmarkData;
        }

		public void run() {
			Jedis jedis = pool.getResource();
			try{
				String hashKey = benchmarkData.getKey();
				long startTime = System.nanoTime();
				jedis.hgetAll(hashKey);
				setRunTimes.offer(System.nanoTime() - startTime);
			} catch (Exception e) {
				System.out.println(e);
			} finally {
				pool.returnResource(jedis);
				latch_.countDown();
			}
		}
    }

    public void performBenchmark(String type) throws InterruptedException
    {
    	this.type = type;
        executor.pause();
		if (type.equals("hset")) {
			for (int i = 0; i < noOps_; i++) {
				executor.submit(new HSetTask(shutDownLatch));
			}
		} else if (type.equals("hget")) {
			for (BenchmarkData benchmarkData : GlobalCache.cacheQueue) {
				executor.submit(new HGetTask(shutDownLatch, benchmarkData));
			}
		} else if (type.equals("hgetall")){
			for (BenchmarkData benchmarkData : GlobalCache.cacheQueue) {
				executor.submit(new HGetAllTask(shutDownLatch, benchmarkData));
			}
		} else {
    		System.out.println("not support type, -w = hset or hget");
    	}
        
        long startTime = System.currentTimeMillis();
        executor.resume();
        executor.shutdown();
        shutDownLatch.await();
        totalNanoRunTime = System.currentTimeMillis() - startTime;
    }

    public void printStats()
    {
        List<Long> points = new ArrayList<Long>();
        setRunTimes.drainTo(points);
        Collections.sort(points);
        int i = 0, curlat = 0;
        float perc, reqpersec;
        int pointsSize = points.size();
        reqpersec = (float)pointsSize/((float)totalNanoRunTime/1000);

        System.out.printf("======%s======\n", this.type);
        System.out.printf(" %d requests completed in %.2f seconds\n", pointsSize, (float)totalNanoRunTime/1000);
        System.out.printf(" %d parallel clients\n", this.noJedisConn);
        System.out.printf(" %d bytes payload\n", this.data.getBytes().length);
        System.out.println(" executor size : " + executor.getMaximumPoolSize());
        
        for (Long l : points)
        {
        	if(l/1000000 != curlat || i == pointsSize - 1) {
        		curlat = (int) (l/1000000);
        		perc = ((float)(i+1)*100)/pointsSize;
        		System.out.printf("%.5f%% <= %d milliseconds\n", perc, curlat);
        	}
            i++;
        }
        System.out.printf("%.2f requests per second\n\n", reqpersec);
    }

    public static void main(String[] args) throws InterruptedException
    {
        CommandLineArgs cla = new CommandLineArgs();
        new JCommander(cla, args);
        if (cla.type.equals("hset")) {
        	Benchmark benchmark = new Benchmark(cla.noOps, cla.noThreads, cla.noConnections, cla.host, cla.port, cla.dataSize);
			benchmark.performBenchmark("hset");
			benchmark.printStats();
        } else if (cla.type.equals("hget")) {
        	Benchmark hset= new Benchmark(cla.noOps, cla.noThreads, cla.noConnections, cla.host, cla.port, cla.dataSize);
			hset.performBenchmark("hset");
			hset.printStats();
			
			if(GlobalCache.cacheQueue.size() != 0) {
				Benchmark hget= new Benchmark(GlobalCache.cacheQueue.size(), cla.noThreads, cla.noConnections, cla.host, cla.port, cla.dataSize);
				hget.performBenchmark("hget");
				hget.printStats();
			}

        } else if (cla.type.equals("hgetall")) {
        	Benchmark hset= new Benchmark(cla.noOps, cla.noThreads, cla.noConnections, cla.host, cla.port, cla.dataSize);
			hset.performBenchmark("hset");
			hset.printStats();
			
			if(GlobalCache.cacheQueue.size() != 0) {
				Benchmark hgetall= new Benchmark(GlobalCache.cacheQueue.size(), cla.noThreads, cla.noConnections, cla.host, cla.port, cla.dataSize);
				hgetall.performBenchmark("hgetall");
				hgetall.printStats();
			}
        } else {
        	System.out.println("only support 'hset' 'hget' 'hgetall'");
        }
    }
}

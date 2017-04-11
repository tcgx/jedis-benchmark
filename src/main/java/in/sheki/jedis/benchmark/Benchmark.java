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
    private LinkedBlockingQueue<Long> setRunTimes = new LinkedBlockingQueue<Long>();
    private PausableThreadPoolExecutor executor;
    private final JedisPool pool;
    private final String data;
    private CountDownLatch shutDownLatch;
    private long totalNanoRunTime;
    private int noJedisConn;
    private String type;
    private List<Keys> keys = new ArrayList<Keys>();


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
			String key = RandomStringUtils.random(15);
			String field;
			for (int i = 0; i < 50; i++) {
				Jedis jedis = pool.getResource();
				field = RandomStringUtils.random(15);
				long startTime = System.nanoTime();
				jedis.hset(key, field, data);
				setRunTimes.offer(System.nanoTime() - startTime);
				pool.returnResource(jedis);
				keys.add(new Keys(key, field));
			}
			latch_.countDown();
		}
    }

    class HGetTask implements Runnable
    {
        private CountDownLatch latch_;
        private Keys key_;

        HGetTask(CountDownLatch latch, Keys key)
        {
            this.latch_ = latch;
            this.key_ = key;
        }

		public void run() {
			String mkey = key_.getK();
			String mfield = key_.getV();
			Jedis jedis = pool.getResource();
			long startTime = System.nanoTime();
			jedis.hget(mkey, mfield);
			setRunTimes.offer(System.nanoTime() - startTime);
			pool.returnResource(jedis);
			latch_.countDown();
		}
    }

    class Keys {
    	private String k;
    	private String v;
    	
    	Keys(String k, String v) {
    		this.k = k;
    		this.v = v;
    	}
    	
    	public String getK() {return k;}
    	public String getV() {return v;}
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
			shutDownLatch = new CountDownLatch(keys.size());
			for (Keys key : keys) {
				executor.submit(new HGetTask(shutDownLatch, key));
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
        long sum = 0;
        int i = 0, curlat = 0;
        float perc, reqpersec;
        int pointsSize = points.size();
        reqpersec = (float)pointsSize/((float)totalNanoRunTime/1000);

        System.out.printf("======%s======", this.type);
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
            sum += l;
            i++;
        }
        System.out.printf("%.2f requests per second\n\n", reqpersec);
        setRunTimes = new LinkedBlockingQueue<Long>();
    }

    public static void main(String[] args) throws InterruptedException
    {
        CommandLineArgs cla = new CommandLineArgs();
        new JCommander(cla, args);
        Benchmark benchmark = new Benchmark(cla.noOps, cla.noThreads, cla.noConnections, cla.host, cla.port, cla.dataSize);
        benchmark.performBenchmark("hset");
        benchmark.printStats();
        benchmark.performBenchmark("hget");
        benchmark.printStats();
    }


}

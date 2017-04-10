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
    private final CountDownLatch shutDownLatch;
    private long totalNanoRunTime;


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
			for (int i = 0; i < 50; i++) {
				long startTime = System.nanoTime();
				Jedis jedis = pool.getResource();
				jedis.hset(key, RandomStringUtils.random(15), data);
				setRunTimes.offer(System.nanoTime() - startTime);
				pool.returnResource(jedis);
			}
			latch_.countDown();
		}
    }

    class HGetTask implements Runnable
    {
        private CountDownLatch latch_;

        HGetTask(CountDownLatch latch)
        {
            this.latch_ = latch;
        }

		public void run() {
			String key = RandomStringUtils.random(15);
			for (int i = 0; i < 50; i++) {
				long startTime = System.nanoTime();
				Jedis jedis = pool.getResource();
				jedis.hget(key, RandomStringUtils.random(15));
				setRunTimes.offer(System.nanoTime() - startTime);
				pool.returnResource(jedis);
			}
			latch_.countDown();
		}
    }

    public void performBenchmark() throws InterruptedException
    {
        executor.pause();
        for (int i = 0; i < noOps_; i++)
        {
            executor.submit(new HGetTask(shutDownLatch));
        }
        long startTime = System.nanoTime();
        executor.resume();
        executor.shutdown();
        shutDownLatch.await();
        totalNanoRunTime = System.nanoTime() - startTime;
    }

    public void printStats()
    {
        List<Long> points = new ArrayList<Long>();
        setRunTimes.drainTo(points);
        Collections.sort(points);
        long sum = 0;
        for (Long l : points)
        {
            sum += l;
        }
        System.out.println("Data size :" + data.length());
        System.out.println("Total count :" + noOps_);
        System.out.println("Threads : " + executor.getMaximumPoolSize());
        System.out.println("Time Test Ran for (ms) : " + TimeUnit.NANOSECONDS.toMillis(totalNanoRunTime));
        System.out.println("Average : " + TimeUnit.NANOSECONDS.toMicros(sum / points.size()) + " us");
        System.out.println("50 % <= " + TimeUnit.NANOSECONDS.toMicros(points.get((points.size() / 2) - 1)) + " us");
        System.out.println("90 % <= " + TimeUnit.NANOSECONDS.toMicros(points.get((points.size() * 90 / 100) - 1)) + " us");
        System.out.println("95 % <= " + TimeUnit.NANOSECONDS.toMicros(points.get((points.size() * 95 / 100) - 1)) + " us");
        System.out.println("99 % <= " + TimeUnit.NANOSECONDS.toMicros(points.get((points.size() * 99 / 100) - 1)) + " us");
        System.out.println("99.9 % <= " + TimeUnit.NANOSECONDS.toMicros(points.get((points.size() * 999 / 1000) - 1)) + " us");
        System.out.println("100 % <= " + TimeUnit.NANOSECONDS.toMicros(points.get(points.size() - 1)) + " us");
        System.out.println((noOps_ * 1000 * 50 / TimeUnit.NANOSECONDS.toMillis(totalNanoRunTime)) + " Operations per second");
    }

    public static void main(String[] args) throws InterruptedException
    {
        CommandLineArgs cla = new CommandLineArgs();
        new JCommander(cla, args);
        Benchmark benchmark = new Benchmark(cla.noOps, cla.noThreads, cla.noConnections, cla.host, cla.port, cla.dataSize);
        benchmark.performBenchmark();
        benchmark.printStats();
    }


}

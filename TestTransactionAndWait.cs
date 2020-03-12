using System;
using StackExchange.Redis;

public class TestTransactionAndWait
{
	private static Lazy<ConnectionMultiplexer> TxnlazyConnection = new Lazy<ConnectionMultiplexer>(() => {
		return ConnectionMultiplexer.Connect("redis-14000.cluster4.virag.demo-rlec.redislabs.com:14000, abortConnect=false, connectRetry=5, connectTimeout=5000, ssl=false, password=''");
		});
	
	public static ConnectionMultiplexer Conn_Txn {
		get {
			return TxnlazyConnection.Value;
			}
		}
	private static Lazy<ConnectionMultiplexer> nonTxnlazyConnection = new Lazy<ConnectionMultiplexer>(() => {
		return ConnectionMultiplexer.Connect("redis-14000.cluster4.virag.demo-rlec.redislabs.com:14000, abortConnect=false, connectRetry=5, connectTimeout=5000, ssl=false, password=''");
		});
	
	public static ConnectionMultiplexer Conn_NonTxn {
		get {
			return nonTxnlazyConnection.Value;
			}
		}	

	public static void Main()
	{
		var watch = System.Diagnostics.Stopwatch.StartNew();

		IDatabase txn_redis = TestTransactionAndWait.Conn_Txn.GetDatabase();
		IDatabase non_txn_redis = TestTransactionAndWait.Conn_NonTxn.GetDatabase();
		watch.Stop();
		Console.WriteLine($"Connecting to RE DB, Execution Time: {watch.ElapsedMilliseconds} ms");		
		var hashKey = "userHashKey:{1234}";

        HashEntry[] redisPersonHash1 =
			{
                //new HashEntry("first_name", "Virag"),
                new HashEntry("last_name", "Tripathi"),
                new HashEntry("age", 42)
            };
        HashEntry[] redisPersonHash2 =
			{
                new HashEntry("first_name", "Virag"),
                new HashEntry("last_name", "Tripathi"),
                new HashEntry("age", 38)
            };			
		if (!watch.IsRunning)
		watch.Restart(); // Reset time to 0 and start measuring
		// do all transactional writes on dedicated redis connection i.e. txn_redis
		var trans1 = txn_redis.CreateTransaction();
		txn_redis.HashSet(hashKey, redisPersonHash1);
		var exec = trans1.ExecuteAsync();
		//var result = txn_redis.Wait(exec);
		//watch.Stop();
		txn_redis.ExecuteAsync("WAIT 1 50").ToString();
		watch.Stop();
		Console.WriteLine($"Response from 1st transaction in RE DB, Execution Time: {watch.ElapsedMilliseconds} ms");
		
		// do all non transactional writes and reads on another redis connection i.e. non_txn_redis

        // Simple PING command
        string cacheCommand = "PING";
        Console.WriteLine("\nCache command  : " + cacheCommand);
        Console.WriteLine("Cache response : " + non_txn_redis.Execute(cacheCommand).ToString());

        // Simple get and put of integral data types into the cache
        cacheCommand = "GET Message";
        Console.WriteLine("\nCache command  : " + cacheCommand + " or StringGet()");
        Console.WriteLine("Cache response : " + non_txn_redis.StringGet("Message").ToString());

        cacheCommand = "SET Message \"Hello! The cache is working from a .NET console app!\"";
        Console.WriteLine("\nCache command  : " + cacheCommand + " or StringSet()");
        Console.WriteLine("Cache response : " + non_txn_redis.StringSet("Message", "Hello! The cache is working from a .NET console app!").ToString());

        // Demonstrate "SET Message" executed as expected...
        cacheCommand = "GET Message";
        Console.WriteLine("\nCache command  : " + cacheCommand + " or StringGet()");
        Console.WriteLine("Cache response : " + non_txn_redis.StringGet("Message").ToString());

        // Get the client list, useful to see if connection list is growing...
        cacheCommand = "CLIENT LIST";
        Console.WriteLine("\nCache command  : " + cacheCommand);
        Console.WriteLine("Cache response : \n" + non_txn_redis.Execute("CLIENT", "LIST").ToString().Replace("id=", "id="));

		var trans2 = txn_redis.CreateTransaction();
		trans2.AddCondition(Condition.HashNotExists(hashKey, "first_name"));
		trans2.HashSetAsync(hashKey, redisPersonHash2, CommandFlags.FireAndForget);
		bool committed = trans2.Execute();
		// ^^^ if true: it was applied; if false: it was rolled back
		// add additional logic for cleanup if necessary
	
		var allHash = non_txn_redis.HashGetAll(hashKey);

		//get all the items
        foreach (var item in allHash)
		{
			Console.WriteLine(string.Format("key : {0}, value : {1}", item.Name, item.Value));
		}

		// destroy the connections 
		TxnlazyConnection.Value.Dispose();
		nonTxnlazyConnection.Value.Dispose();
	}
}
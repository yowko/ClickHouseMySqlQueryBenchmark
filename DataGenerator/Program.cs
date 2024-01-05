using System.Data;
using System.Globalization;
using Bogus;
using ClickHouse.Ado;
using ClickHouse.Client.Copy;
using CsvHelper;
using CsvHelper.Configuration.Attributes;
using MySqlConnector;

await GenTestRecords(999, 100000);

Order[] GetOrders(int BatchSize)
{
    var startDate = new DateTime(2021, 01, 01, 00, 00, 00, DateTimeKind.Utc);
    var order = new Faker<Order>()
        .RuleFor(a => a.Id, f => f.Random.ULong())
        .RuleFor(a => a.OrderDate, f => startDate.AddDays(f.Random.Number(0, 365 * 3)))
        .RuleFor(a => a.ProductId, f => f.Random.Number(1, 10000))
        .RuleFor(a => a.OrderType, f => f.Random.SByte(1, 10))
        .RuleFor(a => a.Amount, f => f.Random.Decimal(0M, 100000M));
    return order.Generate(BatchSize).ToArray();
}

async Task GenTestRecords(int batchcount = 1, int batchsize = 10)
{
    for (var i = 0; i < batchcount; i++)
    {
        var orders = GetOrders(batchsize);
        
        //await StoreToClickHouseTcp(orders);

        await StoreToClickHouse(orders);

        await StoreToMysql(orders);
    }
}

async Task StoreToMysql(Order[] orders)
{
    using var memoryStream = new MemoryStream();
    using var streamWriter = new StreamWriter(memoryStream);
    using var csvWriter = new CsvWriter(streamWriter, CultureInfo.InvariantCulture);
    csvWriter.WriteRecords(orders);
    await streamWriter.FlushAsync();
    memoryStream.Position = 0;
    var connectionString =
        "Server=localhost;Port=3306;Database=test;Username=root;Password=pass.123;Allow User Variables=true;AllowLoadLocalInfile=true;";//
    using var connection = new MySqlConnection(connectionString);
    {
        connection.Open();

        var bl = new MySqlBulkLoader(connection)
        {
            TableName = "orders",
            SourceStream = memoryStream,
            FieldTerminator = ",",
            LineTerminator = "\n",
            NumberOfLinesToSkip = 1,
            FieldQuotationOptional = true,
            Columns = { "order_date", "product_id", "order_type", "amount" }
        };

        var inserted = bl.Load();
        Console.WriteLine(inserted + " rows inserted.");
    }
}

async Task StoreToClickHouse(Order[] orders)
{
    const string connectionString =
        "Compression=True;Timeout=10000;Host=localhost;Port=8123;Database=test;Username=yowko;Password=pass.123";

    using var connection = new ClickHouse.Client.ADO.ClickHouseConnection(connectionString);

    using var bulkCopy = new ClickHouseBulkCopy(connection)
    {
        DestinationTableName = "test.orders",
        ColumnNames = new[] {"id","order_date","product_id","order_type","amount"},
        BatchSize = orders.Length
    };
    await bulkCopy.InitAsync();
    var ordersObj = orders.Select(order => new object[]
        { order.Id,order.OrderDate,order.ProductId,order.OrderType,order.Amount});
        
    try
    {
        await bulkCopy.WriteToServerAsync(ordersObj);
    }
    catch (Exception e)
    {
        Console.WriteLine(e);
    }
    finally
    {
        connection.Close();
    }  
}

async Task StoreToClickHouseTcp(Order[] orders)
{
    const string connectionString =
        "Compress=True;BufferSize=32768;SocketTimeout=10000;CheckCompressedHash=False;Compressor=lz4;Host=localhost;Port=9000;Database=test;User=yowko;Password=pass.123";
    using var connection = new ClickHouseConnection(connectionString);

    connection.Open();

    var command = connection
        .CreateCommand("INSERT INTO orders (id,order_date,product_id,order_type,amount) VALUES @bulk")
        .AddParameter("bulk", DbType.Object,
            orders.Select(a => new object[] { a.Id, a.OrderDate, (UInt32)a.ProductId, a.OrderType, a.Amount }));
    try
    {
        await command.ExecuteNonQueryAsync();
    }
    catch (Exception e)
    {
        Console.WriteLine(e);
        throw;
    }
    finally
    {
        connection.Close();
    }
}
public class Order
{
    [Ignore]
    public ulong Id { get; set; }
    public DateTime OrderDate { get; set; }
    public int ProductId { get; set; }
    public sbyte OrderType { get; set; }
    public decimal Amount { get; set; }
}
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;
using System.Linq;

class Record
{
    public DateTime TpepPickupDatetime { get; set; }
    public DateTime TpepDropoffDatetime { get; set; }
    public int PassengerCount { get; set; }
    public double TripDistance { get; set; }
    public string StoreAndFwdFlag { get; set; }
    public int PULocationID { get; set; }
    public int DOLocationID { get; set; }
    public decimal FareAmount { get; set; }
    public decimal TipAmount { get; set; }
}

class Program
{
    static void Main(string[] args)
    {
        string connectionString = "Server=HP-LAPTOP;Database=DatabaseForTask;User ID=sa;Password=password;"; 
        string csvFilePath = "sample-cab-data.csv";
        string duplicatesFilePath = "duplicates.csv";

        try
        {
            var records = ReadCsvInBatches(csvFilePath, batchSize: 10000);
            var uniqueRecords = RemoveDuplicates(records, duplicatesFilePath);
            BulkInsertToSqlServer(connectionString, uniqueRecords);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
        }
        Console.ReadKey();
    }

    static IEnumerable<Record> ReadCsvInBatches(string filePath, int batchSize)
    {
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            TrimOptions = TrimOptions.Trim,
            MissingFieldFound = null,
            BadDataFound = null
        };

        using (var reader = new StreamReader(filePath))
        using (var csv = new CsvReader(reader, config))
        {
            csv.Read();
            csv.ReadHeader();

            var batch = new List<Record>(batchSize);

            while (csv.Read())
            {
                Record record = null;

                try
                {
                    record = new Record
                    {
                        TpepPickupDatetime = ParseDate(csv.GetField("tpep_pickup_datetime")),
                        TpepDropoffDatetime = ParseDate(csv.GetField("tpep_dropoff_datetime")),
                        PassengerCount = csv.GetField<int?>("passenger_count") ?? 0,
                        TripDistance = csv.GetField<double>("trip_distance"),
                        StoreAndFwdFlag = ParseStoreAndFwdFlag(csv.GetField("store_and_fwd_flag")),
                        PULocationID = csv.GetField<int>("PULocationID"),
                        DOLocationID = csv.GetField<int>("DOLocationID"),
                        FareAmount = csv.GetField<decimal>("fare_amount"),
                        TipAmount = csv.GetField<decimal>("tip_amount")
                    };
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Skipping record due to error: {ex.Message}");
                }

                if (record != null)
                {
                    batch.Add(record);

                    if (batch.Count >= batchSize)
                    {
                        foreach (var r in batch)
                            yield return r;
                        batch.Clear();
                    }
                }
            }

            if (batch.Count > 0)
            {
                foreach (var r in batch)
                    yield return r;
            }
        }
    }



    static IEnumerable<Record> RemoveDuplicates(IEnumerable<Record> records, string duplicatesFilePath)
    {
        var seenRecords = new HashSet<string>();
        var duplicates = new List<Record>();

        foreach (var record in records)
        {
            var key = $"{record.TpepPickupDatetime}|{record.TpepDropoffDatetime}|{record.PassengerCount}";

            if (!seenRecords.Add(key))
            {
                duplicates.Add(record);
            }
            else
            {
                yield return record;
            }
        }

        if (duplicates.Any())
        {
            using (var writer = new StreamWriter(duplicatesFilePath))
            using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
            {
                csv.WriteRecords(duplicates);
            }
        }
    }

    static DateTime ParseDate(string dateStr)
    {
        if (DateTime.TryParseExact(dateStr, "MM/dd/yyyy hh:mm:ss tt", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime date))
        {
            date = DateTime.SpecifyKind(date, DateTimeKind.Unspecified);
            var estTimeZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
            return TimeZoneInfo.ConvertTimeToUtc(date, estTimeZone);
        }
        else
        {
            throw new FormatException($"Invalid DateTime format: {dateStr}");
        }
    }

    static string ParseStoreAndFwdFlag(string flag)
    {
        flag = flag.Trim().ToUpper();
        if (flag == "Y")
            return "Yes";
        else if (flag == "N")
            return "No";
        else
            return flag;
    }

    static void BulkInsertToSqlServer(string connectionString, IEnumerable<Record> records)
    {
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = "ProcessedCabData";
                bulkCopy.ColumnMappings.Add("TpepPickupDatetime", "tpep_pickup_datetime");
                bulkCopy.ColumnMappings.Add("TpepDropoffDatetime", "tpep_dropoff_datetime");
                bulkCopy.ColumnMappings.Add("PassengerCount", "passenger_count");
                bulkCopy.ColumnMappings.Add("TripDistance", "trip_distance");
                bulkCopy.ColumnMappings.Add("StoreAndFwdFlag", "store_and_fwd_flag");
                bulkCopy.ColumnMappings.Add("PULocationID", "PULocationID");
                bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationID");
                bulkCopy.ColumnMappings.Add("FareAmount", "fare_amount");
                bulkCopy.ColumnMappings.Add("TipAmount", "tip_amount");

                var table = new DataTable();
                table.Columns.Add("TpepPickupDatetime", typeof(DateTime));
                table.Columns.Add("TpepDropoffDatetime", typeof(DateTime));
                table.Columns.Add("PassengerCount", typeof(int));
                table.Columns.Add("TripDistance", typeof(double));
                table.Columns.Add("StoreAndFwdFlag", typeof(string));
                table.Columns.Add("PULocationID", typeof(int));
                table.Columns.Add("DOLocationID", typeof(int));
                table.Columns.Add("FareAmount", typeof(decimal));
                table.Columns.Add("TipAmount", typeof(decimal));

                foreach (var record in records)
                {
                    table.Rows.Add(record.TpepPickupDatetime, record.TpepDropoffDatetime, record.PassengerCount,
                                   record.TripDistance, record.StoreAndFwdFlag, record.PULocationID, record.DOLocationID,
                                   record.FareAmount, record.TipAmount);
                }

                bulkCopy.WriteToServer(table);
            }
        }
    }
}



using Common.Domain.PCL.ChartFrames;
using Common.Domain.PCL.TrendBars;
using cTrader.Automate.Small.V1.Backtesting.DataSource;
using cTrader.Automate.Small.V1.Backtesting.DataSource.Spread;
using cTrader.Automate.Small.V1.ChartFrame;
using System.IO;
using System;
using System.Linq;

using server = cTrader.Automate.Small.V1.Backtesting.DataSource.Server;
using System.Collections.Generic;
using Parquet.Data;
using Parquet;
using System.Threading.Tasks;

namespace cTrader.OpenData.ParquetConversion
{
    class Program
    {
        static void Main(string[] args)
        {
            var _account = "";
            var _start = new DateTime(2013, 7, 22);
            var _end = new DateTime(2020, 4,30);

            
            
            var _interval = "Ticks";
            var _startime = DateTime.Now;
            
            var _symbols = GetAvailableSymbol(_account);

            var date_ranges = Enumerable.Range(0,(_end - _start).Days)
                .Select(s => _start.AddDays(s))
                .GroupBy(g => g.ToString("yyyyMM"))
                .ToDictionary(k => k.Key, v => v.ToList());

            var exportTasks = new List<Task>();

            foreach (var itm in _symbols)
            {
                var symbol = itm.Key;
                var path = $"{itm.Value}\\{_interval}";

                //Handle Dates
                foreach(var _mon in date_ranges)
                {
                    var task = Task.Run(() =>
                    {
                        var month_group = _mon.Key;
                        var mon_start = _mon.Value.Min();
                        var mon_end = _mon.Value.Max();

                        //For Tick
                        ITickDataService datasource = new server.BacktestingTickDataService(mon_start, mon_end, path);
                        var tickData = datasource.GetTicks().ToList();

                        WriteToParquet(tickData, symbol, month_group);

                        GC.Collect();
                    });
                    exportTasks.Add(task);
                }
            }

            Task.WaitAll(exportTasks.ToArray());

            Console.WriteLine($"Competed at {DateTime.Now} timespent {(DateTime.Now - _startime).Minutes} mintues");
            Console.ReadLine();
        }

        /// <summary>
        /// Export Data to Parquet in 
        /// </summary>
        /// <param name="data"></param>
        /// <param name="symbol"></param>
        /// <param name="parition_key"></param>
        static void WriteToParquet(List<ITick> data,string symbol, string parition_key, string output_dir = "parquet", CompressionMethod compression_method = CompressionMethod.Gzip, int compression_level = 9)
        {
            //
            #region create data columns with schema metadata and the data you need
            var _asks = data.Select(s => s.Ask).ToArray();
            var askCol = new DataColumn(new DataField<double>("Ask"),_asks);

            var _bids = data.Select(s => s.Bid).ToArray();
            var bidCol = new DataColumn(new DataField<double>("Bid"), _asks);

            var _isRealAsks = data.Select(s => s.IsRealAsk).ToArray();
            var realAskCol = new DataColumn(new DataField<double>("IsRealAsk"), _asks);

            var _isRealBids = data.Select(s => s.IsRealBid).ToArray();
            var realBitCol = new DataColumn(new DataField<double>("IsRealBid"), _asks);

            var _timeUTC = data.Select(s => s.TimeUtc).ToArray();
            var timeCol = new DataColumn(new DataField<double>("TimeUtc"), _asks);
            #endregion

            // create file schema
            var schema = new Schema(
                timeCol.Field,
                askCol.Field,
                bidCol.Field,
                realAskCol.Field,
                realBitCol.Field
                );
            
            var outDir = $".\\{output_dir}\\{symbol}\\";

            if (!Directory.Exists(outDir))
                Directory.CreateDirectory(outDir);

            Console.WriteLine($"Writing {symbol} - {parition_key}");

            using (Stream fileStream = File.OpenWrite($"{outDir}\\{parition_key}.parquet"))
            {
               
                using (var parquetWriter = new ParquetWriter(schema, fileStream) { CompressionMethod = compression_method, CompressionLevel = compression_level })
                {
                    // create a new row group in the file
                    using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                    {
                        groupWriter.WriteColumn(timeCol);
                        groupWriter.WriteColumn(askCol);
                        groupWriter.WriteColumn(bidCol);
                        groupWriter.WriteColumn(realAskCol);
                        groupWriter.WriteColumn(realBitCol);
                    }
                }
            }
        }

        /// <summary>
        /// Determine all symbol
        /// </summary>
        /// <param name="_account"></param>
        /// <returns></returns>
        static Dictionary<String,String> GetAvailableSymbol(string _account)
        {
            var _cachePath = $"{Environment.GetEnvironmentVariable("USERPROFILE")}\\AppData\\Roaming\\pepperstone cTrader\\BacktestingCache\\{_account}";
            return Directory.GetDirectories(_cachePath).ToDictionary(k => k.Replace($"{_cachePath}\\",""), v => v);
        }
    }
}

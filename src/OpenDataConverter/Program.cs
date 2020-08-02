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
using Core.Framework.Extension.PCL.Extensions;

namespace cTrader.OpenData.ParquetConversion
{
    class Program
    {
        static void Main(string[] args)
        {
        
            var _interval = "Ticks";
            var _startime = DateTime.Now;
            
            var exportTasks = new List<Task>();

            var _symbols = GetAvailableSymbol();

            var selectedSymbol = FilterSymobl(_symbols);

            foreach (var itm in selectedSymbol)
            {
                var symbol = itm.Key;
                var path = $"{itm.Value}\\{_interval}";

                //Get Available Trade Date
                var available_dates = GetAvailableDate(path);

                var mon_ranges = available_dates
                    .GroupBy(g => g.ToString("yyyyMM"))
                    .ToDictionary(k => k.Key, v => v.ToList());


                //Handle Dates
                foreach (var _mon in mon_ranges)
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
            var bidCol = new DataColumn(new DataField<double>("Bid"), _bids);

            var _isRealAsks = data.Select(s => s.IsRealAsk).ToArray();
            var realAskCol = new DataColumn(new DataField<bool>("IsRealAsk"), _isRealAsks);

            var _isRealBids = data.Select(s => s.IsRealBid).ToArray();
            var realBitCol = new DataColumn(new DataField<bool>("IsRealBid"), _isRealBids);

            var _timeUTC = data.Select(s => new DateTimeOffset(s.TimeUtc)).ToArray();
            var timeCol = new DataColumn(new DataField<DateTimeOffset>("TimeUtc"), _timeUTC);
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
        static Dictionary<String,String> GetAvailableSymbol()
        {
            var _cachePath = $"{Environment.GetEnvironmentVariable("USERPROFILE")}\\AppData\\Roaming\\pepperstone cTrader\\BacktestingCache";
            var _accounts = Directory.GetDirectories(_cachePath).Select(s=> new FileInfo(s).Name);

            #region User Input
            Console.WriteLine($"We see following accounts. Press enter for first option: \n{String.Join("\n", _accounts) }");
            var _userOption = Console.ReadLine().Trim();
            _userOption = _userOption.IsNullOrEmpty() ? _accounts.First() : _userOption;
            #endregion

            var _accountCacheData = $"{Environment.GetEnvironmentVariable("USERPROFILE")}\\AppData\\Roaming\\pepperstone cTrader\\BacktestingCache\\{_userOption}";

            return Directory.GetDirectories(_accountCacheData).ToDictionary(k => k.Replace($"{_accountCacheData}\\",""), v => v);
        }
        static List<DateTime> GetAvailableDate(string path)
        {
           return Directory.GetFiles(path, "*.tdbc*").Select(s => {
               var file_info = new FileInfo(s);
               return DateTime.ParseExact(file_info.Name.Substring(0, 10), "yyyy.MM.dd", null);
               }).ToList();
        }

        static Dictionary<string, string> FilterSymobl(Dictionary<string, string> symbols)
        {
            #region User Input
            Console.WriteLine($"We see following Symobls.\nUse comma(,) to seperate options.\nPress enter for all options: \n{String.Join("\n", symbols.Keys) }\n");
            var _userOption = Console.ReadLine().Trim();
            #endregion
            if(_userOption.IsNullOrEmpty())
                return symbols;

            var selectedSymobl = _userOption.Split(',');
            return symbols.Where(w => selectedSymobl.Contains(w.Key)).ToDictionary();
        }
    }
}

# cTrader OpenData Conversion

## Why

Quants is hard to find an easy way to analysis data in R / Python. This package means to make Quants' life more easier.



## Getting started
Run ctrader

Open TaskManager to local your application, and copy the program into 

Download Historial Tick Record from server

Once the historial tick data downloaded into your computer, find the Backtest Cache directory.
```batch
%USERPROFILE%\AppData\Roaming\xxxxxxxx cTrader\BacktestingCache
```

Modify the <<account id>> to your account id, then compile and run the application.

```csharp
    var _account = "<<account id>>";
    var _start = new DateTime(2013, 7, 22);
    var _end = new DateTime(2020, 4,30);

```

## License

cTrader OpenData is licensed under the [MIT license](./doc//LICENSE).


## Contributing

Feel free to throw your idea on new integration, features, and contribution to this repo.
